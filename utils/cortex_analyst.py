import streamlit as st
import json
import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple
from .data_loader import run_query, get_base_url, get_pat_token, build_snowflake_headers, get_verify_ssl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AVAILABLE_SEMANTIC_MODELS_PATHS = ["HYPERFORGE.GOLD.SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml"]
API_TIMEOUT = 50

class SnowflakeCortexAnalyst:
    def __init__(self, account: str, user: str, role: Optional[str] = None, verify_ssl: bool = True):
        self.account = account
        self.user = user
        self.role = role
        self.verify_ssl = verify_ssl

        self.base_url = get_base_url(account)
        # Optional connection name to support SNOWFLAKE_CONNECTIONS_<NAME>_TOKEN
        self.connection_name = st.secrets.get("features", {}).get("connection_name") if hasattr(st, "secrets") else None

    def _get_valid_token(self) -> str:
        return get_pat_token(self.connection_name)
    
    def _make_api_request(self, endpoint: str, data: dict) -> Tuple[dict, Optional[str]]:
        try:
            token = self._get_valid_token()
            headers = build_snowflake_headers(token, accept='application/json')
            url = f"{self.base_url}{endpoint}"
            
            print(f"🔍 API REQUEST DEBUG:")
            print(f"URL: {url}")
            print(f"Request Body: {json.dumps(data, indent=2)}")
            print("=" * 50)
            
            response = requests.post(
                url, headers=headers, json=data, 
                timeout=API_TIMEOUT, verify=self.verify_ssl
            )
            
            print(f"🔍 API RESPONSE DEBUG:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Content: {response.text[:500]}...")
            print("=" * 50)
            
            if response.status_code < 400:
                return response.json(), None
            else:
                error_data = response.json() if response.content else {}
                error_msg = f"🚨 Cortex Analyst API Error - Status: {response.status_code}, Message: {error_data.get('message', 'Unknown')}"
                return error_data, error_msg
                
        except Exception as e:
            return {}, f"🚨 Request failed: {str(e)}"

    def get_analyst_response(self, messages: List[Dict], semantic_model_path: str) -> Tuple[dict, Optional[str]]:
        # Convert messages to the correct format expected by Cortex Analyst API
        # Cortex Analyst needs alternating user/assistant messages for context
        api_messages = []
        for msg in messages:
            if msg["role"] == "user":
                api_messages.append({
                    "role": "user",
                    "content": [{"type": "text", "text": msg["content"]}]
                })
            elif msg["role"] == "assistant" and msg.get("api_content"):
                # Use the original API response content structure if available
                api_messages.append({
                    "role": "analyst",  # Cortex uses "analyst" role for responses
                    "content": msg["api_content"]
                })
        
        # Try with SQL execution enabled first
        request_body = {
            "messages": api_messages,
            "semantic_model_file": f"@{semantic_model_path}",
        }
        if self.role:
            request_body["role"] = self.role
        
        print(f"🔧 Calling Cortex Analyst API with {len(api_messages)} messages...")
        return self._make_api_request("/api/v2/cortex/analyst/message", request_body)

    def get_complete_response(self, messages: List[Dict], semantic_model_path: str) -> Tuple[str, Optional[str], Optional[List]]:
        """
        Get complete Cortex Analyst response with SQL execution using data_loader.
        
        Returns:
            Tuple of (formatted_response, error_message, api_content_structure)
            The api_content_structure is the original API response content that should be
            stored for followup questions.
        """
        
        print("🤖 Getting response from Cortex Analyst...")
        response, error = self.get_analyst_response(messages, semantic_model_path)
        
        if error:
            return "", error, None
        
        if isinstance(response, dict) and "message" in response:
            content = response["message"]["content"]
            
            text_parts = []
            sql_statement = None
            
            for item in content:
                if item["type"] == "text":
                    text_parts.append(item["text"])
                elif item["type"] == "sql":
                    sql_statement = item["statement"]
            
            interpretation = "\n\n".join(text_parts)
            
            # If we have SQL, execute it with data_loader
            if sql_statement:
                print("🔍 Executing SQL with data_loader...")
                try:
                    df = run_query(sql_statement)
                    print(f"📊 Query returned {len(df)} rows")
                    
                    if len(df) > 0:
                        formatted_results = self._format_results(df, interpretation)
                        return formatted_results, None, content
                    else:
                        return f"{interpretation}\n\n📊 Query executed successfully but returned no results.", None, content
                
                except Exception as sql_error:
                    return f"{interpretation}\n\n🚨 Error executing query: {str(sql_error)}", None, content
            
            return interpretation, None, content
        
        return str(response), None, None
    
    def _format_results(self, df: pd.DataFrame, interpretation: str) -> str:
        """Format DataFrame results for display."""
        formatted = f"**{interpretation}**\n\n📊 **Query Results ({len(df)} assets found):**\n\n"
        
        for idx, row in df.head(10).iterrows():
            formatted += f"**{idx + 1}. {row.get('ASSET_NAME', 'Asset')}**\n"
            formatted += f"   • Model: {row.get('MODEL', 'N/A')} ({row.get('OEM_NAME', 'N/A')})\n"
            if 'AVG_FAILURE_PROB' in row:
                formatted += f"   • Risk Score: {row.get('AVG_FAILURE_PROB', 0):.3f} failure probability\n"
            if 'AVG_HEALTH_SCORE' in row:
                formatted += f"   • Health Score: {row.get('AVG_HEALTH_SCORE', 0):.1f}%\n"
            if 'DOWNTIME_IMPACT_PER_HOUR' in row:
                formatted += f"   • Downtime Impact: ${row.get('DOWNTIME_IMPACT_PER_HOUR', 0):,.2f}/hour\n"
            formatted += "\n"
        
        if len(df) > 10:
            formatted += f"... and {len(df) - 10} more assets\n"
        
        return formatted

# Global client instance
_cortex_client: Optional[SnowflakeCortexAnalyst] = None

def _get_cortex_client() -> SnowflakeCortexAnalyst:
    global _cortex_client
    
    if _cortex_client is None:
        config = st.secrets["snowflake"]

        verify_ssl = get_verify_ssl(config.get("verify_ssl", True))

        _cortex_client = SnowflakeCortexAnalyst(
            account=config["account"],
            user=config["user"],
            role=config.get("role"),
            verify_ssl=verify_ssl
        )
    
    return _cortex_client

def build_analyst_widget(
    title: str = "Cortex Analyst 🤖",
    semantic_model_path: str = "HYPERFORGE.GOLD.SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml",
    initial_message: str = "Hi! Ask me anything about your HyperForge manufacturing data.",
    placeholder: str = "e.g., 'What assets have the highest risk?'"
):
    st.subheader(title)
    
    try:
        client = _get_cortex_client()
    except Exception as e:
        st.error(f"Failed to initialize Cortex client: {e}")
        return

    messages_key = f"cortex_messages_{semantic_model_path.replace('/', '_').replace('.', '_')}"
    
    if messages_key not in st.session_state:
        st.session_state[messages_key] = [{"role": "assistant", "content": initial_message}]
    
    for message in st.session_state[messages_key]:
        with st.chat_message(message["role"]):
            st.markdown(message["content"], unsafe_allow_html=True)

    if prompt := st.chat_input(placeholder):
        st.session_state[messages_key].append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("🤖 Analyzing..."):
                try:
                    api_messages = []
                    for msg in st.session_state[messages_key]:
                        if msg["role"] in ["user", "assistant"]:
                            api_messages.append(msg)
                    
                    assistant_response, error_msg, api_content = client.get_complete_response(api_messages, semantic_model_path)
                    
                    if error_msg:
                        assistant_response = error_msg
                        st.error(error_msg)
                    
                    st.markdown(assistant_response, unsafe_allow_html=True)
                    
                    # Store both the formatted response and the original API content
                    assistant_msg = {"role": "assistant", "content": assistant_response}
                    if api_content:
                        assistant_msg["api_content"] = api_content
                    st.session_state[messages_key].append(assistant_msg)

                except Exception as e:
                    error_msg = f"🚨 Unexpected error: {str(e)}"
                    st.error(error_msg)
                    st.session_state[messages_key].append({"role": "assistant", "content": error_msg})
