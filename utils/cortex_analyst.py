import streamlit as st
import json
import requests
import jwt
import os
import pandas as pd
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization
import logging
from typing import List, Dict, Optional, Tuple
from .data_loader import run_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AVAILABLE_SEMANTIC_MODELS_PATHS = ["HYPERFORGE.GOLD.SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml"]
API_TIMEOUT = 50

class SnowflakeCortexAnalyst:
    def __init__(self, account: str, user: str, private_key_path: str = None, personal_access_token: str = None, role: Optional[str] = None, verify_ssl: bool = True):
        self.account = account
        self.user = user
        self.private_key_path = private_key_path
        self.personal_access_token = personal_access_token
        self.role = role
        self.verify_ssl = verify_ssl
        
        url_account = account.replace('_', '-')
        self.base_url = f"https://{url_account}.snowflakecomputing.com"
        self._jwt_token = None
        self._token_expires_at = None
        
        # Determine authentication method
        if personal_access_token:
            self.auth_method = "PAT"
            print("🔑 Using Personal Access Token (PAT) authentication")
        elif private_key_path:
            self.auth_method = "JWT"
            print("🔑 Using JWT authentication")
        else:
            raise ValueError("Either personal_access_token or private_key_path must be provided")
    
    def _load_private_key(self):
        with open(self.private_key_path, 'rb') as key_file:
            key_data = key_file.read()
        
        if self.private_key_path.endswith('.p8'):
            try:
                return serialization.load_der_private_key(key_data, password=None)
            except ValueError:
                pass
        
        return serialization.load_pem_private_key(key_data, password=None)
    
    def _generate_jwt_token(self) -> str:
        private_key = self._load_private_key()
        
        jwt_account = self.account.upper()
        jwt_user = self.user.upper()
        
        now = datetime.utcnow()
        iat = int(now.timestamp())
        exp = int((now + timedelta(hours=1)).timestamp())
        
        payload = {
            'iss': f"{jwt_user}.{jwt_account}",
            'sub': jwt_user,
            'iat': iat,
            'exp': exp,
            'aud': f"https://{jwt_account.replace('_', '-').lower()}.snowflakecomputing.com"
        }
        
        headers = {'typ': 'JWT', 'alg': 'RS256'}
        
        token = jwt.encode(payload, private_key, algorithm='RS256', headers=headers)
        
        self._jwt_token = token
        self._token_expires_at = datetime.fromtimestamp(exp)
        
        return token
    
    def _get_valid_token(self) -> str:
        if self.auth_method == "PAT":
            return self.personal_access_token
        else:  # JWT
            if (self._jwt_token is None or 
                self._token_expires_at is None or 
                datetime.utcnow() >= self._token_expires_at - timedelta(minutes=5)):
                return self._generate_jwt_token()
            return self._jwt_token
    
    def _make_api_request(self, endpoint: str, data: dict) -> Tuple[dict, Optional[str]]:
        try:
            token = self._get_valid_token()
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Add PAT-specific header if using PAT authentication
            if self.auth_method == "PAT":
                headers['X-Snowflake-Authorization-Token-Type'] = 'PROGRAMMATIC_ACCESS_TOKEN'
            
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
        api_messages = []
        for msg in messages:
            if msg["role"] == "user":
                api_messages.append({
                    "role": "user",
                    "content": [{"type": "text", "text": msg["content"]}]
                })
        
        # Try with SQL execution enabled first
        request_body = {
            "messages": api_messages,
            "semantic_model_file": f"@{semantic_model_path}",
        }
        if self.role:
            request_body["role"] = self.role
        
        print(f"🔧 Calling Cortex Analyst API...")
        return self._make_api_request("/api/v2/cortex/analyst/message", request_body)

    def get_complete_response(self, messages: List[Dict], semantic_model_path: str) -> Tuple[str, Optional[str]]:
        """Get complete Cortex Analyst response with SQL execution using data_loader."""
        
        print("🤖 Getting response from Cortex Analyst...")
        response, error = self.get_analyst_response(messages, semantic_model_path)
        
        if error:
            return "", error
        
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
                        return formatted_results, None
                    else:
                        return f"{interpretation}\n\n📊 Query executed successfully but returned no results.", None
                
                except Exception as sql_error:
                    return f"{interpretation}\n\n🚨 Error executing query: {str(sql_error)}", None
            
            return interpretation, None
        
        return str(response), None
    
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
        
        verify_ssl = config.get("verify_ssl", True)
        if isinstance(verify_ssl, str):
            verify_ssl = verify_ssl.lower() in ('true', '1', 'yes', 'on')
        
        personal_access_token = config.get("personal_access_token")
        private_key_path = config.get("private_key_file")
        
        _cortex_client = SnowflakeCortexAnalyst(
            account=config["account"],
            user=config["user"],
            private_key_path=private_key_path,
            personal_access_token=personal_access_token,
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
                            api_messages.append({"role": msg["role"], "content": msg["content"]})
                    
                    assistant_response, error_msg = client.get_complete_response(api_messages, semantic_model_path)
                    
                    if error_msg:
                        assistant_response = error_msg
                        st.error(error_msg)
                    
                    st.markdown(assistant_response, unsafe_allow_html=True)
                    st.session_state[messages_key].append({"role": "assistant", "content": assistant_response})

                except Exception as e:
                    error_msg = f"🚨 Unexpected error: {str(e)}"
                    st.error(error_msg)
                    st.session_state[messages_key].append({"role": "assistant", "content": error_msg})
