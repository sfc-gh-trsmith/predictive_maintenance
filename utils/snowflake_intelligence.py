import streamlit as st
import json
import requests
import jwt
import os
import pandas as pd
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization
import logging
from typing import List, Dict, Optional, Tuple, Any
from .data_loader import run_query
from .intelligence_tools import IntelligenceToolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TIMEOUT = 50

class SnowflakeIntelligenceAgent:
    """
    Snowflake Intelligence Agent client - similar to CortexAnalyst but for Intelligence agents.
    Handles agent communication, tool calling, and response processing.
    """
    
    def __init__(self, account: str, user: str, agent_name: str, private_key_path: str = None, 
                 personal_access_token: str = None, role: Optional[str] = None, verify_ssl: bool = True):
        self.account = account
        self.user = user
        self.agent_name = agent_name
        self.private_key_path = private_key_path
        self.personal_access_token = personal_access_token
        self.role = role
        self.verify_ssl = verify_ssl
        
        url_account = account.replace('_', '-')
        self.base_url = f"https://{url_account}.snowflakecomputing.com"
        self._jwt_token = None
        self._token_expires_at = None
        
        # Initialize tool executor
        self.tool_executor = IntelligenceToolExecutor()
        
        # Thread management for context
        self._thread_id = None
        
        # Determine authentication method
        if personal_access_token:
            self.auth_method = "PAT"
            print("🔑 Using Personal Access Token (PAT) authentication for Intelligence Agent")
        elif private_key_path:
            self.auth_method = "JWT"
            print("🔑 Using JWT authentication for Intelligence Agent")
        else:
            raise ValueError("Either personal_access_token or private_key_path must be provided")
    
    def _load_private_key(self):
        """Load private key for JWT authentication"""
        with open(self.private_key_path, 'rb') as key_file:
            key_data = key_file.read()
        
        if self.private_key_path.endswith('.p8'):
            try:
                return serialization.load_der_private_key(key_data, password=None)
            except ValueError:
                pass
        
        return serialization.load_pem_private_key(key_data, password=None)
    
    def _generate_jwt_token(self) -> str:
        """Generate JWT token for authentication"""
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
        """Get valid authentication token"""
        if self.auth_method == "PAT":
            return self.personal_access_token
        else:  # JWT
            if (self._jwt_token is None or 
                self._token_expires_at is None or 
                datetime.utcnow() >= self._token_expires_at - timedelta(minutes=5)):
                return self._generate_jwt_token()
            return self._jwt_token
    
    def _make_api_request(self, endpoint: str, data: dict) -> Tuple[dict, Optional[str]]:
        """Make API request to Snowflake Intelligence Agent"""
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
            
            print(f"🧠 INTELLIGENCE AGENT REQUEST DEBUG:")
            print(f"Agent: {self.agent_name}")
            print(f"URL: {url}")
            print(f"Request Body: {json.dumps(data, indent=2)}")
            print("=" * 50)
            
            response = requests.post(
                url, headers=headers, json=data, 
                timeout=API_TIMEOUT, verify=self.verify_ssl
            )
            
            print(f"🧠 INTELLIGENCE AGENT RESPONSE DEBUG:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Content: {response.text[:500]}...")
            print("=" * 50)
            
            if response.status_code < 400:
                return response.json(), None
            else:
                error_data = response.json() if response.content else {}
                error_msg = f"🚨 Intelligence Agent API Error - Status: {response.status_code}, Message: {error_data.get('message', 'Unknown')}"
                return error_data, error_msg
                
        except Exception as e:
            return {}, f"🚨 Intelligence Agent request failed: {str(e)}"
    
    def _get_or_create_thread_id(self) -> str:
        """Get existing thread ID or create a new one for context management."""
        if self._thread_id is None:
            try:
                # Create a new thread using the Cortex threads API
                thread_data = {
                    "origin_application": "HyperForge"  # Shortened to 9 bytes (under 16 byte limit)
                }
                
                response, error = self._make_api_request("/api/v2/cortex/threads", thread_data)
                if not error and "thread_id" in response:
                    self._thread_id = response["thread_id"]
                    print(f"🧵 Created new thread: {self._thread_id}")
                else:
                    # Fallback to a simple UUID-like string
                    import uuid
                    self._thread_id = str(uuid.uuid4())
                    print(f"🧵 Using fallback thread ID: {self._thread_id}")
            except Exception as e:
                # Fallback to a simple UUID-like string
                import uuid
                self._thread_id = str(uuid.uuid4())
                print(f"🧵 Thread creation failed, using fallback: {self._thread_id}")
        
        return self._thread_id
    
    def _make_streaming_api_request(self, endpoint: str, data: dict) -> Tuple[dict, Optional[str]]:
        """Make API request and handle streaming response from Intelligence Agent."""
        try:
            token = self._get_valid_token()
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'  # Accept streaming response
            }
            
            # Add PAT-specific header if using PAT authentication
            if self.auth_method == "PAT":
                headers['X-Snowflake-Authorization-Token-Type'] = 'PROGRAMMATIC_ACCESS_TOKEN'
            
            url = f"{self.base_url}{endpoint}"
            
            print(f"🧠 INTELLIGENCE AGENT REQUEST DEBUG:")
            print(f"Agent: {self.agent_name}")
            print(f"URL: {url}")
            print(f"Request Body: {json.dumps(data, indent=2)}")
            print("=" * 50)
            
            response = requests.post(
                url, headers=headers, json=data, 
                timeout=API_TIMEOUT, verify=self.verify_ssl, stream=True
            )
            
            print(f"🧠 INTELLIGENCE AGENT RESPONSE DEBUG:")
            print(f"Status Code: {response.status_code}")
            
            if response.status_code < 400:
                # Parse streaming response
                return self._parse_streaming_response(response), None
            else:
                error_content = response.text[:500] if response.text else "No content"
                print(f"Response Content: {error_content}...")
                print("=" * 50)
                
                error_msg = f"🚨 Intelligence Agent API Error - Status: {response.status_code}"
                return {}, error_msg
                
        except Exception as e:
            return {}, f"🚨 Intelligence Agent streaming request failed: {str(e)}"
    
    def _parse_streaming_response(self, response) -> dict:
        """Parse Server-Sent Events streaming response from Intelligence Agent."""
        content_parts = []
        status = "unknown"
        thinking_content = []
        final_response = ""
        tools_executed = []
        
        try:
            print(f"🧠 Parsing streaming response...")
            
            for line in response.iter_lines(decode_unicode=True):
                if line.startswith('data: '):
                    data_str = line[6:]  # Remove 'data: ' prefix
                    if data_str.strip():
                        try:
                            data = json.loads(data_str)
                            
                            # Handle different event types
                            if 'status' in data:
                                status = data['status']
                                print(f"🧠 Agent status: {status}")
                            
                            # Handle thinking delta (reasoning process)
                            if 'text' in data and 'content_index' in data:
                                thinking_content.append(data['text'])
                            
                            # Handle final message content
                            if 'content' in data:
                                if isinstance(data['content'], list):
                                    for item in data['content']:
                                        if isinstance(item, dict):
                                            if item.get('type') == 'text':
                                                content_parts.append(item.get('text', ''))
                                            elif item.get('type') == 'tool_calls':
                                                tools_executed.append(item)
                                elif isinstance(data['content'], str):
                                    content_parts.append(data['content'])
                            
                            # Handle direct message responses
                            if 'message' in data and isinstance(data['message'], dict):
                                if 'content' in data['message']:
                                    final_response = data['message']['content']
                            
                            # Handle response content directly
                            if 'response' in data:
                                if isinstance(data['response'], dict) and 'content' in data['response']:
                                    final_response = data['response']['content']
                                elif isinstance(data['response'], str):
                                    final_response = data['response']
                            
                        except json.JSONDecodeError:
                            # Skip invalid JSON lines
                            continue
                elif line.startswith('event: '):
                    # Log event types for debugging
                    event_type = line[7:]  # Remove 'event: ' prefix
                    print(f"🧠 Event type: {event_type}")
            
            # Combine all content sources in priority order
            if final_response:
                response_text = final_response
            elif content_parts:
                response_text = ''.join(content_parts)
            elif thinking_content:
                # Use thinking content as fallback, but clean it up
                thinking_text = ''.join(thinking_content)
                response_text = f"Based on my analysis: {thinking_text}"
            else:
                response_text = "I've processed your request successfully."
            
            print(f"🧠 Final parsed response length: {len(response_text)} characters")
            print(f"🧠 Response preview: {response_text[:200]}...")
            
            return {
                "message": {
                    "content": response_text,
                    "role": "assistant"
                },
                "status": status,
                "thinking": ''.join(thinking_content),
                "tools": tools_executed
            }
            
        except Exception as e:
            print(f"🚨 Error parsing streaming response: {str(e)}")
            return {
                "message": {
                    "content": "I apologize, but I encountered an error processing the response. Please try again.",
                    "role": "assistant"  
                },
                "status": "error"
            }

    def get_agent_response(self, messages: List[Dict]) -> Tuple[dict, Optional[str]]:
        """
        Get response from Snowflake Intelligence Agent.
        Try multiple request formats until one works.
        """
        latest_user_message = None
        for msg in reversed(messages):
            if msg["role"] == "user":
                latest_user_message = msg["content"]
                break
        
        if not latest_user_message:
            return {}, "No user message found in conversation"
        
        # Based on Snowflake documentation, Intelligence Agents expect this format
        # with thread_id and parent_message_id for context management
        # Let's try a few variations to see what works
        
        base_request = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": latest_user_message}]
                }
            ]
        }
        
        # Try different combinations of required fields
        request_variations = [
            # Variation 1: With thread_id and parent_message_id
            {
                **base_request,
                "thread_id": self._get_or_create_thread_id(),
                "parent_message_id": "0"
            },
            # Variation 2: Without thread management (simpler)
            base_request,
            # Variation 3: With just thread_id
            {
                **base_request,
                "thread_id": self._get_or_create_thread_id()
            },
            # Variation 4: Different message format
            {
                "messages": [
                    {
                        "role": "user",
                        "content": latest_user_message
                    }
                ],
                "thread_id": self._get_or_create_thread_id(),
                "parent_message_id": "0"
            }
        ]
        
        print(f"🧠 Calling Intelligence Agent API...")
        # Use the correct Cortex Agent REST API endpoint structure
        # Format: POST /api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run
        endpoint = f"/api/v2/databases/snowflake_intelligence/schemas/agents/agents/{self.agent_name.split('.')[-1]}:run"
        
        # Try each variation
        for i, request_body in enumerate(request_variations, 1):
            print(f"🧠 Trying request variation {i}: {json.dumps(request_body, indent=2)}")
            
            # First try streaming request
            response, error = self._make_streaming_api_request(endpoint, request_body)
            
            if not error:
                print(f"✅ Request variation {i} succeeded with streaming!")
                return response, None
            else:
                print(f"❌ Request variation {i} failed with streaming: {error}")
                
                # If streaming fails, try regular API request as fallback
                print(f"🔄 Trying variation {i} with regular API request...")
                response, error = self._make_api_request(endpoint, request_body)
                
                if not error:
                    print(f"✅ Request variation {i} succeeded with regular API!")
                    return response, None
                else:
                    print(f"❌ Request variation {i} also failed with regular API: {error}")
                    
                if i < len(request_variations):
                    print(f"🔄 Trying next variation...")
                    continue
        
        return {}, f"All request variations failed. Last error: {error}"

    def get_complete_response(self, messages: List[Dict]) -> Tuple[str, Optional[str]]:
        """Get complete Intelligence Agent response with tool execution."""
        
        print("🧠 Getting response from Intelligence Agent...")
        response, error = self.get_agent_response(messages)
        
        if error:
            return "", error
        
        # Process Intelligence Agent response
        if isinstance(response, dict):
            if "message" in response:
                # Extract content from message
                message = response["message"]
                if isinstance(message, dict) and "content" in message:
                    return message["content"], None
                elif isinstance(message, str):
                    return message, None
            elif "content" in response:
                return response["content"], None
            elif "response" in response:
                return response["response"], None
            else:
                # Fallback - convert entire response to string
                return str(response), None
        else:
            return str(response), None
    
    def _process_agent_response(self, agent_message: dict) -> Tuple[str, Optional[str]]:
        """Process Intelligence Agent response, including tool calls."""
        
        text_parts = []
        tool_calls = []
        
        # Extract content based on response structure
        if isinstance(agent_message, dict):
            if "content" in agent_message:
                content = agent_message["content"]
                if isinstance(content, str):
                    text_parts.append(content)
                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict):
                            if item.get("type") == "text":
                                text_parts.append(item.get("text", ""))
                            elif item.get("type") == "tool_call":
                                tool_calls.append(item)
                        elif isinstance(item, str):
                            text_parts.append(item)
            
            # Check for tool calls in message
            if "tool_calls" in agent_message:
                tool_calls.extend(agent_message["tool_calls"])
        
        # Process tool calls if any
        tool_results = []
        if tool_calls:
            print(f"🔧 Processing {len(tool_calls)} tool calls...")
            for tool_call in tool_calls:
                try:
                    result = self.tool_executor.execute_tool(tool_call)
                    tool_results.append(result)
                except Exception as e:
                    logger.error(f"Tool execution failed: {str(e)}")
                    tool_results.append(f"Tool execution failed: {str(e)}")
        
        # Combine text and tool results
        interpretation = "\n\n".join(text_parts) if text_parts else "I understand your request."
        
        if tool_results:
            formatted_results = f"{interpretation}\n\n📊 **Actions Completed:**\n\n"
            for i, result in enumerate(tool_results, 1):
                formatted_results += f"{i}. {result}\n\n"
            return formatted_results, None
        
        return interpretation, None
    
    def _format_results(self, df: pd.DataFrame, interpretation: str) -> str:
        """Format DataFrame results for display (similar to Cortex Analyst)."""
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


# Global Intelligence client instance
_intelligence_client: Optional[SnowflakeIntelligenceAgent] = None

def _get_intelligence_client() -> SnowflakeIntelligenceAgent:
    """Get or create the global Intelligence client instance"""
    global _intelligence_client
    
    if _intelligence_client is None:
        config = st.secrets["snowflake"]
        
        verify_ssl = config.get("verify_ssl", True)
        if isinstance(verify_ssl, str):
            verify_ssl = verify_ssl.lower() in ('true', '1', 'yes', 'on')
        
        personal_access_token = config.get("personal_access_token")
        private_key_path = config.get("private_key_file")
        
        # Get agent name from config
        agent_name = st.secrets.get("features", {}).get(
            "intelligence_agent", 
            "SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT"
        )
        
        _intelligence_client = SnowflakeIntelligenceAgent(
            account=config["account"],
            user=config["user"],
            agent_name=agent_name,
            private_key_path=private_key_path,
            personal_access_token=personal_access_token,
            role=config.get("role"),
            verify_ssl=verify_ssl
        )
    
    return _intelligence_client
