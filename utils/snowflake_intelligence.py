import streamlit as st
import json
import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple, Any
from .data_loader import (
    run_query,
    get_base_url,
    get_pat_token,
    build_snowflake_headers,
    get_verify_ssl,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TIMEOUT = 50


class IntelligenceToolExecutor:
    """
    Handles execution of tools called by the Snowflake Intelligence Agent.
    Provides maintenance operations beyond simple data queries.
    """
    
    def __init__(self):
        self.available_tools = {
            "query_asset_health": self._query_asset_health,
            "create_maintenance_work_order": self._create_maintenance_work_order,
            "get_asset_failure_prediction": self._get_asset_failure_prediction,
            "schedule_preventive_maintenance": self._schedule_preventive_maintenance,
            "get_maintenance_history": self._get_maintenance_history,
            "calculate_downtime_risk": self._calculate_downtime_risk,
            "get_oee_metrics": self._get_oee_metrics,
            "trigger_maintenance_alert": self._trigger_maintenance_alert
        }
    
    def execute_tool(self, tool_call: Dict[str, Any]) -> str:
        """
        Execute a tool based on the tool call from Intelligence Agent.
        
        Args:
            tool_call: Dictionary containing tool name and parameters
            
        Returns:
            String result of tool execution
        """
        try:
            tool_name = tool_call.get("name") or tool_call.get("function", {}).get("name")
            tool_params = tool_call.get("parameters") or tool_call.get("function", {}).get("arguments", {})
            
            if not tool_name:
                return "❌ Tool call missing name"
            
            if tool_name not in self.available_tools:
                return f"❌ Unknown tool: {tool_name}"
            
            logger.info(f"🔧 Executing tool: {tool_name} with params: {tool_params}")
            
            # Execute the tool
            result = self.available_tools[tool_name](tool_params)
            
            logger.info(f"✅ Tool {tool_name} completed successfully")
            return result
            
        except Exception as e:
            error_msg = f"❌ Tool execution failed: {str(e)}"
            logger.error(error_msg)
            return error_msg
    
    def _query_asset_health(self, params: Dict[str, Any]) -> str:
        """Query asset health information"""
        try:
            asset_id = params.get("asset_id")
            limit = params.get("limit", 10)
            
            if asset_id:
                query = f"""
                SELECT 
                    a.ASSET_NAME,
                    a.MODEL,
                    a.OEM_NAME,
                    h.LATEST_HEALTH_SCORE as HEALTH_SCORE,
                    h.AVG_FAILURE_PROBABILITY as FAILURE_RISK,
                    a.DOWNTIME_IMPACT_PER_HOUR
                FROM HYPERFORGE.SILVER.DIM_ASSET a
                JOIN HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH h ON a.ASSET_ID = h.ASSET_ID
                WHERE a.ASSET_ID = {asset_id} AND a.IS_CURRENT = TRUE
                ORDER BY h.HOUR_TIMESTAMP DESC
                LIMIT 1
                """
            else:
                query = f"""
                SELECT 
                    a.ASSET_NAME,
                    a.MODEL,
                    a.OEM_NAME,
                    h.LATEST_HEALTH_SCORE as HEALTH_SCORE,
                    h.AVG_FAILURE_PROBABILITY as FAILURE_RISK,
                    a.DOWNTIME_IMPACT_PER_HOUR
                FROM HYPERFORGE.SILVER.DIM_ASSET a
                JOIN HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH h ON a.ASSET_ID = h.ASSET_ID
                WHERE a.IS_CURRENT = TRUE
                ORDER BY h.AVG_FAILURE_PROBABILITY DESC
                LIMIT {limit}
                """
            
            df = run_query(query)
            
            if len(df) == 0:
                return "No asset health data found"
            
            result = "🏥 **Asset Health Status:**\n\n"
            for idx, row in df.iterrows():
                health_score = row['HEALTH_SCORE']
                failure_risk = row['FAILURE_RISK']
                
                # Determine status emoji
                if health_score >= 90:
                    status_emoji = "✅"
                elif health_score >= 70:
                    status_emoji = "⚠️"
                else:
                    status_emoji = "🚨"
                
                result += f"{status_emoji} **{row['ASSET_NAME']}**\n"
                result += f"   • Health Score: {health_score:.1f}%\n"
                result += f"   • Failure Risk: {failure_risk:.3f}\n"
                result += f"   • Model: {row['MODEL']} ({row['OEM_NAME']})\n"
                result += f"   • Downtime Impact: ${row['DOWNTIME_IMPACT_PER_HOUR']:,.2f}/hour\n\n"
            
            return result
            
        except Exception as e:
            return f"❌ Failed to query asset health: {str(e)}"
    
    def _create_maintenance_work_order(self, params: Dict[str, Any]) -> str:
        """Create a maintenance work order"""
        try:
            asset_id = params.get("asset_id")
            asset_name = params.get("asset_name", "Unknown Asset")
            priority = params.get("priority", "Medium")
            work_type = params.get("work_type", "Preventive Maintenance")
            description = params.get("description", "Scheduled maintenance")
            
            # In a real implementation, this would integrate with the CMMS system
            # For now, we'll simulate creating a work order
            
            work_order_id = f"WO-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Log the work order creation (in real system, this would insert into database)
            logger.info(f"Creating work order {work_order_id} for asset {asset_id}")
            
            result = f"✅ **Maintenance Work Order Created**\n\n"
            result += f"• **Work Order ID:** {work_order_id}\n"
            result += f"• **Asset:** {asset_name} (ID: {asset_id})\n"
            result += f"• **Type:** {work_type}\n"
            result += f"• **Priority:** {priority}\n"
            result += f"• **Description:** {description}\n"
            result += f"• **Created:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            result += f"• **Status:** Pending Assignment\n\n"
            result += f"📧 Work order has been submitted to the maintenance team."
            
            return result
            
        except Exception as e:
            return f"❌ Failed to create work order: {str(e)}"
    
    def _get_asset_failure_prediction(self, params: Dict[str, Any]) -> str:
        """Get failure predictions for assets"""
        try:
            days_ahead = params.get("days_ahead", 7)
            threshold = params.get("threshold", 0.5)
            
            query = f"""
            SELECT 
                a.ASSET_NAME,
                a.MODEL,
                h.AVG_FAILURE_PROBABILITY,
                h.MIN_RUL_DAYS,
                a.DOWNTIME_IMPACT_PER_HOUR
            FROM HYPERFORGE.SILVER.DIM_ASSET a
            JOIN HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH h ON a.ASSET_ID = h.ASSET_ID
            WHERE a.IS_CURRENT = TRUE 
            AND h.AVG_FAILURE_PROBABILITY > {threshold}
            AND h.MIN_RUL_DAYS <= {days_ahead}
            ORDER BY h.AVG_FAILURE_PROBABILITY DESC
            LIMIT 10
            """
            
            df = run_query(query)
            
            if len(df) == 0:
                return f"✅ No assets predicted to fail within {days_ahead} days (threshold: {threshold})"
            
            result = f"⚠️ **Assets at Risk of Failure (Next {days_ahead} days):**\n\n"
            total_risk_value = 0
            
            for idx, row in df.iterrows():
                failure_prob = row['AVG_FAILURE_PROBABILITY']
                rul_days = row['MIN_RUL_DAYS']
                impact = row['DOWNTIME_IMPACT_PER_HOUR']
                
                risk_level = "🚨 Critical" if failure_prob > 0.8 else "⚠️ High" if failure_prob > 0.6 else "🟡 Medium"
                
                result += f"{risk_level} **{row['ASSET_NAME']}**\n"
                result += f"   • Failure Probability: {failure_prob:.1%}\n"
                result += f"   • Remaining Useful Life: {rul_days} days\n"
                result += f"   • Potential Impact: ${impact:,.2f}/hour\n\n"
                
                total_risk_value += failure_prob * impact
            
            result += f"💰 **Total Risk Value:** ${total_risk_value:,.2f}/hour potential impact"
            
            return result
            
        except Exception as e:
            return f"❌ Failed to get failure predictions: {str(e)}"
    
    def _schedule_preventive_maintenance(self, params: Dict[str, Any]) -> str:
        """Schedule preventive maintenance for assets"""
        try:
            asset_ids = params.get("asset_ids", [])
            schedule_date = params.get("schedule_date")
            maintenance_type = params.get("maintenance_type", "Preventive")
            
            if not asset_ids:
                return "❌ No asset IDs provided for scheduling"
            
            scheduled_items = []
            
            for asset_id in asset_ids:
                # Get asset info
                asset_query = f"""
                SELECT ASSET_NAME, MODEL, OEM_NAME 
                FROM HYPERFORGE.SILVER.DIM_ASSET 
                WHERE ASSET_ID = {asset_id} AND IS_CURRENT = TRUE
                """
                
                asset_df = run_query(asset_query)
                if len(asset_df) > 0:
                    asset_name = asset_df.iloc[0]['ASSET_NAME']
                    scheduled_items.append({
                        'asset_id': asset_id,
                        'asset_name': asset_name,
                        'schedule_id': f"PM-{datetime.now().strftime('%Y%m%d')}-{asset_id}"
                    })
            
            if not scheduled_items:
                return "❌ No valid assets found for scheduling"
            
            result = f"📅 **Preventive Maintenance Scheduled**\n\n"
            result += f"• **Maintenance Type:** {maintenance_type}\n"
            result += f"• **Scheduled Date:** {schedule_date or 'Next available slot'}\n"
            result += f"• **Assets Scheduled:** {len(scheduled_items)}\n\n"
            
            for item in scheduled_items:
                result += f"   ✅ {item['asset_name']} (Schedule ID: {item['schedule_id']})\n"
            
            result += f"\n📧 Maintenance team has been notified of the scheduled activities."
            
            return result
            
        except Exception as e:
            return f"❌ Failed to schedule maintenance: {str(e)}"
    
    def _get_maintenance_history(self, params: Dict[str, Any]) -> str:
        """Get maintenance history for assets"""
        try:
            asset_id = params.get("asset_id")
            days_back = params.get("days_back", 30)
            
            query = f"""
            SELECT 
                a.ASSET_NAME,
                m.COMPLETED_DATE,
                wt.WO_TYPE_NAME,
                m.DOWNTIME_HOURS,
                m.LABOR_COST + m.PARTS_COST as TOTAL_COST,
                m.TECHNICIAN_NOTES,
                m.FAILURE_FLAG
            FROM HYPERFORGE.SILVER.FCT_MAINTENANCE_LOG m
            JOIN HYPERFORGE.SILVER.DIM_ASSET a ON m.ASSET_ID = a.ASSET_ID
            JOIN HYPERFORGE.SILVER.DIM_WORK_ORDER_TYPE wt ON m.WO_TYPE_ID = wt.WO_TYPE_ID
            WHERE m.COMPLETED_DATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
            """
            
            if asset_id:
                query += f" AND m.ASSET_ID = {asset_id}"
            
            query += " ORDER BY m.COMPLETED_DATE DESC LIMIT 20"
            
            df = run_query(query)
            
            if len(df) == 0:
                return f"No maintenance history found for the last {days_back} days"
            
            result = f"🔧 **Maintenance History (Last {days_back} days):**\n\n"
            
            for idx, row in df.iterrows():
                failure_indicator = "🚨 " if row['FAILURE_FLAG'] else ""
                result += f"{failure_indicator}**{row['ASSET_NAME']}** - {row['COMPLETED_DATE']}\n"
                result += f"   • Type: {row['WO_TYPE_NAME']}\n"
                result += f"   • Downtime: {row['DOWNTIME_HOURS']} hours\n"
                result += f"   • Cost: ${row['TOTAL_COST']:,.2f}\n"
                if row['TECHNICIAN_NOTES']:
                    result += f"   • Notes: {row['TECHNICIAN_NOTES'][:100]}...\n"
                result += "\n"
            
            return result
            
        except Exception as e:
            return f"❌ Failed to get maintenance history: {str(e)}"
    
    def _calculate_downtime_risk(self, params: Dict[str, Any]) -> str:
        """Calculate financial impact of potential downtime"""
        try:
            time_horizon = params.get("time_horizon_days", 30)
            
            query = f"""
            SELECT 
                a.ASSET_NAME,
                a.DOWNTIME_IMPACT_PER_HOUR,
                h.AVG_FAILURE_PROBABILITY,
                h.MIN_RUL_DAYS,
                (a.DOWNTIME_IMPACT_PER_HOUR * h.AVG_FAILURE_PROBABILITY * 24) as DAILY_RISK_VALUE
            FROM HYPERFORGE.SILVER.DIM_ASSET a
            JOIN HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH h ON a.ASSET_ID = h.ASSET_ID
            WHERE a.IS_CURRENT = TRUE 
            AND h.MIN_RUL_DAYS <= {time_horizon}
            ORDER BY DAILY_RISK_VALUE DESC
            LIMIT 10
            """
            
            df = run_query(query)
            
            if len(df) == 0:
                return f"✅ No significant downtime risks identified for {time_horizon} day horizon"
            
            total_risk = df['DAILY_RISK_VALUE'].sum() * time_horizon
            
            result = f"💰 **Downtime Risk Analysis ({time_horizon} days):**\n\n"
            result += f"**Total Portfolio Risk:** ${total_risk:,.2f}\n\n"
            
            for idx, row in df.iterrows():
                daily_risk = row['DAILY_RISK_VALUE']
                period_risk = daily_risk * time_horizon
                
                result += f"⚠️ **{row['ASSET_NAME']}**\n"
                result += f"   • Daily Risk Value: ${daily_risk:,.2f}\n"
                result += f"   • {time_horizon}-Day Risk: ${period_risk:,.2f}\n"
                result += f"   • Failure Probability: {row['AVG_FAILURE_PROBABILITY']:.1%}\n"
                result += f"   • Time to Failure: {row['MIN_RUL_DAYS']} days\n\n"
            
            return result
            
        except Exception as e:
            return f"❌ Failed to calculate downtime risk: {str(e)}"
    
    def _get_oee_metrics(self, params: Dict[str, Any]) -> str:
        """Get Overall Equipment Effectiveness metrics"""
        try:
            days_back = params.get("days_back", 7)
            
            query = f"""
            SELECT 
                a.ASSET_NAME,
                l.LINE_NAME,
                AVG(p.ACTUAL_RUNTIME_HOURS / p.PLANNED_RUNTIME_HOURS) as AVAILABILITY,
                AVG((p.UNITS_PRODUCED - p.UNITS_SCRAPPED) / p.UNITS_PRODUCED) as QUALITY_RATE,
                AVG(p.UNITS_PRODUCED / (p.ACTUAL_RUNTIME_HOURS * 100)) as PERFORMANCE_RATE
            FROM HYPERFORGE.SILVER.FCT_PRODUCTION_LOG p
            JOIN HYPERFORGE.SILVER.DIM_ASSET a ON p.ASSET_ID = a.ASSET_ID
            JOIN HYPERFORGE.SILVER.DIM_LINE l ON a.LINE_ID = l.LINE_ID
            WHERE p.PRODUCTION_DATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
            AND a.IS_CURRENT = TRUE
            GROUP BY a.ASSET_NAME, l.LINE_NAME
            ORDER BY AVAILABILITY DESC
            LIMIT 10
            """
            
            df = run_query(query)
            
            if len(df) == 0:
                return f"No OEE data available for the last {days_back} days"
            
            result = f"📊 **OEE Metrics (Last {days_back} days):**\n\n"
            
            for idx, row in df.iterrows():
                availability = row['AVAILABILITY'] * 100
                quality = row['QUALITY_RATE'] * 100
                performance = row['PERFORMANCE_RATE'] * 100
                oee = (availability * quality * performance) / 10000
                
                oee_status = "🟢" if oee >= 85 else "🟡" if oee >= 65 else "🔴"
                
                result += f"{oee_status} **{row['ASSET_NAME']}** ({row['LINE_NAME']})\n"
                result += f"   • Overall OEE: {oee:.1f}%\n"
                result += f"   • Availability: {availability:.1f}%\n"
                result += f"   • Quality: {quality:.1f}%\n"
                result += f"   • Performance: {performance:.1f}%\n\n"
            
            return result
            
        except Exception as e:
            return f"❌ Failed to get OEE metrics: {str(e)}"
    
    def _trigger_maintenance_alert(self, params: Dict[str, Any]) -> str:
        """Trigger maintenance alerts for critical conditions"""
        try:
            alert_type = params.get("alert_type", "Critical Asset Condition")
            asset_ids = params.get("asset_ids", [])
            message = params.get("message", "Immediate attention required")
            
            # In a real system, this would integrate with alerting systems
            alert_id = f"ALERT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            result = f"🚨 **Maintenance Alert Triggered**\n\n"
            result += f"• **Alert ID:** {alert_id}\n"
            result += f"• **Type:** {alert_type}\n"
            result += f"• **Message:** {message}\n"
            result += f"• **Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            
            if asset_ids:
                result += f"• **Assets Affected:** {len(asset_ids)} assets\n"
                for asset_id in asset_ids[:5]:  # Show first 5
                    result += f"   - Asset ID: {asset_id}\n"
                if len(asset_ids) > 5:
                    result += f"   - ... and {len(asset_ids) - 5} more\n"
            
            result += f"\n📧 Alert has been sent to maintenance team and supervisors."
            
            return result
            
        except Exception as e:
            return f"❌ Failed to trigger alert: {str(e)}"


class SnowflakeIntelligenceAgent:
    """
    Snowflake Intelligence Agent client - similar to CortexAnalyst but for Intelligence agents.
    Handles agent communication, tool calling, and response processing.
    """
    
    def __init__(self, account: str, user: str, agent_name: str,
                 role: Optional[str] = None, verify_ssl: bool = True):
        self.account = account
        self.user = user
        self.agent_name = agent_name
        self.role = role
        self.verify_ssl = verify_ssl
        
        self.base_url = get_base_url(account)
        
        # Initialize tool executor
        self.tool_executor = IntelligenceToolExecutor()
        
        # Thread management for context
        self._thread_id = None
        
        # Optional connection name for SNOWFLAKE_CONNECTIONS_<NAME>_TOKEN
        self.connection_name = st.secrets.get("features", {}).get("connection_name") if hasattr(st, "secrets") else None
    
    def _get_valid_token(self) -> str:
        """Get PAT from configured sources."""
        return get_pat_token(self.connection_name)
    
    def _make_api_request(self, endpoint: str, data: dict) -> Tuple[dict, Optional[str]]:
        """Make API request to Snowflake Intelligence Agent"""
        try:
            token = self._get_valid_token()
            headers = build_snowflake_headers(token, accept='application/json')
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
            headers = build_snowflake_headers(token, accept='text/event-stream')
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

        verify_ssl = get_verify_ssl(config.get("verify_ssl", True))

        # Get agent name from config
        agent_name = st.secrets.get("features", {}).get(
            "intelligence_agent", 
            "SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT"
        )
        
        _intelligence_client = SnowflakeIntelligenceAgent(
            account=config["account"],
            user=config["user"],
            agent_name=agent_name,
            role=config.get("role"),
            verify_ssl=verify_ssl
        )
    
    return _intelligence_client
