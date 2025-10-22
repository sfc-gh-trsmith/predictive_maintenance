import streamlit as st
import logging
from typing import List, Dict, Optional, Tuple
from .cortex_analyst import SnowflakeCortexAnalyst, _get_cortex_client
from .snowflake_intelligence import SnowflakeIntelligenceAgent, _get_intelligence_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnifiedAssistant:
    """
    Unified interface that routes between Snowflake Intelligence Agent (primary)
    and Cortex Analyst (fallback/rollback) based on configuration.
    """
    
    def __init__(self):
        self.use_intelligence = self._get_feature_flag()
        self.fallback_to_cortex = self._get_fallback_flag()
        self.intelligence_client = None
        self.cortex_client = None
        
        logger.info(f"UnifiedAssistant initialized - Intelligence: {self.use_intelligence}, Fallback: {self.fallback_to_cortex}")
    
    def _get_feature_flag(self) -> bool:
        """Check if Intelligence should be used (config-driven)"""
        try:
            return st.secrets.get("features", {}).get("use_intelligence", True)
        except Exception:
            # Default to Cortex if secrets not available
            return False
    
    def _get_fallback_flag(self) -> bool:
        """Check if automatic fallback to Cortex is enabled"""
        try:
            return st.secrets.get("features", {}).get("fallback_to_cortex", True)
        except Exception:
            return True
    
    def get_complete_response(self, messages: List[Dict], semantic_model_path: str) -> Tuple[str, Optional[str], Optional[List]]:
        """
        Single entry point that routes to appropriate backend.
        
        Args:
            messages: Conversation messages
            semantic_model_path: Path to semantic model (used by Cortex fallback)
            
        Returns:
            Tuple of (response_text, error_message, api_content)
        """
        
        if self.use_intelligence:
            try:
                logger.info("🧠 Attempting Intelligence Agent response...")
                response, error = self._get_intelligence_response(messages)
                return response, error, None  # Intelligence Agent doesn't return api_content
            except Exception as e:
                logger.warning(f"Intelligence Agent failed: {str(e)}")
                
                if self.fallback_to_cortex:
                    logger.info("🔄 Falling back to Cortex Analyst...")
                    try:
                        response, error, api_content = self._get_cortex_response(messages, semantic_model_path)
                        if not error:
                            # Add fallback indicator to response
                            fallback_note = "\n\n*Note: Response provided by Cortex Analyst (Intelligence Agent temporarily unavailable)*"
                            response = response + fallback_note
                        return response, error, api_content
                    except Exception as cortex_error:
                        return "", f"🚨 Both Intelligence Agent and Cortex Analyst failed. Intelligence: {str(e)}, Cortex: {str(cortex_error)}", None
                else:
                    return "", f"🚨 Intelligence Agent failed: {str(e)}", None
        else:
            logger.info("🔍 Using Cortex Analyst (Intelligence disabled)")
            return self._get_cortex_response(messages, semantic_model_path)
    
    def _get_intelligence_response(self, messages: List[Dict]) -> Tuple[str, Optional[str]]:
        """Get response from Snowflake Intelligence Agent"""
        if self.intelligence_client is None:
            self.intelligence_client = _get_intelligence_client()
        
        return self.intelligence_client.get_complete_response(messages)
    
    def _get_cortex_response(self, messages: List[Dict], semantic_model_path: str) -> Tuple[str, Optional[str], Optional[List]]:
        """Get response from Cortex Analyst (fallback)"""
        if self.cortex_client is None:
            self.cortex_client = _get_cortex_client()
        
        return self.cortex_client.get_complete_response(messages, semantic_model_path)


# Global unified client instance
_unified_client: Optional[UnifiedAssistant] = None

def _get_unified_client() -> UnifiedAssistant:
    """Get or create the global unified client instance"""
    global _unified_client
    
    if _unified_client is None:
        _unified_client = UnifiedAssistant()
    
    return _unified_client


def build_unified_widget(
    title: str = "SnowCore Industries Assistant 🤖",
    semantic_model_path: str = "HYPERFORGE.GOLD.SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml",
    initial_message: str = "Hi! I'm your intelligent manufacturing assistant. I can analyze data and help with maintenance operations.",
    placeholder: str = "e.g., 'What assets have the highest risk?' or 'Create a work order for pump maintenance'"
):
    """
    Unified widget that routes to Intelligence Agent (primary) or Cortex Analyst (fallback).
    Interface is identical to the original cortex_analyst widget.
    """
    st.subheader(title)
    
    try:
        client = _get_unified_client()
        
        # Show current backend in debug mode
        if st.secrets.get("debug", {}).get("show_backend", False):
            backend = "Intelligence Agent" if client.use_intelligence else "Cortex Analyst"
            st.caption(f"Backend: {backend}")
            
    except Exception as e:
        st.error(f"Failed to initialize assistant: {e}")
        return

    messages_key = f"unified_messages_{semantic_model_path.replace('/', '_').replace('.', '_')}"
    
    if messages_key not in st.session_state:
        st.session_state[messages_key] = [{"role": "assistant", "content": initial_message}]
    
    # Create container for messages to ensure proper layout
    messages_container = st.container()
    
    with messages_container:
        # Display conversation history
        for message in st.session_state[messages_key]:
            with st.chat_message(message["role"]):
                st.markdown(message["content"], unsafe_allow_html=True)

    # Handle user input - this stays at the bottom and triggers a rerun
    if prompt := st.chat_input(placeholder):
        # Add user message to session state immediately
        st.session_state[messages_key].append({"role": "user", "content": prompt})
        
        # Display the user message immediately
        with messages_container:
            with st.chat_message("user"):
                st.markdown(prompt)

            # Show assistant response
            with st.chat_message("assistant"):
                with st.spinner("🤖 Thinking..."):
                    try:
                        # Prepare messages for API (filter out non-API roles if needed)
                        api_messages = []
                        for msg in st.session_state[messages_key]:
                            if msg["role"] in ["user", "assistant"]:
                                api_messages.append(msg)
                        
                        # Get response from unified client
                        assistant_response, error_msg, api_content = client.get_complete_response(api_messages, semantic_model_path)
                        
                        # Debug logging
                        logger.info(f"Assistant response received: {len(assistant_response) if assistant_response else 0} characters")
                        logger.info(f"Error message: {error_msg}")
                        
                        if error_msg:
                            assistant_response = error_msg
                            st.error(error_msg)
                        elif not assistant_response or assistant_response.strip() == "":
                            assistant_response = "I apologize, but I didn't receive a proper response. Please try asking your question again."
                            st.warning("Empty response received - this may indicate a streaming parsing issue")
                            logger.warning("Empty assistant response received")
                        
                        # Display the response
                        st.markdown(assistant_response, unsafe_allow_html=True)
                        
                        # Add assistant response to session state with API content for followup
                        assistant_msg = {"role": "assistant", "content": assistant_response}
                        if api_content:
                            assistant_msg["api_content"] = api_content
                        st.session_state[messages_key].append(assistant_msg)

                    except Exception as e:
                        error_msg = f"🚨 Unexpected error: {str(e)}"
                        st.error(error_msg)
                        st.session_state[messages_key].append({"role": "assistant", "content": error_msg})
        
        # Trigger a rerun to refresh the display
        st.rerun()


# Legacy compatibility function
def build_analyst_widget(*args, **kwargs):
    """
    Legacy compatibility wrapper for existing build_analyst_widget calls.
    Routes to the new unified widget.
    """
    return build_unified_widget(*args, **kwargs)
