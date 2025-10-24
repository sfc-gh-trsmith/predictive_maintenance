import streamlit as st
import logging
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from .cortex_analyst import SnowflakeCortexAnalyst, _get_cortex_client
from .snowflake_intelligence import SnowflakeIntelligenceAgent, _get_intelligence_client
from .conversation_manager import get_conversation_manager
from .assistant_ui_components import SUGGESTED_QUESTIONS, get_contextual_suggestions

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
    placeholder: str = "e.g., 'What assets have the highest risk?' or 'Create a work order for pump maintenance'",
    page_context: Optional[str] = None,
    enable_suggested_questions: bool = True,
    enable_conversation_controls: bool = True
):
    """
    Enhanced unified widget with suggested questions, conversation controls, and full analytics.
    Routes to Intelligence Agent (primary) or Cortex Analyst (fallback).
    """
    
    # Initialize client and conversation manager
    try:
        client = _get_unified_client()
        conv_manager = get_conversation_manager(storage_backend="session")
        conversation_id = conv_manager.get_conversation_id(page_context or "default")
    except Exception as e:
        st.error(f"Failed to initialize assistant: {e}")
        return

    messages_key = f"unified_messages_{semantic_model_path.replace('/', '_').replace('.', '_')}"
    
    if messages_key not in st.session_state:
        st.session_state[messages_key] = [{
            "role": "assistant",
            "content": initial_message,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    
    # Header with title and controls
    if enable_conversation_controls:
        header_col1, header_col2, header_col3 = st.columns([10, 1, 1])
    else:
        header_col1 = st.container()
    
    with header_col1:
        st.subheader(title)
        
        # Show backend indicator in debug mode
        if st.secrets.get("debug", {}).get("show_backend", False):
            backend = "Intelligence Agent" if client.use_intelligence else "Cortex Analyst"
            st.caption(f"🔧 Backend: {backend}")
    
    if enable_conversation_controls:
        # Export button
        with header_col2:
            if st.button("📤", key="export_conv", help="Export conversation"):
                export_data = conv_manager.export_conversation(conversation_id, format="markdown")
                st.download_button(
                    label="Download",
                    data=export_data,
                    file_name=f"conversation_{conversation_id}.md",
                    mime="text/markdown",
                    key="download_md"
                )
        
        # Clear button
        with header_col3:
            if st.button("🗑️", key="clear_conv", help="Clear conversation"):
                if "confirm_clear" not in st.session_state:
                    st.session_state["confirm_clear"] = False
                
                if not st.session_state["confirm_clear"]:
                    st.session_state["confirm_clear"] = True
                    st.toast("Click again to confirm clearing conversation")
                else:
                    st.session_state[messages_key] = [{
                        "role": "assistant",
                        "content": initial_message,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }]
                    conv_manager.clear_conversation(conversation_id)
                    st.session_state["confirm_clear"] = False
                    st.rerun()
    
    st.divider()
    
    # Suggested questions section (collapsible, starts collapsed)
    if enable_suggested_questions and len(st.session_state[messages_key]) <= 2:
        with st.expander("💡 Need inspiration? Try these questions:", expanded=False):
            _render_suggested_questions(
                messages_key=messages_key,
                page_context=page_context
            )
        st.divider()
    
    # Messages container
    messages_container = st.container()
    
    with messages_container:
        # Display conversation history
        for i, message in enumerate(st.session_state[messages_key]):
            with st.chat_message(message["role"]):
                st.markdown(message["content"], unsafe_allow_html=True)
                
                # Optional: Add feedback buttons for assistant messages
                if message["role"] == "assistant" and i > 0:
                    _render_feedback_buttons(f"msg_{i}")
    
    # Check for pending question from suggested questions
    pending_question = st.session_state.get("pending_question")
    if pending_question:
        del st.session_state["pending_question"]
        prompt = pending_question
    else:
        prompt = st.chat_input(placeholder)
    
    # Process the prompt (either from chat input or suggested question)
    if prompt:
        start_time = time.time()
        
        # Add user message
        user_message = {
            "role": "user",
            "content": prompt,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        st.session_state[messages_key].append(user_message)
        
        # Save to conversation manager
        conv_manager.save_message(
            conversation_id=conversation_id,
            role="user",
            content=prompt
        )
        
        # Display the user message immediately
        with messages_container:
            with st.chat_message("user"):
                st.markdown(prompt)

            # Show assistant response
            with st.chat_message("assistant"):
                with st.spinner("🤖 Thinking..."):
                    try:
                        # Prepare messages for API
                        api_messages = []
                        for msg in st.session_state[messages_key]:
                            if msg["role"] in ["user", "assistant"]:
                                api_messages.append(msg)
                        
                        # Get response from unified client
                        assistant_response, error_msg, api_content = client.get_complete_response(
                            api_messages,
                            semantic_model_path
                        )
                        
                        # Calculate response time
                        response_time_ms = int((time.time() - start_time) * 1000)
                        
                        # Debug logging
                        logger.info(f"Assistant response received: {len(assistant_response) if assistant_response else 0} characters")
                        logger.info(f"Error message: {error_msg}")
                        
                        if error_msg:
                            assistant_response = error_msg
                            st.error(error_msg)
                        elif not assistant_response or assistant_response.strip() == "":
                            assistant_response = "I apologize, but I didn't receive a proper response. Please try asking your question again."
                            st.warning("Empty response received")
                            logger.warning("Empty assistant response received")
                        
                        # Display the response
                        st.markdown(assistant_response, unsafe_allow_html=True)
                        
                        # Add assistant response to session state
                        assistant_msg = {
                            "role": "assistant",
                            "content": assistant_response,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "backend_used": "Intelligence Agent" if client.use_intelligence else "Cortex Analyst",
                            "response_time_ms": response_time_ms
                        }
                        if api_content:
                            assistant_msg["api_content"] = api_content
                        st.session_state[messages_key].append(assistant_msg)
                        
                        # Save to conversation manager
                        conv_manager.save_message(
                            conversation_id=conversation_id,
                            role="assistant",
                            content=assistant_response,
                            backend_used=assistant_msg["backend_used"],
                            response_time_ms=response_time_ms
                        )
                        
                        # Show performance metrics in debug mode
                        if st.secrets.get("debug", {}).get("show_metrics", False):
                            st.caption(f"⚡ Response time: {response_time_ms}ms | Backend: {assistant_msg['backend_used']}")

                    except Exception as e:
                        error_msg = f"🚨 Unexpected error: {str(e)}"
                        st.error(error_msg)
                        st.session_state[messages_key].append({
                            "role": "assistant",
                            "content": error_msg,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
        
        # Trigger a rerun to refresh the display
        st.rerun()


def _render_suggested_questions(messages_key: str, page_context: Optional[str] = None):
    """Render suggested questions that trigger API calls when clicked."""
    suggestions = get_contextual_suggestions(page_context) if page_context else SUGGESTED_QUESTIONS
    
    for category, questions in suggestions.items():
        st.markdown(f"**{category}**")
        cols = st.columns(2)
        for i, question in enumerate(questions[:4]):  # Limit to 4 per category
            with cols[i % 2]:
                if st.button(question, key=f"sq_{category}_{i}", use_container_width=True):
                    # Set pending question to be processed
                    st.session_state["pending_question"] = question
                    st.rerun()


def _render_feedback_buttons(message_id: str):
    """Render simple feedback buttons for assistant messages."""
    cols = st.columns([1, 1, 10])
    
    with cols[0]:
        if st.button("👍", key=f"like_{message_id}", help="Helpful"):
            _log_feedback(message_id, "positive")
            st.toast("Thanks for your feedback!")
    
    with cols[1]:
        if st.button("👎", key=f"dislike_{message_id}", help="Not helpful"):
            _log_feedback(message_id, "negative")
            st.toast("Thanks! We'll improve.")


def _log_feedback(message_id: str, rating: str):
    """Log feedback for continuous improvement."""
    if "feedback_log" not in st.session_state:
        st.session_state["feedback_log"] = []
    
    st.session_state["feedback_log"].append({
        "message_id": message_id,
        "rating": rating,
        "timestamp": datetime.now().isoformat()
    })
    logger.info(f"Feedback logged: {message_id} - {rating}")


# Legacy compatibility function
def build_analyst_widget(*args, **kwargs):
    """
    Legacy compatibility wrapper for existing build_analyst_widget calls.
    Routes to the new unified widget.
    """
    return build_unified_widget(*args, **kwargs)
