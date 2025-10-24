import streamlit as st
from streamlit_option_menu import option_menu

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="SnowCore Industries Predictive Maintenance Dashboard",
    page_icon="🏭",
    layout="wide"
)

from views import executive_summary, oee_drilldown, financial_risk, asset_detail, line_visualization
from utils.unified_assistant import build_unified_widget

# Custom CSS for better styling
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .chat-container {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        height: 600px;
        overflow-y: auto;
        border: 1px solid #dee2e6;
    }
    .chat-message {
        margin: 0.5rem 0;
        padding: 0.75rem;
        border-radius: 8px;
        max-width: 100%;
        word-wrap: break-word;
    }
    .user-message {
        background-color: #007bff;
        color: white;
        margin-left: 20%;
    }
    .assistant-message {
        background-color: #e9ecef;
        color: #333;
        margin-right: 20%;
    }
    .sql-code {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 4px;
        padding: 0.5rem;
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

st.title("🏭 SnowCore Industries Predictive Maintenance Dashboard")

# Create a fragment version of the widget for parallel loading
@st.fragment
def build_unified_widget_fragment(page_context=None):
    """Fragment wrapper to allow independent widget loading"""
    build_unified_widget(page_context=page_context)

# --- TOP NAVIGATION MENU ---
selected_page = option_menu(
    menu_title=None,
    options=["Executive Summary", "OEE Drill-Down", "Financial Risk Drill-Down", "Asset Detail", "Line Visualization"],
    icons=["building", "graph-down", "cash-coin", "search", "diagram-3"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

# --- APP LAYOUT WITH CORTEX WIDGET ---
# 2. Create columns for the main content and the analyst widget
main_content, analyst_widget_col = st.columns([2.5, 1])

# 4. Place the analyst widget in the right-hand column FIRST (so it loads in parallel)
# Using fragment decorator allows it to load independently
with analyst_widget_col:
    build_unified_widget_fragment(page_context=selected_page)

# 3. Place the page router in the main content column
with main_content:
    if selected_page == "Executive Summary":
        executive_summary.show_page()
    elif selected_page == "OEE Drill-Down":
        oee_drilldown.show_page()
    elif selected_page == "Financial Risk Drill-Down":
        financial_risk.show_page()
    elif selected_page == "Asset Detail":
        asset_detail.show_page()
    elif selected_page == "Line Visualization":
        line_visualization.show_page()