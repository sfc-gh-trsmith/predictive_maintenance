import streamlit as st
from streamlit_option_menu import option_menu

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="SnowCore Industries Predictive Maintenance Dashboard",
    page_icon="🏭",
    layout="wide"
)

from views import executive_summary, oee_drilldown, financial_risk, asset_detail, line_visualization
from utils.cortex_analyst import build_analyst_widget # <-- 1. IMPORT THE WIDGET
from utils.unified_assistant import build_unified_widget


st.title("🏭 SnowCore Industries Predictive Maintenance Dashboard")

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

# 4. Place the analyst widget in the right-hand column
with analyst_widget_col:
    build_unified_widget()