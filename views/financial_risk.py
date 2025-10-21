# ==================================================================================================
# VIEW SPECIFICATION: Financial Risk Drill-Down
# ==================================================================================================
#
# PURPOSE:
#   - To quantify and visualize the financial risk associated with potential asset failures.
#   - Allows users to identify which types of assets and which specific machines pose the
#     greatest financial threat to the operation.
#
# DATA SOURCES:
#   - Primary: `HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH`
#     - This table provides the core risk factors (failure probability, RUL) in a pre-aggregated
#       format for maximum performance.
#   - Secondary: `HYPERFORGE.SILVER` Layer
#     - `DIM_ASSET`: Joined to get asset names and the crucial `DOWNTIME_IMPACT_PER_HOUR` value.
#     - `DIM_ASSET_CLASS`: Joined to get the descriptive name for each asset category.
#
# FUNCTIONALITY & DRILL PATH:
#   1. Risk Contribution by Asset Class (Level 1):
#      - The view calculates the "Production at Risk" for every asset.
#      - It then aggregates this risk by asset class and displays it in a treemap, making it
#        easy to see which categories (e.g., "CNC Machines") contribute the most risk.
#   2. Critical Asset Matrix (Level 2):
#      - A dropdown allows the user to filter the view to a single asset class.
#      - The view then renders a bubble chart (risk matrix) for all assets in that class.
#      - This matrix plots Failure Probability vs. Financial Impact, with bubble size indicating
#        urgency (e.g., tied to risk amount). Assets in the top-right quadrant are the most critical.
#   3. Asset Action Plan (Level 3):
#      - Based on the matrix, the view automatically identifies the single highest-risk asset
#        in the selected category and displays a summary "Action Plan" card with key stats (RUL,
#        Failure Probability) and a recommended action.
#
# VISUALIZATIONS:
#   - `plotly.express.treemap`: Used to show the proportional risk contribution of each asset class.
#   - `plotly.express.scatter`: Used to render the interactive bubble chart (risk matrix).
#
# USER INTERACTION:
#   - `st.selectbox`: Allows the user to filter the risk matrix by asset class.
#   - Interactive Charts: The user can hover over the treemap and bubble chart segments to
#     see detailed tooltips with specific values.
#
# ==================================================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
from utils.data_loader import run_query

def show_page():
    """Renders the Financial Risk Drill-Down page."""
    st.header("💰 Financial Risk Drill-Down")

    # --- On-Demand Data Loading ---
    # This query joins the GOLD layer health data with SILVER dimensions for context.
    query = """
        SELECT
            G.ASSET_ID,
            A.ASSET_NAME,
            AC.CLASS_NAME,
            A.DOWNTIME_IMPACT_PER_HOUR,
            G.AVG_FAILURE_PROBABILITY,
            G.LATEST_HEALTH_SCORE,
            G.MIN_RUL_DAYS
        FROM HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH G
        JOIN HYPERFORGE.SILVER.DIM_ASSET A ON G.ASSET_ID = A.ASSET_ID AND A.IS_CURRENT = TRUE
        JOIN HYPERFORGE.SILVER.DIM_ASSET_CLASS AC ON A.ASSET_CLASS_ID = AC.ASSET_CLASS_ID
        QUALIFY ROW_NUMBER() OVER (PARTITION BY G.ASSET_ID ORDER BY G.HOUR_TIMESTAMP DESC) = 1;
    """
    df_risk = run_query(query)
    df_risk['PRODUCTION_AT_RISK'] = df_risk['AVG_FAILURE_PROBABILITY'] * df_risk['DOWNTIME_IMPACT_PER_HOUR'] * 24

    # --- UI Rendering ---
    st.subheader("1. Risk Contribution by Asset Class")
    risk_by_class = df_risk.groupby('CLASS_NAME')['PRODUCTION_AT_RISK'].sum().reset_index()
    fig_treemap = px.treemap(risk_by_class, path=['CLASS_NAME'], values='PRODUCTION_AT_RISK', title='Total Production at Risk by Asset Class', color='PRODUCTION_AT_RISK', color_continuous_scale='Reds')
    st.plotly_chart(fig_treemap, use_container_width=True)

    st.subheader("2. Critical Asset Risk Matrix")
    selected_class = st.selectbox("Select Asset Class to Analyze:", options=df_risk['CLASS_NAME'].unique())
    
    if selected_class:
        class_df = df_risk[df_risk['CLASS_NAME'] == selected_class]
        fig_bubble = px.scatter(
            class_df, x="AVG_FAILURE_PROBABILITY", y="DOWNTIME_IMPACT_PER_HOUR", size="PRODUCTION_AT_RISK",
            color="LATEST_HEALTH_SCORE", color_continuous_scale="RdYlGn_r", hover_name="ASSET_NAME",
            title=f"Risk Matrix for {selected_class}", size_max=60
        )
        st.plotly_chart(fig_bubble, use_container_width=True)
        
        st.subheader("3. Asset Action Plan")
        if not class_df.empty:
            high_risk_asset = class_df.sort_values("PRODUCTION_AT_RISK", ascending=False).iloc[0]
            st.error(f"**Highest Risk Asset Identified:** {high_risk_asset['ASSET_NAME']}")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Remaining Useful Life (RUL)", f"{high_risk_asset['MIN_RUL_DAYS']} days")
                st.metric("Failure Probability", f"{high_risk_asset['AVG_FAILURE_PROBABILITY']:.0%}")
            with col2:
                st.write("**Predicted Failure Mode:** Spindle Bearing Wear")
                st.write("**Recommended Action:** Schedule immediate inspection and prepare for parts replacement.")
        else:
            st.info(f"No assets found for the class '{selected_class}'.")