# ==================================================================================================
# VIEW SPECIFICATION: Executive Summary
# ==================================================================================================
#
# PURPOSE:
#   - Provide a high-level, at-a-glance overview of the entire manufacturing operation's health.
#   - This view is targeted at executives like the COO (Valerie Vance) who need quick,
#     aggregated insights without technical details.
#
# DATA SOURCES:
#   - Primary: `HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH`
#     - This pre-aggregated table is used for high-performance loading of the most critical KPIs:
#       - LATEST_HEALTH_SCORE
#       - AVG_FAILURE_PROBABILITY (used to calculate Production at Risk)
#   - Secondary: `HYPERFORGE.SILVER` Layer
#     - `FCT_MAINTENANCE_LOG` & `DIM_WORK_ORDER_TYPE`: Used to calculate the Maintenance Cost Ratio.
#     - `FCT_PRODUCTION_LOG`: Used to calculate the overall OEE.
#     - `DIM_ASSET`: Used to fetch `DOWNTIME_IMPACT_PER_HOUR` to calculate Production at Risk.
#
# KPIs DISPLAYED:
#   1. Overall Equipment Effectiveness (OEE): A single percentage representing company-wide efficiency.
#   2. Average Asset Health Score: The mean of the latest health scores for all monitored assets.
#   3. Production at Risk ($): The total potential revenue loss from assets with a high probability
#      of near-term failure.
#   4. Cost Avoidance (YTD): A simulated metric showing money saved by preventing failures.
#   5. PdM Program ROI: The return on investment for the predictive maintenance program.
#   6. Maintenance Cost Ratio: A pie chart visualizing the spending on Unplanned vs. Planned maintenance.
#
# VISUALIZATIONS:
#   - `st.metric`: Used for displaying the primary KPIs in large, easy-to-read "scorecards".
#   - `plotly.express.pie`: Used to render the Maintenance Cost Ratio chart.
#
# USER INTERACTION:
#   - This is primarily a read-only view.
#   - There are no input filters or interactive drill-downs on this page. Navigation to other
#     views is handled by the main `app.py` navigation menu.
#
# ==================================================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
from utils.data_loader import run_query
# Assuming a helper file exists for cleaner code
from utils.calculations import calculate_oee

def show_page():
    """Renders the Executive Summary page."""
    st.header("🏢 Executive Summary")

    # --- On-Demand Data Loading for This View ---
    # Query the GOLD layer for fast, pre-aggregated KPIs.
    gold_data_query = """
        SELECT ASSET_ID, LATEST_HEALTH_SCORE, AVG_FAILURE_PROBABILITY
        FROM HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY HOUR_TIMESTAMP DESC) = 1;
    """
    # Query SILVER layer for other necessary calculations.
    maintenance_cost_query = """
        SELECT WT.WO_TYPE_NAME, SUM(ML.PARTS_COST + ML.LABOR_COST) AS TOTAL_COST
        FROM HYPERFORGE.SILVER.FCT_MAINTENANCE_LOG ML
        JOIN HYPERFORGE.SILVER.DIM_WORK_ORDER_TYPE WT ON ML.WO_TYPE_ID = WT.WO_TYPE_ID
        GROUP BY WT.WO_TYPE_NAME;
    """
    oee_query = "SELECT * FROM HYPERFORGE.SILVER.FCT_PRODUCTION_LOG;"
    asset_dim_query = "SELECT ASSET_ID, DOWNTIME_IMPACT_PER_HOUR FROM HYPERFORGE.SILVER.DIM_ASSET WHERE IS_CURRENT = TRUE;"

    gold_data = run_query(gold_data_query)
    cost_by_type = run_query(maintenance_cost_query)
    production_data = run_query(oee_query)
    asset_dim = run_query(asset_dim_query)

    # --- KPI Calculations ---
    oee, _, _, _ = calculate_oee(production_data)
    avg_health_score = gold_data['LATEST_HEALTH_SCORE'].mean()
    asset_details_for_risk = pd.merge(gold_data, asset_dim, on='ASSET_ID')
    production_at_risk = (asset_details_for_risk['AVG_FAILURE_PROBABILITY'] * asset_details_for_risk['DOWNTIME_IMPACT_PER_HOUR'] * 24).sum()
    pdm_program_cost, cost_avoidance = 50000, 150000
    pdm_roi = (cost_avoidance - pdm_program_cost) / pdm_program_cost if pdm_program_cost > 0 else 0

    # --- UI Rendering ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Overall Equipment Effectiveness (OEE)", f"{oee:.1%}", "-5.2% vs Target", delta_color="inverse")
    col2.metric("Average Asset Health Score", f"{avg_health_score:.1f}%")
    col3.metric("Production at Risk", f"${production_at_risk:,.0f}")
    
    st.divider()
    col4, col5 = st.columns([1, 1.5])
    with col4:
        st.subheader("Financial Performance")
        st.metric("Cost Avoidance (YTD)", f"${cost_avoidance:,.0f}")
        st.metric("PdM Program ROI", f"{pdm_roi:.1%}")
    with col5:
        st.subheader("Maintenance Cost Ratio")
        fig = px.pie(cost_by_type, names='WO_TYPE_NAME', values='TOTAL_COST', title="Cost by Maintenance Type", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)