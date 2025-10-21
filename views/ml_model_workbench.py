# pages/3_📈_Analytics_Workbench.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.data_loader import get_mock_data
from utils.cortex_analyst import render_chat_panel

st.set_page_config(page_title="Analytics Workbench", layout="wide")

# --- Data Loading ---
data = get_mock_data()
sensor_df = data['sensors']
maintenance_df = data['maintenance']
assets_with_sensors = sensor_df['asset_id'].unique()

# --- Page Title ---
st.title("📈 Reliability & Analytics Workbench")
st.markdown("Workspace for deep-dive analysis, model validation, and continuous improvement.")

# --- Main Layout ---
main_col, chat_col = st.columns([2.5, 1.5])

with main_col:
    # --- Control Panel ---
    st.subheader("Analysis Configuration")
    control_c1, control_c2 = st.columns(2)
    
    selected_assets = control_c1.multiselect(
        "Select Assets to Analyze:",
        options=assets_with_sensors,
        default=assets_with_sensors[0] if len(assets_with_sensors) > 0 else None
    )
    
    available_metrics = ['temperature', 'vibration', 'pressure']
    selected_metrics = control_c2.multiselect(
        "Select Sensor Metrics to Plot:",
        options=available_metrics,
        default=available_metrics[:2]
    )

    # Filter data based on selections
    plot_df = sensor_df[sensor_df['asset_id'].isin(selected_assets)]
    
    # --- Multi-Metric Time-Series Plotter ---
    st.subheader("Multi-Metric Time-Series Plotter")
    if not plot_df.empty and selected_metrics:
        fig = px.line(plot_df, x='timestamp', y=selected_metrics, color='asset_id', 
                      title="Sensor Readings Over Time", labels={'value': 'Sensor Value', 'timestamp': 'Time'})
        fig.update_layout(legend_title_text='Asset ID')
        st.plotly_chart(fig, use_container_width=True)
        
        # --- Data Export ---
        csv = plot_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export Plotted Data to CSV",
            data=csv,
            file_name=f"sensor_data_{'_'.join(selected_assets)}.csv",
            mime='text/csv',
        )
    else:
        st.warning("Please select at least one asset and one metric to display the plot.")

    st.markdown("<hr>", unsafe_allow_html=True)

    # --- Lower Section with More Tools ---
    tool_c1, tool_c2 = st.columns(2)
    
    with tool_c1:
        st.subheader("Correlation Heatmap")
        if len(selected_assets) == 1:
            single_asset_df = sensor_df[sensor_df['asset_id'] == selected_assets[0]][available_metrics]
            corr = single_asset_df.corr()
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=corr.values,
                x=corr.columns,
                y=corr.columns,
                colorscale='RdBu',
                zmin=-1, zmax=1
            ))
            fig_heatmap.update_layout(title=f"Sensor Correlation for {selected_assets[0]}")
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("Please select a single asset to view its correlation heatmap.")

        st.subheader("AI Feature Importance")
        feature_importance_data = {
            'Feature': ['VIBRATION_STDDEV_7D', 'AVG_TEMP_LAST_24H', 'PRESSURE_SPIKE_COUNT', 'RUNTIME_HOURS_CUM'],
            'Importance': [0.45, 0.30, 0.15, 0.10]
        }
        fig_feat = px.bar(pd.DataFrame(feature_importance_data), x='Importance', y='Feature', orientation='h',
                          title="Top Features for Failure Prediction (Pump Model)")
        st.plotly_chart(fig_feat, use_container_width=True)
            
    with tool_c2:
        st.subheader("Maintenance History Log")
        st.write("Historical work orders for the selected asset(s).")
        filtered_logs = maintenance_df[maintenance_df['asset_id'].isin(selected_assets)]
        st.dataframe(filtered_logs[['timestamp', 'asset_id', 'notes', 'technician']], use_container_width=True)
        
        st.subheader("Model Feedback")
        st.write("Was the last prediction for this asset accurate?")
        feedback_c1, feedback_c2 = st.columns(2)
        if feedback_c1.button("Prediction was Correct 👍"):
            st.success("Thank you! Your feedback will be used for model retraining.")
        if feedback_c2.button("Prediction was Incorrect 👎"):
            st.warning("Thank you! Flagging this event for review by the data science team.")

with chat_col:
    render_chat_panel(data, 'analytics')