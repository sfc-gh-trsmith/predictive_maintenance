# pages/2_🔧_Fleet_Operations_Center.py
import streamlit as st
import pandas as pd
import plotly.express as px
from utils.data_loader import get_mock_data
from utils.cortex_analyst import render_chat_panel

st.set_page_config(page_title="Fleet Operations Center", layout="wide")

# --- Data Loading ---
data = get_mock_data()
df = pd.merge(data['assets'], data['health'], on='asset_id')
tech_df = data['technicians']

# --- Page Title ---
st.title("🔧 Fleet Operations Center")
st.markdown("Real-time monitoring, alert triage, and maintenance resource dispatch.")

# --- Main Layout ---
main_col, chat_col = st.columns([2.5, 1.5])

with main_col:
    # --- Asset Risk Triage List ---
    st.subheader("Asset Risk Triage List")
    
    # Filters for the triage list
    f1, f2 = st.columns(2)
    selected_type = f1.multiselect("Filter by Asset Type:", options=df['asset_type'].unique(), default=df['asset_type'].unique())
    health_threshold = f2.slider("Show assets with health score below:", 0, 100, 80)
    
    triage_df = df[(df['asset_type'].isin(selected_type)) & (df['health_score'] <= health_threshold)]
    triage_df = triage_df.sort_values('health_score').reset_index(drop=True)
    
    st.dataframe(triage_df[['asset_id', 'location', 'health_score', 'predicted_failure_mode', 'rul_days']],
                 column_config={
                     "health_score": st.column_config.ProgressColumn(
                         "Health Score",
                         help="The AI-powered health score of the asset. Lower is worse.",
                         min_value=0,
                         max_value=100,
                         format="%d"
                     ),
                     "rul_days": st.column_config.NumberColumn(
                         "RUL (Days)",
                         help="Remaining Useful Life in days until predicted failure."
                     )
                 }, use_container_width=True)

    # --- Click-to-Detail Simulation ---
    st.markdown("#### Asset Deep Dive")
    selected_asset_id = st.selectbox("Select an asset to see details:", options=triage_df['asset_id'])
    
    if selected_asset_id:
        with st.expander(f"Details for {selected_asset_id}", expanded=True):
            asset_details = df[df['asset_id'] == selected_asset_id].iloc[0]
            
            st.write(f"**Location:** {asset_details['location']} | **Type:** {asset_details['asset_type']} | **Model:** {asset_details['model']}")
            
            c1, c2 = st.columns([2,1])
            with c1:
                st.write("**Recent Sensor Readings**")
                sensor_data = data['sensors'][data['sensors']['asset_id'] == selected_asset_id]
                if not sensor_data.empty:
                    fig = px.line(sensor_data, x='timestamp', y=['temperature', 'vibration'], title="Vibration & Temperature Trend")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No detailed sensor data available for this asset.")

            with c2:
                st.write("**Maintenance History**")
                history = data['maintenance'][data['maintenance']['asset_id'] == selected_asset_id].head(5)
                st.dataframe(history[['timestamp', 'notes']], use_container_width=True, hide_index=True)

            st.write("**Recommended Actions:**")
            b1, b2, b3 = st.columns(3)
            if b1.button("Create High-Priority Work Order", key=f"wo_{selected_asset_id}"):
                st.toast(f"✅ Work Order created for {selected_asset_id} in CMMS!", icon="🛠️")
            if b2.button("Acknowledge Alert", key=f"ack_{selected_asset_id}"):
                st.toast(f"👍 Alert for {selected_asset_id} acknowledged.", icon="🔔")
            if b3.button("Snooze Alert (24h)", key=f"snz_{selected_asset_id}"):
                st.toast(f"😴 Alert for {selected_asset_id} snoozed.", icon="💤")

    st.markdown("<hr>", unsafe_allow_html=True)
    
    # --- Other Visualizations ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Technician Status & Workload")
        fig_tech = px.bar(tech_df, x='workload', y='technician', orientation='h', title="Active Work Orders per Technician")
        fig_tech.update_layout(yaxis_title=None, xaxis_title="Work Orders")
        st.plotly_chart(fig_tech, use_container_width=True)
    
    with c2:
        st.subheader("Alerts Feed")
        alerts = triage_df[triage_df['health_score'] < 50].head(5)
        for _, row in alerts.iterrows():
            st.error(f"**CRITICAL ALERT:** {row['predicted_failure_mode']} predicted on **{row['asset_id']}** in {row['location']}.")


with chat_col:
    render_chat_panel(data, 'operations')