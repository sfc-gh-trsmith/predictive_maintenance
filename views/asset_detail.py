# ==================================================================================================
# VIEW SPECIFICATION: Asset Detail View
# ==================================================================================================
#
# PURPOSE:
#   - Provide comprehensive information for a single asset and its associated sensors.
#   - Parameterized by asset selection and date range for flexible analysis.
#   - Real-time monitoring capabilities with historical context.
#
# DATA SOURCES:
#   - Primary: `HYPERFORGE.SILVER.DIM_ASSET` for asset details
#   - Primary: `HYPERFORGE.SILVER.DIM_SENSOR` for sensor information
#   - Primary: `HYPERFORGE.SILVER.FCT_ASSET_TELEMETRY` for time-series sensor data
#   - Primary: `HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH` for health metrics
#   - Secondary: `HYPERFORGE.SILVER.FCT_MAINTENANCE_LOG` for maintenance history
#
# KEY FEATURES:
#   1. Asset Selection: Dropdown with search/filter capabilities
#   2. Date Range Picker: Flexible time range selection with presets
#   3. Real-time Monitoring: Live sensor data visualization
#   4. Health Dashboard: Current health score, predictions, and alerts
#   5. Maintenance History: Recent maintenance activities and recommendations
#   6. Interactive Charts: Zoom, filter, and export capabilities
#
# USER INTERACTION:
#   - Asset selection dropdown with search functionality
#   - Date range picker with common presets (24h, 7d, 30d, custom)
#   - Real-time toggle for live monitoring
#   - Interactive charts with drill-down capabilities
#   - Export functionality for reports
#
# ==================================================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.data_loader import run_query
# Note: Cortex Analyst integration can be added later if needed

def show_page():
    """Renders the Asset Detail page."""
    st.header("🔍 Asset Detail View")
    st.markdown("Comprehensive monitoring and analysis for individual assets with real-time sensor data.")
    
    # Initialize session state for asset and date range
    if 'selected_asset_id' not in st.session_state:
        st.session_state.selected_asset_id = None
    if 'selected_date_range' not in st.session_state:
        st.session_state.selected_date_range = '7d'
    if 'real_time_mode' not in st.session_state:
        st.session_state.real_time_mode = False
    
    # --- Control Panel ---
    st.subheader("Asset Selection & Configuration")
    
    # Create two columns for controls
    control_col1, control_col2, control_col3 = st.columns([2, 1, 1])
    
    with control_col1:
        # Hierarchical asset selection
        selected_asset = display_hierarchical_asset_selection()
    
    with control_col2:
        # Date range selection
        date_presets = {
            '24h': 'Last 24 Hours',
            '7d': 'Last 7 Days', 
            '30d': 'Last 30 Days',
            'custom': 'Custom Range'
        }
        date_range = st.selectbox(
            "Time Range:",
            options=list(date_presets.keys()),
            format_func=lambda x: date_presets[x],
            index=list(date_presets.keys()).index(st.session_state.selected_date_range)
        )
        st.session_state.selected_date_range = date_range
    
    with control_col3:
        # Real-time toggle
        real_time = st.toggle("Real-time", value=st.session_state.real_time_mode)
        st.session_state.real_time_mode = real_time
    
    # Custom date range picker
    if date_range == 'custom':
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
    else:
        # Calculate date range based on preset
        end_date = datetime.now()
        if date_range == '24h':
            start_date = end_date - timedelta(hours=24)
        elif date_range == '7d':
            start_date = end_date - timedelta(days=7)
        elif date_range == '30d':
            start_date = end_date - timedelta(days=30)
    
    # --- Main Content ---
    if selected_asset:
        # Get asset details
        asset_details = get_asset_details(selected_asset)
        
        # Display asset overview
        display_asset_overview(asset_details)
        
        # Get sensor data for the selected asset and date range
        sensor_data = get_sensor_data(selected_asset, start_date, end_date)
        
        if not sensor_data.empty:
            # Display sensor monitoring dashboard
            display_sensor_dashboard(sensor_data, asset_details)
            
            # Display maintenance history
            display_maintenance_history(selected_asset, start_date, end_date)
        else:
            st.warning(f"No sensor data available for asset {selected_asset} in the selected time range.")
    else:
        st.info("Please select an asset to view detailed information.")

def display_hierarchical_asset_selection():
    """Display hierarchical asset selection (Plant → Line → Process → Asset)."""
    st.markdown("**Asset Selection**")
    
    # Initialize session state for hierarchy
    if 'selected_plant' not in st.session_state:
        st.session_state.selected_plant = None
    if 'selected_line' not in st.session_state:
        st.session_state.selected_line = None
    if 'selected_process' not in st.session_state:
        st.session_state.selected_process = None
    if 'selected_asset' not in st.session_state:
        st.session_state.selected_asset = None
    
    # Get hierarchy data
    hierarchy_data = get_hierarchy_data()
    
    if hierarchy_data.empty:
        st.error("No hierarchy data available")
        return None
    
    # Add reset button
    if st.button("🔄 Reset Selection", help="Clear all selections and start over"):
        st.session_state.selected_plant = None
        st.session_state.selected_line = None
        st.session_state.selected_process = None
        st.session_state.selected_asset = None
        st.session_state.selected_asset_id = None
        st.rerun()
    
    # Create a visual hierarchy display
    with st.expander("🏭 Manufacturing Hierarchy", expanded=True):
        # Step 1: Plant Selection
        plants = hierarchy_data['PLANT_NAME'].unique()
        selected_plant = st.selectbox(
            "🏭 Select Plant:",
            options=plants,
            index=0 if st.session_state.selected_plant is None else list(plants).index(st.session_state.selected_plant) if st.session_state.selected_plant in plants else 0,
            key="plant_selector"
        )
        st.session_state.selected_plant = selected_plant
        
        # Step 2: Line Selection (filtered by plant)
        if selected_plant:
            plant_data = hierarchy_data[hierarchy_data['PLANT_NAME'] == selected_plant]
            lines = plant_data['LINE_NAME'].unique()
            
            selected_line = st.selectbox(
                "📏 Select Production Line:",
                options=lines,
                index=0 if st.session_state.selected_line is None else list(lines).index(st.session_state.selected_line) if st.session_state.selected_line in lines else 0,
                key="line_selector"
            )
            st.session_state.selected_line = selected_line
            
            # Step 3: Process Selection (filtered by line)
            if selected_line:
                line_data = plant_data[plant_data['LINE_NAME'] == selected_line]
                processes = line_data['PROCESS_NAME'].unique()
                
                selected_process = st.selectbox(
                    "⚙️ Select Process:",
                    options=processes,
                    index=0 if st.session_state.selected_process is None else list(processes).index(st.session_state.selected_process) if st.session_state.selected_process in processes else 0,
                    key="process_selector"
                )
                st.session_state.selected_process = selected_process
                
                # Step 4: Asset Selection (filtered by process)
                if selected_process:
                    process_data = line_data[line_data['PROCESS_NAME'] == selected_process]
                    assets = process_data[['ASSET_ID', 'ASSET_NAME', 'MODEL', 'OEM_NAME']].drop_duplicates()
                    
                    selected_asset = st.selectbox(
                        "🔧 Select Asset:",
                        options=assets['ASSET_ID'],
                        format_func=lambda x: f"{assets[assets['ASSET_ID'] == x]['ASSET_NAME'].iloc[0]} ({assets[assets['ASSET_ID'] == x]['MODEL'].iloc[0]})",
                        index=0 if st.session_state.selected_asset is None else list(assets['ASSET_ID']).index(st.session_state.selected_asset) if st.session_state.selected_asset in assets['ASSET_ID'].values else 0,
                        key="asset_selector"
                    )
                    st.session_state.selected_asset = selected_asset
                    st.session_state.selected_asset_id = selected_asset
                    
                    # Display selected asset details
                    if selected_asset:
                        asset_info = assets[assets['ASSET_ID'] == selected_asset].iloc[0]
                        st.success(f"✅ Selected: **{asset_info['ASSET_NAME']}** ({asset_info['MODEL']}) by {asset_info['OEM_NAME']}")
                    
                    return selected_asset
                else:
                    return None
            else:
                return None
        else:
            return None
    
    # Display current selection path
    if st.session_state.selected_plant:
        st.info(f"📍 **Current Path:** {st.session_state.selected_plant} → {st.session_state.selected_line or 'Select Line'} → {st.session_state.selected_process or 'Select Process'} → {st.session_state.selected_asset or 'Select Asset'}")
    
    return st.session_state.selected_asset

def get_hierarchy_data():
    """Get complete hierarchy data for asset selection."""
    try:
        query = """
            SELECT 
                A.ASSET_ID,
                A.ASSET_NAME,
                A.MODEL,
                A.OEM_NAME,
                AC.CLASS_NAME,
                P.PROCESS_NAME,
                L.LINE_NAME,
                PL.PLANT_NAME
            FROM HYPERFORGE.SILVER.DIM_ASSET A
            JOIN HYPERFORGE.SILVER.DIM_ASSET_CLASS AC ON A.ASSET_CLASS_ID = AC.ASSET_CLASS_ID
            JOIN HYPERFORGE.SILVER.DIM_PROCESS P ON A.PROCESS_ID = P.PROCESS_ID
            JOIN HYPERFORGE.SILVER.DIM_LINE L ON P.LINE_ID = L.LINE_ID
            JOIN HYPERFORGE.SILVER.DIM_PLANT PL ON L.PLANT_ID = PL.PLANT_ID
            WHERE A.IS_CURRENT = TRUE
            ORDER BY PL.PLANT_NAME, L.LINE_NAME, P.PROCESS_NAME, A.ASSET_NAME
        """
        result = run_query(query)
        if result.empty:
            # Return mock hierarchy data with multiple plants and lines
            return pd.DataFrame({
                'ASSET_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'ASSET_NAME': [
                    'Primary Coolant Pump', 'Conveyor Drive Motor', 'Air Compressor Unit',
                    'Hydraulic Pump System', 'Main Drive Motor', 'Cooling Fan Assembly',
                    'Assembly Robot Arm', 'Conveyor Drive System', 'Pneumatic Press Unit',
                    'Welding Robot System', 'Material Handling Motor', 'Heat Treatment Furnace'
                ],
                'MODEL': [
                    'HydroFlow 5000', 'IronHorse 75HP', 'CompMax 200',
                    'PowerFlow 3000', 'PowerMax 50HP', 'AeroMax 1200',
                    'FlexArm 6000', 'MegaDrive 100HP', 'PowerPress 5000',
                    'WeldMaster Pro', 'FlexDrive 80HP', 'ThermoPro 3000'
                ],
                'OEM_NAME': [
                    'FlowServe', 'Siemens', 'Atlas Copco',
                    'Bosch Rexroth', 'ABB', 'Ziehl-Abegg',
                    'KUKA', 'Schneider Electric', 'SMC',
                    'Fanuc', 'Rockwell', 'Despatch'
                ],
                'CLASS_NAME': [
                    'Rotating Equipment', 'Rotating Equipment', 'Rotating Equipment',
                    'Rotating Equipment', 'Rotating Equipment', 'Electrical Systems',
                    'Control Systems', 'Rotating Equipment', 'Static Equipment',
                    'Control Systems', 'Rotating Equipment', 'Static Equipment'
                ],
                'PROCESS_NAME': [
                    'Machining Operations', 'Machining Operations', 'Machining Operations',
                    'Metal Forming', 'Metal Forming', 'Metal Forming',
                    'Robotic Assembly', 'Robotic Assembly', 'Robotic Assembly',
                    'Welding Station', 'Welding Station', 'Heat Treatment'
                ],
                'LINE_NAME': [
                    'Production Line A', 'Production Line A', 'Production Line A',
                    'Production Line B', 'Production Line B', 'Production Line B',
                    'Assembly Line 1', 'Assembly Line 1', 'Assembly Line 1',
                    'Assembly Line 2', 'Assembly Line 2', 'Assembly Line 2'
                ],
                'PLANT_NAME': [
                    'Davidson Manufacturing', 'Davidson Manufacturing', 'Davidson Manufacturing',
                    'Davidson Manufacturing', 'Davidson Manufacturing', 'Davidson Manufacturing',
                    'Charlotte Assembly', 'Charlotte Assembly', 'Charlotte Assembly',
                    'Charlotte Assembly', 'Charlotte Assembly', 'Charlotte Assembly'
                ]
            })
        return result
    except Exception as e:
        st.warning(f"Database connection issue: {str(e)}. Using mock data.")
        # Return mock hierarchy data for development/testing
        return pd.DataFrame({
            'ASSET_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'ASSET_NAME': [
                'Primary Coolant Pump', 'Conveyor Drive Motor', 'Air Compressor Unit',
                'Hydraulic Pump System', 'Main Drive Motor', 'Cooling Fan Assembly',
                'Assembly Robot Arm', 'Conveyor Drive System', 'Pneumatic Press Unit',
                'Welding Robot System', 'Material Handling Motor', 'Heat Treatment Furnace'
            ],
            'MODEL': [
                'HydroFlow 5000', 'IronHorse 75HP', 'CompMax 200',
                'PowerFlow 3000', 'PowerMax 50HP', 'AeroMax 1200',
                'FlexArm 6000', 'MegaDrive 100HP', 'PowerPress 5000',
                'WeldMaster Pro', 'FlexDrive 80HP', 'ThermoPro 3000'
            ],
            'OEM_NAME': [
                'FlowServe', 'Siemens', 'Atlas Copco',
                'Bosch Rexroth', 'ABB', 'Ziehl-Abegg',
                'KUKA', 'Schneider Electric', 'SMC',
                'Fanuc', 'Rockwell', 'Despatch'
            ],
            'CLASS_NAME': [
                'Rotating Equipment', 'Rotating Equipment', 'Rotating Equipment',
                'Rotating Equipment', 'Rotating Equipment', 'Electrical Systems',
                'Control Systems', 'Rotating Equipment', 'Static Equipment',
                'Control Systems', 'Rotating Equipment', 'Static Equipment'
            ],
            'PROCESS_NAME': [
                'Machining Operations', 'Machining Operations', 'Machining Operations',
                'Metal Forming', 'Metal Forming', 'Metal Forming',
                'Robotic Assembly', 'Robotic Assembly', 'Robotic Assembly',
                'Welding Station', 'Welding Station', 'Heat Treatment'
            ],
            'LINE_NAME': [
                'Production Line A', 'Production Line A', 'Production Line A',
                'Production Line B', 'Production Line B', 'Production Line B',
                'Assembly Line 1', 'Assembly Line 1', 'Assembly Line 1',
                'Assembly Line 2', 'Assembly Line 2', 'Assembly Line 2'
            ],
            'PLANT_NAME': [
                'Davidson Manufacturing', 'Davidson Manufacturing', 'Davidson Manufacturing',
                'Davidson Manufacturing', 'Davidson Manufacturing', 'Davidson Manufacturing',
                'Charlotte Assembly', 'Charlotte Assembly', 'Charlotte Assembly',
                'Charlotte Assembly', 'Charlotte Assembly', 'Charlotte Assembly'
            ]
        })

def get_asset_details(asset_id):
    """Get detailed information for a specific asset."""
    try:
        query = """
            SELECT 
                A.ASSET_ID,
                A.ASSET_NAME,
                A.MODEL,
                A.OEM_NAME,
                A.INSTALLATION_DATE,
                A.DOWNTIME_IMPACT_PER_HOUR,
                AC.CLASS_NAME,
                P.PROCESS_NAME,
                L.LINE_NAME,
                PL.PLANT_NAME,
                G.LATEST_HEALTH_SCORE,
                G.AVG_FAILURE_PROBABILITY,
                G.MIN_RUL_DAYS
            FROM HYPERFORGE.SILVER.DIM_ASSET A
            JOIN HYPERFORGE.SILVER.DIM_ASSET_CLASS AC ON A.ASSET_CLASS_ID = AC.ASSET_CLASS_ID
            JOIN HYPERFORGE.SILVER.DIM_PROCESS P ON A.PROCESS_ID = P.PROCESS_ID
            JOIN HYPERFORGE.SILVER.DIM_LINE L ON P.LINE_ID = L.LINE_ID
            JOIN HYPERFORGE.SILVER.DIM_PLANT PL ON L.PLANT_ID = PL.PLANT_ID
            LEFT JOIN (
                SELECT ASSET_ID, LATEST_HEALTH_SCORE, AVG_FAILURE_PROBABILITY, MIN_RUL_DAYS
                FROM HYPERFORGE.GOLD.AGG_ASSET_HOURLY_HEALTH
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY HOUR_TIMESTAMP DESC) = 1
            ) G ON A.ASSET_ID = G.ASSET_ID
            WHERE A.ASSET_ID = %s AND A.IS_CURRENT = TRUE
        """
        result = run_query(query, params=[asset_id])
        return result.iloc[0] if not result.empty else None
    except Exception as e:
        st.warning(f"Database connection issue: {str(e)}. Using mock data.")
        # Return mock data for development/testing
        return pd.Series({
            'ASSET_ID': asset_id,
            'ASSET_NAME': f'Asset {asset_id}',
            'MODEL': 'Mock Model',
            'OEM_NAME': 'Mock OEM',
            'INSTALLATION_DATE': '2022-01-01',
            'DOWNTIME_IMPACT_PER_HOUR': 5000.0,
            'CLASS_NAME': 'Rotating Equipment',
            'PROCESS_NAME': 'Machining Operations',
            'LINE_NAME': 'Production Line A',
            'PLANT_NAME': 'Davidson Manufacturing',
            'LATEST_HEALTH_SCORE': 85.5,
            'AVG_FAILURE_PROBABILITY': 0.15,
            'MIN_RUL_DAYS': 120
        })

def get_sensor_data(asset_id, start_date, end_date):
    """Get sensor data for the specified asset and date range."""
    try:
        query = """
            SELECT 
                S.SENSOR_SK,
                S.SENSOR_NK,
                S.SENSOR_TYPE,
                S.UNITS_OF_MEASURE,
                T.RECORDED_AT,
                T.TEMPERATURE_C,
                T.VIBRATION_MM_S,
                T.PRESSURE_PSI,
                T.HEALTH_SCORE,
                T.FAILURE_PROBABILITY,
                T.RUL_DAYS,
                T.IS_ANOMALOUS
            FROM HYPERFORGE.SILVER.DIM_SENSOR S
            JOIN HYPERFORGE.SILVER.FCT_ASSET_TELEMETRY T ON S.ASSET_ID = T.ASSET_ID
            WHERE S.ASSET_ID = %s 
            AND T.RECORDED_AT BETWEEN %s AND %s
            ORDER BY T.RECORDED_AT DESC
        """
        return run_query(query, params=[asset_id, start_date, end_date])
    except Exception as e:
        st.warning(f"Database connection issue: {str(e)}. Using mock data.")
        # Return mock sensor data for development/testing
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate mock time series data
        time_points = pd.date_range(start=start_date, end=end_date, freq='H')
        mock_data = []
        
        for i, timestamp in enumerate(time_points):
            mock_data.append({
                'SENSOR_SK': 1,
                'SENSOR_NK': f'sensor_{asset_id}_001',
                'SENSOR_TYPE': 'Temperature',
                'UNITS_OF_MEASURE': 'Celsius',
                'RECORDED_AT': timestamp,
                'TEMPERATURE_C': 65.0 + np.sin(i * 0.1) * 5 + np.random.normal(0, 2),
                'VIBRATION_MM_S': 0.5 + np.sin(i * 0.2) * 0.2 + np.random.normal(0, 0.1),
                'PRESSURE_PSI': 140.0 + np.sin(i * 0.05) * 10 + np.random.normal(0, 3),
                'HEALTH_SCORE': max(70, 95 - i * 0.1 + np.random.normal(0, 2)),
                'FAILURE_PROBABILITY': min(0.9, 0.1 + i * 0.001 + np.random.normal(0, 0.02)),
                'RUL_DAYS': max(1, 200 - i * 0.5 + np.random.normal(0, 5)),
                'IS_ANOMALOUS': np.random.random() < 0.1
            })
        
        return pd.DataFrame(mock_data)

def display_asset_overview(asset_details):
    """Display asset overview information."""
    if asset_details is None:
        st.error("Asset details not found.")
        return
    
    st.subheader("Asset Overview")
    
    # Create columns for asset information
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Asset Name", asset_details.get('ASSET_NAME', 'N/A'))
        st.metric("Model", asset_details.get('MODEL', 'N/A'))
        st.metric("OEM", asset_details.get('OEM_NAME', 'N/A'))
    
    with col2:
        st.metric("Plant", asset_details.get('PLANT_NAME', 'N/A'))
        st.metric("Line", asset_details.get('LINE_NAME', 'N/A'))
        st.metric("Process", asset_details.get('PROCESS_NAME', 'N/A'))
    
    with col3:
        # Health metrics
        health_score = asset_details.get('LATEST_HEALTH_SCORE', 0)
        failure_prob = asset_details.get('AVG_FAILURE_PROBABILITY', 0)
        rul_days = asset_details.get('MIN_RUL_DAYS', 0)
        
        st.metric("Health Score", f"{health_score:.1f}%", delta=None)
        st.metric("Failure Probability", f"{failure_prob:.1%}")
        st.metric("RUL (Days)", f"{rul_days}")
    
    # Health status indicator
    if health_score >= 90:
        st.success("✅ Asset is in excellent condition")
    elif health_score >= 75:
        st.warning("⚠️ Asset requires attention")
    else:
        st.error("🚨 Asset requires immediate attention")

def display_sensor_dashboard(sensor_data, asset_details):
    """Display sensor monitoring dashboard."""
    st.subheader("Sensor Monitoring Dashboard")
    
    # Get unique sensor types for this asset
    sensor_types = sensor_data['SENSOR_TYPE'].unique()
    
    # Create tabs for different sensor types
    if len(sensor_types) > 0:
        tabs = st.tabs([f"📊 {sensor_type}" for sensor_type in sensor_types])
        
        for i, sensor_type in enumerate(sensor_types):
            with tabs[i]:
                # Filter data for this sensor type
                type_data = sensor_data[sensor_data['SENSOR_TYPE'] == sensor_type]
                
                if not type_data.empty:
                    # Create time series chart
                    fig = go.Figure()
                    
                    # Add traces for different metrics
                    if 'TEMPERATURE_C' in type_data.columns and not type_data['TEMPERATURE_C'].isna().all():
                        fig.add_trace(go.Scatter(
                            x=type_data['RECORDED_AT'],
                            y=type_data['TEMPERATURE_C'],
                            mode='lines+markers',
                            name='Temperature (°C)',
                            line=dict(color='red')
                        ))
                    
                    if 'VIBRATION_MM_S' in type_data.columns and not type_data['VIBRATION_MM_S'].isna().all():
                        fig.add_trace(go.Scatter(
                            x=type_data['RECORDED_AT'],
                            y=type_data['VIBRATION_MM_S'],
                            mode='lines+markers',
                            name='Vibration (mm/s)',
                            line=dict(color='blue'),
                            yaxis='y2'
                        ))
                    
                    if 'PRESSURE_PSI' in type_data.columns and not type_data['PRESSURE_PSI'].isna().all():
                        fig.add_trace(go.Scatter(
                            x=type_data['RECORDED_AT'],
                            y=type_data['PRESSURE_PSI'],
                            mode='lines+markers',
                            name='Pressure (PSI)',
                            line=dict(color='green'),
                            yaxis='y3'
                        ))
                    
                    # Update layout
                    fig.update_layout(
                        title=f"{sensor_type} Sensor Readings Over Time",
                        xaxis_title="Time",
                        yaxis_title="Temperature (°C)",
                        yaxis2=dict(title="Vibration (mm/s)", overlaying="y", side="right"),
                        yaxis3=dict(title="Pressure (PSI)", overlaying="y", side="right"),
                        hovermode='x unified',
                        height=400
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Display current values
                    latest_data = type_data.iloc[0]
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if not pd.isna(latest_data.get('TEMPERATURE_C')):
                            st.metric("Current Temperature", f"{latest_data['TEMPERATURE_C']:.1f}°C")
                    
                    with col2:
                        if not pd.isna(latest_data.get('VIBRATION_MM_S')):
                            st.metric("Current Vibration", f"{latest_data['VIBRATION_MM_S']:.2f} mm/s")
                    
                    with col3:
                        if not pd.isna(latest_data.get('PRESSURE_PSI')):
                            st.metric("Current Pressure", f"{latest_data['PRESSURE_PSI']:.1f} PSI")
                else:
                    st.info(f"No data available for {sensor_type} sensors")

def display_maintenance_history(asset_id, start_date, end_date):
    """Display maintenance history for the asset."""
    st.subheader("Maintenance History")
    
    query = """
        SELECT 
            ML.ACTION_DATE_SK,
            ML.COMPLETED_DATE,
            ML.DOWNTIME_HOURS,
            ML.PARTS_COST,
            ML.LABOR_COST,
            ML.FAILURE_FLAG,
            WT.WO_TYPE_NAME,
            T.TECHNICIAN_NAME,
            FC.FAILURE_DESCRIPTION,
            ML.TECHNICIAN_NOTES
        FROM HYPERFORGE.SILVER.FCT_MAINTENANCE_LOG ML
        JOIN HYPERFORGE.SILVER.DIM_WORK_ORDER_TYPE WT ON ML.WO_TYPE_ID = WT.WO_TYPE_ID
        LEFT JOIN HYPERFORGE.SILVER.DIM_TECHNICIAN T ON ML.TECHNICIAN_ID = T.TECHNICIAN_ID
        LEFT JOIN HYPERFORGE.SILVER.DIM_FAILURE_CODE FC ON ML.FAILURE_CODE_ID = FC.FAILURE_CODE_ID
        WHERE ML.ASSET_ID = %s
        AND ML.COMPLETED_DATE BETWEEN %s AND %s
        ORDER BY ML.COMPLETED_DATE DESC
    """
    
    maintenance_data = run_query(query, params=[asset_id, start_date, end_date])
    
    if not maintenance_data.empty:
        # Display maintenance summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_downtime = maintenance_data['DOWNTIME_HOURS'].sum()
            st.metric("Total Downtime", f"{total_downtime:.1f} hours")
        
        with col2:
            total_cost = (maintenance_data['PARTS_COST'] + maintenance_data['LABOR_COST']).sum()
            st.metric("Total Cost", f"${total_cost:,.2f}")
        
        with col3:
            failure_count = maintenance_data['FAILURE_FLAG'].sum()
            st.metric("Failure Events", f"{failure_count}")
        
        # Display maintenance table
        st.dataframe(
            maintenance_data[['COMPLETED_DATE', 'WO_TYPE_NAME', 'DOWNTIME_HOURS', 'PARTS_COST', 'LABOR_COST', 'TECHNICIAN_NAME', 'TECHNICIAN_NOTES']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No maintenance records found for the selected time period.")

# Main execution
if __name__ == "__main__":
    show_page()
