# Dynamic Data Generation Summary

## Overview
Updated `HYPERFORGE_PREDICTIVE_MAINTENANCE.sql` to dynamically generate data for all fact tables spanning from **September 1, 2025** to the **current date** when the script is executed.

## Changes Made

### 1. **FCT_ASSET_TELEMETRY** (Hourly Sensor Data)
- **Period**: Sept 1, 2025 to current date (hourly readings)
- **Coverage**: All 18 assets
- **Features**:
  - Dynamic date range using `CURRENT_TIMESTAMP()`
  - Realistic sensor degradation patterns over time
  - Temperature, vibration, pressure readings
  - Health scores, failure probabilities, and RUL (Remaining Useful Life)
  - Anomaly detection based on thresholds
  - Hourly granularity for detailed monitoring

### 2. **FCT_MAINTENANCE_LOG** (Maintenance Events)
- **Period**: Sept 1, 2025 to current date
- **Coverage**: All 18 assets
- **Maintenance Patterns**:
  - **Preventive Maintenance (PM)**: Every 30 days
  - **Inspections**: Every 15 days
  - **Predictive Maintenance**: Every 20 days
  - **Emergency Repairs**: 2% random chance daily
- **Realistic Details**:
  - Downtime hours vary by work order type (4-8 hrs for emergency, 0.5-1.5 hrs for inspections)
  - Parts and labor costs calculated based on work order type
  - Failure codes assigned to emergency repairs
  - Technician rotation (10 technicians)
  - Contextual technician notes

### 3. **FCT_PRODUCTION_LOG** (Daily Production Metrics)
- **Period**: Sept 1, 2025 to current date (daily records)
- **Coverage**: All 18 assets
- **Features**:
  - Plant-specific planned runtime (24/7 for Davidson, 20 hrs for Charlotte lines 1&2, 18 hrs for line 3)
  - Maintenance downtime impact on actual runtime
  - Realistic production rates per asset (48-100 units/hr depending on asset)
  - Quality metrics (units scrapped increases during failures)
  - Links to maintenance events for correlated analysis

### 4. **DIM_MATERIAL** (Parts Inventory)
- **New Data**: 20 common maintenance parts/materials
- **Includes**: Bearings, seals, filters, oils, sensors, motors, valves, etc.
- **Details**: Part numbers, descriptions, suppliers, and unit costs

### 5. **FCT_MAINTENANCE_PARTS_USED** (Parts Consumption)
- **New Dynamic Generation**: Links maintenance events to parts consumed
- **Logic**:
  - Emergency repairs: 2-5 parts (critical components)
  - Predictive maintenance: 1-3 parts
  - Preventive maintenance: 2-4 parts (filters, oils, seals)
  - Inspections: 0-2 parts (minimal consumables)
- **Features**: Quantity used and total cost per part

### 6. **AGG_ASSET_HOURLY_HEALTH** (Gold Layer - Hourly Aggregates)
- **Source**: Aggregated from FCT_ASSET_TELEMETRY
- **Metrics**:
  - Average temperature per hour
  - Maximum vibration per hour
  - Standard deviation of pressure
  - Latest health score
  - Average failure probability
  - Minimum RUL (Remaining Useful Life)

### 7. **ML_FEATURE_STORE** (Gold Layer - Daily ML Features)
- **Source**: Computed from telemetry and maintenance data
- **Features**:
  - Average temperature last 24 hours
  - Vibration standard deviation over 7 days
  - Pressure trend over 7 days
  - Cycles since last preventive maintenance
  - Days since last failure
  - OEM failure rate estimate
  - Downtime impact risk
  - Target variable: Failed in next 7 days (for predictive modeling)

## Key Benefits

1. **Future-Proof**: Script automatically generates data up to current date when run
2. **Realistic**: Data includes proper degradation patterns, maintenance impacts, and correlations
3. **Scalable**: Handles data generation efficiently using CTEs and generators
4. **Complete**: All fact tables now have data (including previously empty FCT_MAINTENANCE_PARTS_USED)
5. **Integrated**: Maintenance events properly impact production metrics
6. **ML-Ready**: Feature store includes engineered features and target variables for modeling

## Execution Notes

- Total data volume scales with time (~18 assets × 24 hours × days)
- Generator row counts set conservatively (10,000 for hourly = ~13 months, 365 for daily)
- All calculations use timezone-neutral timestamps (TIMESTAMP_NTZ)
- Random variations ensure realistic data patterns without being completely deterministic

## Data Volumes (as of Oct 21, 2025)

Assuming ~51 days from Sept 1 to Oct 21:
- **FCT_ASSET_TELEMETRY**: ~22,000 records (18 assets × 24 hrs × 51 days)
- **FCT_MAINTENANCE_LOG**: ~450-550 records (depends on randomized emergency repairs)
- **FCT_PRODUCTION_LOG**: ~918 records (18 assets × 51 days)
- **FCT_MAINTENANCE_PARTS_USED**: ~900-1,500 records (2-3 parts per maintenance event on average)
- **AGG_ASSET_HOURLY_HEALTH**: ~22,000 records (hourly aggregates)
- **ML_FEATURE_STORE**: ~918 records (daily features per asset)

