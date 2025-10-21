USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROLE HYPERFORGE_ROLE;

/*************************************************************************************************/
-- HYPERFORGE PREDICTIVE MAINTENANCE DATABASE
-- Version: 1.0
-- Description: DDL and sample DML for the Bronze, Silver, and Gold layers.
/*************************************************************************************************/

-- Step 0: Setup Database and Schemas
CREATE OR REPLACE DATABASE HYPERFORGE;


USE DATABASE HYPERFORGE;
USE SCHEMA PUBLIC;

GRANT CREATE TABLE ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE PROCEDURE ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE FUNCTION ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE SEQUENCE ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE STREAMLIT ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE SEMANTIC VIEW ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE MATERIALIZED VIEW ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
CREATE OR REPLACE SCHEMA BRONZE COMMENT = 'Schema for raw, unaltered source data';
CREATE OR REPLACE SCHEMA SILVER COMMENT = 'Schema for cleaned, conformed, and integrated data (Star Schema)';
CREATE OR REPLACE SCHEMA GOLD COMMENT = 'Schema for business-level aggregates and ML feature stores';

-- Create an event table if it doesn't already exist
CREATE or replace EVENT TABLE HYPERFORGE.PUBLIC.HYPERFORGE_EVENTS;
-- Associate the event table with the account
ALTER ACCOUNT SET EVENT_TABLE = HYPERFORGE.PUBLIC.HYPERFORGE_EVENTS;

-- Set the log level for the database containing your app
ALTER DATABASE HYPERFORGE SET LOG_LEVEL = INFO;

-- Set the trace level for the database containing your app
ALTER DATABASE HYPERFORGE SET TRACE_LEVEL = ON_EVENT;

GRANT CREATE STAGE ON ALL SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE STAGE ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE STREAMLIT ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT USAGE ON ALL STAGES IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;

-- Grants for the Snowflake Intelligence Roles
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE HYPERFORGE_ROLE;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE HYPERFORGE_ROLE;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE HYPERFORGE_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE HYPERFORGE_ROLE;
GRANT CREATE AGENT ON ALL SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE AGENT ON FUTURE SCHEMAS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT CREATE AGENT ON SCHEMA GOLD TO ROLE HYPERFORGE_ROLE;


GRANT SELECT ON ALL SEMANTIC VIEWS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;
GRANT SELECT ON FUTURE SEMANTIC VIEWS IN DATABASE HYPERFORGE TO ROLE HYPERFORGE_ROLE;

-- Create warehouse for HyperForge
CREATE WAREHOUSE IF NOT EXISTS HYPERFORGE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Default Warehouse for HyperForge';

-- Create warehouse for Streamlit apps
CREATE WAREHOUSE IF NOT EXISTS HYPERFORGE_STREAMLIT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for HyperForge Streamlit applications';

-- Grant warehouse usage to roles
GRANT USAGE ON WAREHOUSE HYPERFORGE_STREAMLIT_WH TO ROLE HYPERFORGE_ROLE;

-- Grant the new role to user 
SET MY_USER = (SELECT CURRENT_USER());   
GRANT ROLE HYPERFORGE_ROLE TO USER IDENTIFIER($MY_USER);


USE ROLE HYPERFORGE_ROLE;


---------------------------------------------------------------------------------------------------
-- ## BRONZE LAYER (Raw & Staging)
-- Tables in this layer use the VARIANT data type to land semi-structured JSON as-is.
---------------------------------------------------------------------------------------------------
USE SCHEMA HYPERFORGE.BRONZE;

CREATE OR REPLACE TABLE RAW_IOT_TELEMETRY (
    RAW_PAYLOAD         VARIANT,
    SOURCE_TIMESTAMP    TIMESTAMP_NTZ,
    INGESTION_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_MAINTENANCE_LOGS (
    LOG_DATA            VARIANT,
    SOURCE_FILENAME     VARCHAR,
    INGESTION_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_EQUIPMENT_MASTER (
    EQUIPMENT_DATA      VARIANT,
    INGESTION_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


---------------------------------------------------------------------------------------------------
-- ## SILVER LAYER (Conformed & Integrated Star Schema)
-- This is the single source of truth, structured for analytics.
-- Rationalized to align with plant hierarchy and business impact modeling
---------------------------------------------------------------------------------------------------
USE SCHEMA HYPERFORGE.SILVER;

-- Dimension Tables (The "Who, What, Where")

-- Date Dimension (Standard)
CREATE OR REPLACE TABLE DIM_DATE (
    DATE_SK         NUMBER(8) PRIMARY KEY, -- YYYYMMDD
    FULL_DATE       DATE NOT NULL,
    DAY_OF_WEEK     VARCHAR(10),
    MONTH_NAME      VARCHAR(10),
    QUARTER         NUMBER(1),
    YEAR            NUMBER(4)
);

-- Plant Dimension (Location Hierarchy Level 1)
CREATE OR REPLACE TABLE DIM_PLANT (
    PLANT_ID        NUMBER(10,0) PRIMARY KEY,
    PLANT_NAME      VARCHAR(100),
    LOCATION        VARCHAR(100)
);

-- Production Line Dimension (Location Hierarchy Level 2)
CREATE OR REPLACE TABLE DIM_LINE (
    LINE_ID         NUMBER(10,0) PRIMARY KEY,
    PLANT_ID        NUMBER(10,0),
    LINE_NAME       VARCHAR(100),
    HOURLY_REVENUE  NUMBER(10,2), -- Used for calculating revenue loss
    FOREIGN KEY (PLANT_ID) REFERENCES DIM_PLANT(PLANT_ID)
);

-- Asset Class Dimension (Asset Categorization)
CREATE OR REPLACE TABLE DIM_ASSET_CLASS (
    ASSET_CLASS_ID  NUMBER(10,0) PRIMARY KEY,
    CLASS_NAME      VARCHAR(100)
);

-- Asset Dimension (Central Dimension - replaces DIM_EQUIPMENT)
CREATE OR REPLACE TABLE DIM_ASSET (
    ASSET_ID                INTEGER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    ASSET_NK                VARCHAR(50) NOT NULL, -- Natural Key (Serial Number)
    ASSET_NAME              VARCHAR(100),
    MODEL                   VARCHAR(50),
    OEM_NAME                VARCHAR(50),
    LINE_ID                 NUMBER(10,0),
    ASSET_CLASS_ID          NUMBER(10,0),
    INSTALLATION_DATE       DATE,
    DOWNTIME_IMPACT_PER_HOUR NUMBER(12,2), -- Used for "Production at Risk" KPI
    -- For Slowly Changing Dimensions (Type 2)
    SCD_START_DATE          TIMESTAMP_NTZ NOT NULL,
    SCD_END_DATE            TIMESTAMP_NTZ,
    IS_CURRENT              BOOLEAN,
    FOREIGN KEY (LINE_ID) REFERENCES DIM_LINE(LINE_ID),
    FOREIGN KEY (ASSET_CLASS_ID) REFERENCES DIM_ASSET_CLASS(ASSET_CLASS_ID)
);

-- Work Order Type Dimension (Enhanced maintenance categorization)
CREATE OR REPLACE TABLE DIM_WORK_ORDER_TYPE (
    WO_TYPE_ID      NUMBER(10,0) PRIMARY KEY,
    WO_TYPE_NAME    VARCHAR(50), -- e.g., 'Unplanned Emergency', 'Planned Predictive', 'Planned Preventive'
    WO_TYPE_CODE    VARCHAR(10)
);

-- Sensor Dimension (Retained for detailed sensor tracking)
CREATE OR REPLACE TABLE DIM_SENSOR (
    SENSOR_SK       INTEGER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    SENSOR_NK       VARCHAR(50) NOT NULL, -- Natural Key (Sensor UUID)
    ASSET_ID        INTEGER, -- Foreign Key to DIM_ASSET
    SENSOR_TYPE     VARCHAR(50),
    UNITS_OF_MEASURE VARCHAR(20),
    FOREIGN KEY (ASSET_ID) REFERENCES DIM_ASSET(ASSET_ID)
);

-- Fact Tables (The "Measurements and Events")

-- Time-series sensor data and ML predictions (Consolidated telemetry)
CREATE OR REPLACE TABLE FCT_ASSET_TELEMETRY (
    TELEMETRY_ID        NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    ASSET_ID            INTEGER NOT NULL,
    DATE_SK             NUMBER(8) NOT NULL,
    RECORDED_AT         TIMESTAMP_NTZ,
    TEMPERATURE_C       NUMBER(5,2),
    VIBRATION_MM_S      NUMBER(5,2),
    PRESSURE_PSI        NUMBER(6,2),
    HEALTH_SCORE        NUMBER(5,2), -- e.g., 0-100
    FAILURE_PROBABILITY NUMBER(3,2), -- e.g., 0-1.0
    RUL_DAYS            NUMBER(5,0), -- Remaining Useful Life in days
    IS_ANOMALOUS        BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (ASSET_ID) REFERENCES DIM_ASSET(ASSET_ID)
) COMMENT = 'Consolidated telemetry with ML predictions and health scores'
CLUSTER BY (ASSET_ID, RECORDED_AT); -- Optimized for time-series queries on specific assets

-- Log of all maintenance activities (Enhanced)
CREATE OR REPLACE TABLE FCT_MAINTENANCE_LOG (
    LOG_ID              NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    ASSET_ID            INTEGER NOT NULL,
    WO_TYPE_ID          NUMBER(10,0) NOT NULL,
    ACTION_DATE_SK      NUMBER(8) NOT NULL,
    COMPLETED_DATE      DATE,
    DOWNTIME_HOURS      NUMBER(5,1),
    PARTS_COST          NUMBER(10,2),
    LABOR_COST          NUMBER(10,2),
    FAILURE_FLAG        BOOLEAN COMMENT 'TRUE if this action was in response to a failure',
    TECHNICIAN_ID       NUMBER(38,0) COMMENT 'Foreign key to DIM_TECHNICIAN',
    FAILURE_CODE_ID     NUMBER(38,0) COMMENT 'Foreign key to DIM_FAILURE_CODE, populated when FAILURE_FLAG is TRUE',
    TECHNICIAN_NOTES    VARCHAR(1000),
    FOREIGN KEY (ASSET_ID) REFERENCES DIM_ASSET(ASSET_ID),
    FOREIGN KEY (WO_TYPE_ID) REFERENCES DIM_WORK_ORDER_TYPE(WO_TYPE_ID),
    FOREIGN KEY (TECHNICIAN_ID) REFERENCES DIM_TECHNICIAN(TECHNICIAN_ID),
    FOREIGN KEY (FAILURE_CODE_ID) REFERENCES DIM_FAILURE_CODE(FAILURE_CODE_ID)
) CLUSTER BY (ASSET_ID, ACTION_DATE_SK);

-- Daily production summary for OEE calculations (New)
CREATE OR REPLACE TABLE FCT_PRODUCTION_LOG (
    PROD_LOG_ID         NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    ASSET_ID            INTEGER NOT NULL,
    DATE_SK             NUMBER(8) NOT NULL,
    PRODUCTION_DATE     DATE,
    PLANNED_RUNTIME_HOURS   NUMBER(4,1),
    ACTUAL_RUNTIME_HOURS    NUMBER(4,1), -- Drives OEE "Availability"
    UNITS_PRODUCED      NUMBER(10,0), -- Drives OEE "Performance"
    UNITS_SCRAPPED      NUMBER(10,0), -- Drives OEE "Quality"
    FOREIGN KEY (ASSET_ID) REFERENCES DIM_ASSET(ASSET_ID)
) CLUSTER BY (ASSET_ID, PRODUCTION_DATE);

CREATE OR REPLACE TABLE DIM_TECHNICIAN (
    TECHNICIAN_ID   NUMBER(38,0) AUTOINCREMENT  PRIMARY KEY,
    EMPLOYEE_NK     VARCHAR(20) NOT NULL, -- Natural Key from HR system
    TECHNICIAN_NAME VARCHAR(100),
    CRAFT           VARCHAR(50), -- e.g., 'Mechanic', 'Electrician', 'Instrumentation'
    SHIFT           VARCHAR(10),
    HIRE_DATE       DATE,
    IS_ACTIVE       BOOLEAN
);

CREATE OR REPLACE TABLE DIM_FAILURE_CODE (
    FAILURE_CODE_ID     NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    FAILURE_HIERARCHY_1 VARCHAR(50), -- e.g., 'Mechanical', 'Electrical', 'Operational'
    FAILURE_HIERARCHY_2 VARCHAR(50), -- e.g., 'Bearing', 'Motor', 'Seal'
    FAILURE_HIERARCHY_3 VARCHAR(50), -- e.g., 'Over-lubrication', 'Misalignment', 'Contamination'
    FAILURE_DESCRIPTION VARCHAR(255)
);

CREATE OR REPLACE TABLE DIM_MATERIAL (
    MATERIAL_ID         INTEGER PRIMARY KEY,
    MATERIAL_NK         VARCHAR(50) NOT NULL, -- Part Number / SKU
    MATERIAL_DESC       VARCHAR(255),
    SUPPLIER_NAME       VARCHAR(100),
    UNIT_COST           NUMBER(10,2)
);

CREATE OR REPLACE TABLE FCT_MAINTENANCE_PARTS_USED (
    LOG_ID              NUMBER(38,0) NOT NULL, -- Foreign Key to FCT_MAINTENANCE_LOG
    MATERIAL_ID         INTEGER NOT NULL,      -- Foreign Key to DIM_MATERIAL
    QUANTITY_USED       NUMBER(8,2),
    TOTAL_COST          NUMBER(10,2),
    PRIMARY KEY (LOG_ID, MATERIAL_ID)
);

CREATE OR REPLACE TABLE FCT_BUDGET (
    BUDGET_ID           INTEGER PRIMARY KEY,
    PLANT_ID            NUMBER(10,0),
    YEAR                NUMBER(4),
    QUARTER             NUMBER(1),
    BUDGET_TYPE         VARCHAR(50), -- e.g., 'OpEx Maintenance', 'CapEx Project'
    BUDGET_AMOUNT       NUMBER(15,2),
    FOREIGN KEY (PLANT_ID) REFERENCES DIM_PLANT(PLANT_ID)
);


---------------------------------------------------------------------------------------------------
-- ## GOLD LAYER (Application & Feature Store)
-- Purpose-built tables for high-speed dashboards and ML model training.
---------------------------------------------------------------------------------------------------
USE SCHEMA HYPERFORGE.GOLD;

CREATE OR REPLACE TABLE AGG_ASSET_HOURLY_HEALTH (
    HOUR_TIMESTAMP          TIMESTAMP_NTZ,
    ASSET_ID                INTEGER,
    AVG_TEMPERATURE_C       FLOAT,
    MAX_VIBRATION_MM_S      FLOAT,
    STDDEV_PRESSURE_PSI     FLOAT,
    LATEST_HEALTH_SCORE     NUMBER(5, 2),
    AVG_FAILURE_PROBABILITY NUMBER(3, 2),
    MIN_RUL_DAYS            NUMBER(5, 0)
);

CREATE OR REPLACE TABLE ML_FEATURE_STORE (
    OBSERVATION_DATE_SK     NUMBER(8),
    ASSET_ID                INTEGER,
    -- Example Features
    AVG_TEMP_LAST_24H       FLOAT,
    VIBRATION_STDDEV_7D     FLOAT,
    PRESSURE_TREND_7D       FLOAT,
    CYCLES_SINCE_LAST_PM    INTEGER,
    DAYS_SINCE_LAST_FAILURE INTEGER,
    OEM_FAILURE_RATE_EST    FLOAT,
    DOWNTIME_IMPACT_RISK    NUMBER(12, 2), -- Calculated risk based on asset downtime impact
    -- Target Variable
    FAILED_IN_NEXT_7_DAYS   BOOLEAN
);

/*************************************************************************************************/
-- Step 2: Insert Sample Data (DML)
/*************************************************************************************************/

-- Insert into BRONZE Layer
USE SCHEMA HYPERFORGE.BRONZE;
INSERT INTO RAW_EQUIPMENT_MASTER (EQUIPMENT_DATA)
SELECT PARSE_JSON(column1) FROM VALUES
('{ "serialNumber": "EQ-PUMP-001", "model": "HydroFlow 5000", "oem": "FlowServe", "installDate": "2022-01-15", "assetType": "Centrifugal Pump", "plant": "Davidson NC"}'),
('{ "serialNumber": "EQ-MOTOR-007", "model": "IronHorse 75HP", "oem": "Siemens", "installDate": "2021-11-20", "assetType": "Induction Motor", "plant": "Davidson NC"}');

INSERT INTO RAW_IOT_TELEMETRY (RAW_PAYLOAD, SOURCE_TIMESTAMP)
SELECT PARSE_JSON(column1), column2::TIMESTAMP_NTZ FROM VALUES
-- Normal readings for Pump 001
('{ "deviceId": "EQ-PUMP-001-VIB", "metric": "vibration", "value": 0.51, "unit": "mm/s" }', '2025-09-22 10:00:00'),
('{ "deviceId": "EQ-PUMP-001-TMP", "metric": "temperature", "value": 65.2, "unit": "C" }', '2025-09-22 10:00:00'),
('{ "deviceId": "EQ-PUMP-001-VIB", "metric": "vibration", "value": 0.55, "unit": "mm/s" }', '2025-09-22 11:00:00'),
-- Anomalous reading
('{ "deviceId": "EQ-PUMP-001-VIB", "metric": "vibration", "value": 2.15, "unit": "mm/s" }', '2025-09-23 08:00:00'),
('{ "deviceId": "EQ-PUMP-001-TMP", "metric": "temperature", "value": 75.8, "unit": "C" }', '2025-09-23 08:00:00');

INSERT INTO RAW_MAINTENANCE_LOGS (LOG_DATA)
SELECT PARSE_JSON(column1) FROM VALUES
('{ "workOrderId": "WO-9987", "assetId": "EQ-PUMP-001", "type": "CM", "notes": "High vibration detected. Found bearing misalignment.", "downtimeHours": 4, "laborCost": 600, "partsCost": 250, "failure": true, "date": "2025-09-23"}');


-- Insert into SILVER Layer
USE SCHEMA HYPERFORGE.SILVER;
-- Populate Dimensions first
SET (START_DATE, END_DATE) = ('1995-01-01', '2030-12-31');
SET GENERATOR_RECORD_COUNT = (select DATEDIFF(DAY, $START_DATE, $END_DATE) + 1);
INSERT OVERWRITE INTO DIM_DATE (DATE_SK, FULL_DATE, DAY_OF_WEEK, MONTH_NAME, QUARTER, YEAR)
SELECT 
    TO_NUMBER(TO_CHAR(d.DATE, 'YYYYMMDD')) AS DATE_SK,
    d.DATE AS FULL_DATE,
    DAYNAME(d.DATE) AS DAY_OF_WEEK,
    MONTHNAME(d.DATE) AS MONTH_NAME,
    QUARTER(d.DATE) AS QUARTER,
    YEAR(d.DATE) AS YEAR
FROM (
    SELECT DATEADD(DAY, SEQ4(), $START_DATE) AS DATE
    FROM TABLE(GENERATOR(ROWCOUNT => $GENERATOR_RECORD_COUNT))
) d;

-- Plant and Line Hierarchy
INSERT INTO DIM_PLANT (PLANT_ID, PLANT_NAME, LOCATION) VALUES
(1, 'Davidson Manufacturing', 'Davidson NC'),
(2, 'Charlotte Assembly', 'Charlotte NC');

INSERT INTO DIM_LINE (LINE_ID, PLANT_ID, LINE_NAME, HOURLY_REVENUE) VALUES
(101, 1, 'Production Line A', 15000.00),
(102, 1, 'Production Line B', 12000.00),
(103, 1, 'Production Line C', 14000.00),
(201, 2, 'Assembly Line 1', 18000.00),
(202, 2, 'Assembly Line 2', 16000.00),
(203, 2, 'Assembly Line 3', 17500.00);

-- Asset Classifications
INSERT INTO DIM_ASSET_CLASS (ASSET_CLASS_ID, CLASS_NAME) VALUES
(1, 'Rotating Equipment'),
(2, 'Static Equipment'),
(3, 'Electrical Systems'),
(4, 'Control Systems');

-- Work Order Types (Enhanced)
INSERT INTO DIM_WORK_ORDER_TYPE (WO_TYPE_ID, WO_TYPE_NAME, WO_TYPE_CODE) VALUES
(1, 'Unplanned Emergency', 'UE'),
(2, 'Planned Predictive', 'PP'),
(3, 'Planned Preventive', 'PM'),
(4, 'Inspection', 'INSP');

-- Assets (formerly Equipment) - 3 per line across 6 lines = 18 total
INSERT INTO DIM_ASSET (ASSET_NK, ASSET_NAME, MODEL, OEM_NAME, LINE_ID, ASSET_CLASS_ID, INSTALLATION_DATE, DOWNTIME_IMPACT_PER_HOUR, SCD_START_DATE, IS_CURRENT) VALUES
-- Line 101 Assets (Production Line A)
('EQ-PUMP-001', 'Primary Coolant Pump', 'HydroFlow 5000', 'FlowServe', 101, 1, '2022-01-15', 7500.00, '2022-01-15', TRUE),
('EQ-MOTOR-007', 'Conveyor Drive Motor', 'IronHorse 75HP', 'Siemens', 101, 1, '2021-11-20', 5000.00, '2021-11-20', TRUE),
('EQ-COMP-101', 'Air Compressor Unit', 'CompMax 200', 'Atlas Copco', 101, 1, '2022-03-10', 6000.00, '2022-03-10', TRUE),

-- Line 102 Assets (Production Line B) 
('EQ-PUMP-102', 'Hydraulic Pump System', 'PowerFlow 3000', 'Bosch Rexroth', 102, 1, '2021-08-15', 4500.00, '2021-08-15', TRUE),
('EQ-MOTOR-102', 'Main Drive Motor', 'PowerMax 50HP', 'ABB', 102, 1, '2021-09-20', 4000.00, '2021-09-20', TRUE),
('EQ-FAN-102', 'Cooling Fan Assembly', 'AeroMax 1200', 'Ziehl-Abegg', 102, 3, '2022-01-05', 2500.00, '2022-01-05', TRUE),

-- Line 103 Assets (Production Line C)
('EQ-PUMP-103', 'Process Circulation Pump', 'FlowTech 4000', 'Grundfos', 103, 1, '2021-12-12', 5500.00, '2021-12-12', TRUE),
('EQ-MOTOR-103', 'Conveyor Motor Assembly', 'DriveForce 60HP', 'Siemens', 103, 1, '2022-02-18', 4800.00, '2022-02-18', TRUE),
('EQ-VALVE-103', 'Control Valve System', 'PrecisionFlow 500', 'Emerson', 103, 2, '2022-04-22', 3200.00, '2022-04-22', TRUE),

-- Line 201 Assets (Assembly Line 1)
('EQ-ROBOT-201', 'Assembly Robot Arm', 'FlexArm 6000', 'KUKA', 201, 4, '2021-06-30', 9000.00, '2021-06-30', TRUE),
('EQ-MOTOR-201', 'Conveyor Drive System', 'MegaDrive 100HP', 'Schneider Electric', 201, 1, '2021-07-15', 6500.00, '2021-07-15', TRUE),
('EQ-PRESS-201', 'Pneumatic Press Unit', 'PowerPress 5000', 'SMC', 201, 2, '2021-08-01', 7200.00, '2021-08-01', TRUE),

-- Line 202 Assets (Assembly Line 2)
('EQ-ROBOT-202', 'Welding Robot System', 'WeldMaster Pro', 'Fanuc', 202, 4, '2021-09-10', 8500.00, '2021-09-10', TRUE),
('EQ-MOTOR-202', 'Material Handling Motor', 'FlexDrive 80HP', 'Rockwell', 202, 1, '2021-10-05', 5800.00, '2021-10-05', TRUE),
('EQ-HEAT-202', 'Heat Treatment Furnace', 'ThermoPro 3000', 'Despatch', 202, 2, '2021-11-12', 8000.00, '2021-11-12', TRUE),

-- Line 203 Assets (Assembly Line 3)
('EQ-ROBOT-203', 'Packaging Robot', 'PackBot 2000', 'ABB Robotics', 203, 4, '2022-01-20', 7800.00, '2022-01-20', TRUE),
('EQ-MOTOR-203', 'Sorting System Motor', 'SortDrive 45HP', 'Baldor', 203, 1, '2022-02-14', 4200.00, '2022-02-14', TRUE),
('EQ-SCAN-203', 'Quality Control Scanner', 'VisionScan Pro', 'Cognex', 203, 4, '2022-03-18', 6200.00, '2022-03-18', TRUE);

-- Populate Technicians
INSERT INTO DIM_TECHNICIAN (EMPLOYEE_NK, TECHNICIAN_NAME, CRAFT, SHIFT, HIRE_DATE, IS_ACTIVE) VALUES
('EMP001', 'John Martinez', 'Mechanic', 'Day', '2020-03-15', TRUE),
('EMP002', 'Sarah Chen', 'Electrician', 'Day', '2019-08-22', TRUE),
('EMP003', 'Mike Johnson', 'Instrumentation', 'Day', '2021-01-10', TRUE),
('EMP004', 'Lisa Rodriguez', 'Mechanic', 'Evening', '2020-11-05', TRUE),
('EMP005', 'David Kim', 'Electrician', 'Evening', '2018-09-18', TRUE),
('EMP006', 'Angela Thompson', 'Instrumentation', 'Evening', '2022-04-12', TRUE),
('EMP007', 'Robert Wilson', 'Mechanic', 'Night', '2019-12-03', TRUE),
('EMP008', 'Maria Garcia', 'Electrician', 'Night', '2021-06-28', TRUE),
('EMP009', 'James Lee', 'Instrumentation', 'Night', '2020-07-14', TRUE),
('EMP010', 'Emily Davis', 'Mechanic', 'Day', '2022-01-25', TRUE);

-- Populate Failure Codes
INSERT INTO DIM_FAILURE_CODE (FAILURE_HIERARCHY_1, FAILURE_HIERARCHY_2, FAILURE_HIERARCHY_3, FAILURE_DESCRIPTION) VALUES
('Mechanical', 'Bearing', 'Misalignment', 'Bearing misalignment causing excessive vibration and wear'),
('Mechanical', 'Bearing', 'Lubrication', 'Inadequate or contaminated lubrication leading to bearing failure'),
('Mechanical', 'Seal', 'Wear', 'Seal deterioration causing fluid leakage'),
('Mechanical', 'Coupling', 'Failure', 'Coupling failure due to fatigue or misalignment'),
('Mechanical', 'Impeller', 'Erosion', 'Impeller damage due to cavitation or erosion'),
('Electrical', 'Motor', 'Winding', 'Motor winding failure due to overheating or insulation breakdown'),
('Electrical', 'Motor', 'Connection', 'Loose or corroded electrical connections'),
('Electrical', 'Sensor', 'Calibration', 'Sensor drift or calibration issues affecting readings'),
('Electrical', 'Power Supply', 'Voltage', 'Power supply voltage fluctuations or failures'),
('Operational', 'Overload', 'Capacity', 'Equipment operated beyond design capacity'),
('Operational', 'Temperature', 'Overheating', 'Equipment overheating due to operational conditions'),
('Operational', 'Contamination', 'Foreign Object', 'Foreign object contamination causing operational issues');

-- Populate Budget Data
INSERT INTO FCT_BUDGET (BUDGET_ID, PLANT_ID, YEAR, QUARTER, BUDGET_TYPE, BUDGET_AMOUNT) VALUES
-- Davidson Manufacturing (Plant 1) - 2025 Budget
(1, 1, 2025, 1, 'OpEx Maintenance', 450000.00),
(2, 1, 2025, 2, 'OpEx Maintenance', 475000.00),
(3, 1, 2025, 3, 'OpEx Maintenance', 500000.00),
(4, 1, 2025, 4, 'OpEx Maintenance', 525000.00),
(5, 1, 2025, 1, 'CapEx Project', 125000.00),
(6, 1, 2025, 2, 'CapEx Project', 150000.00),
(7, 1, 2025, 3, 'CapEx Project', 200000.00),
(8, 1, 2025, 4, 'CapEx Project', 175000.00),

-- Charlotte Assembly (Plant 2) - 2025 Budget
(9, 2, 2025, 1, 'OpEx Maintenance', 550000.00),
(10, 2, 2025, 2, 'OpEx Maintenance', 580000.00),
(11, 2, 2025, 3, 'OpEx Maintenance', 600000.00),
(12, 2, 2025, 4, 'OpEx Maintenance', 625000.00),
(13, 2, 2025, 1, 'CapEx Project', 175000.00),
(14, 2, 2025, 2, 'CapEx Project', 200000.00),
(15, 2, 2025, 3, 'CapEx Project', 250000.00),
(16, 2, 2025, 4, 'CapEx Project', 225000.00),

-- Davidson Manufacturing (Plant 1) - 2024 Actuals for comparison
(17, 1, 2024, 1, 'OpEx Maintenance', 425000.00),
(18, 1, 2024, 2, 'OpEx Maintenance', 445000.00),
(19, 1, 2024, 3, 'OpEx Maintenance', 485000.00),
(20, 1, 2024, 4, 'OpEx Maintenance', 510000.00),
(21, 1, 2024, 1, 'CapEx Project', 100000.00),
(22, 1, 2024, 2, 'CapEx Project', 120000.00),
(23, 1, 2024, 3, 'CapEx Project', 180000.00),
(24, 1, 2024, 4, 'CapEx Project', 160000.00),

-- Charlotte Assembly (Plant 2) - 2024 Actuals for comparison
(25, 2, 2024, 1, 'OpEx Maintenance', 520000.00),
(26, 2, 2024, 2, 'OpEx Maintenance', 555000.00),
(27, 2, 2024, 3, 'OpEx Maintenance', 575000.00),
(28, 2, 2024, 4, 'OpEx Maintenance', 595000.00),
(29, 2, 2024, 1, 'CapEx Project', 150000.00),
(30, 2, 2024, 2, 'CapEx Project', 175000.00),
(31, 2, 2024, 3, 'CapEx Project', 220000.00),
(32, 2, 2024, 4, 'CapEx Project', 200000.00);

-- Sensors (3 per asset = 54 total sensors)
INSERT INTO DIM_SENSOR (SENSOR_NK, ASSET_ID, SENSOR_TYPE, UNITS_OF_MEASURE) VALUES
-- Asset 1 (Primary Coolant Pump) Sensors
('EQ-PUMP-001-VIB', 1, 'Vibration', 'mm/s'),
('EQ-PUMP-001-TMP', 1, 'Temperature', 'Celsius'),
('EQ-PUMP-001-PSI', 1, 'Pressure', 'PSI'),

-- Asset 2 (Conveyor Drive Motor) Sensors
('EQ-MOTOR-007-VIB', 2, 'Vibration', 'mm/s'),
('EQ-MOTOR-007-TMP', 2, 'Temperature', 'Celsius'),
('EQ-MOTOR-007-RPM', 2, 'Rotational Speed', 'RPM'),

-- Asset 3 (Air Compressor Unit) Sensors
('EQ-COMP-101-VIB', 3, 'Vibration', 'mm/s'),
('EQ-COMP-101-TMP', 3, 'Temperature', 'Celsius'),
('EQ-COMP-101-PSI', 3, 'Pressure', 'PSI'),

-- Asset 4 (Hydraulic Pump System) Sensors
('EQ-PUMP-102-VIB', 4, 'Vibration', 'mm/s'),
('EQ-PUMP-102-TMP', 4, 'Temperature', 'Celsius'),
('EQ-PUMP-102-PSI', 4, 'Pressure', 'PSI'),

-- Asset 5 (Main Drive Motor) Sensors
('EQ-MOTOR-102-VIB', 5, 'Vibration', 'mm/s'),
('EQ-MOTOR-102-TMP', 5, 'Temperature', 'Celsius'),
('EQ-MOTOR-102-CUR', 5, 'Current', 'Amps'),

-- Asset 6 (Cooling Fan Assembly) Sensors
('EQ-FAN-102-VIB', 6, 'Vibration', 'mm/s'),
('EQ-FAN-102-TMP', 6, 'Temperature', 'Celsius'),
('EQ-FAN-102-RPM', 6, 'Rotational Speed', 'RPM'),

-- Asset 7 (Process Circulation Pump) Sensors
('EQ-PUMP-103-VIB', 7, 'Vibration', 'mm/s'),
('EQ-PUMP-103-TMP', 7, 'Temperature', 'Celsius'),
('EQ-PUMP-103-FLW', 7, 'Flow Rate', 'GPM'),

-- Asset 8 (Conveyor Motor Assembly) Sensors
('EQ-MOTOR-103-VIB', 8, 'Vibration', 'mm/s'),
('EQ-MOTOR-103-TMP', 8, 'Temperature', 'Celsius'),
('EQ-MOTOR-103-TRQ', 8, 'Torque', 'Nm'),

-- Asset 9 (Control Valve System) Sensors
('EQ-VALVE-103-PSI', 9, 'Pressure', 'PSI'),
('EQ-VALVE-103-TMP', 9, 'Temperature', 'Celsius'),
('EQ-VALVE-103-POS', 9, 'Position', 'Percent'),

-- Asset 10 (Assembly Robot Arm) Sensors
('EQ-ROBOT-201-TMP', 10, 'Temperature', 'Celsius'),
('EQ-ROBOT-201-CUR', 10, 'Current', 'Amps'),
('EQ-ROBOT-201-POS', 10, 'Position', 'Degrees'),

-- Asset 11 (Conveyor Drive System) Sensors
('EQ-MOTOR-201-VIB', 11, 'Vibration', 'mm/s'),
('EQ-MOTOR-201-TMP', 11, 'Temperature', 'Celsius'),
('EQ-MOTOR-201-CUR', 11, 'Current', 'Amps'),

-- Asset 12 (Pneumatic Press Unit) Sensors
('EQ-PRESS-201-PSI', 12, 'Pressure', 'PSI'),
('EQ-PRESS-201-TMP', 12, 'Temperature', 'Celsius'),
('EQ-PRESS-201-FOR', 12, 'Force', 'kN'),

-- Asset 13 (Welding Robot System) Sensors
('EQ-ROBOT-202-TMP', 13, 'Temperature', 'Celsius'),
('EQ-ROBOT-202-CUR', 13, 'Current', 'Amps'),
('EQ-ROBOT-202-VOL', 13, 'Voltage', 'Volts'),

-- Asset 14 (Material Handling Motor) Sensors
('EQ-MOTOR-202-VIB', 14, 'Vibration', 'mm/s'),
('EQ-MOTOR-202-TMP', 14, 'Temperature', 'Celsius'),
('EQ-MOTOR-202-SPD', 14, 'Speed', 'RPM'),

-- Asset 15 (Heat Treatment Furnace) Sensors
('EQ-HEAT-202-TMP', 15, 'Temperature', 'Celsius'),
('EQ-HEAT-202-GAS', 15, 'Gas Flow', 'SCFM'),
('EQ-HEAT-202-OXY', 15, 'Oxygen Level', 'Percent'),

-- Asset 16 (Packaging Robot) Sensors
('EQ-ROBOT-203-TMP', 16, 'Temperature', 'Celsius'),
('EQ-ROBOT-203-SPD', 16, 'Speed', 'Units/Min'),
('EQ-ROBOT-203-POS', 16, 'Position', 'mm'),

-- Asset 17 (Sorting System Motor) Sensors
('EQ-MOTOR-203-VIB', 17, 'Vibration', 'mm/s'),
('EQ-MOTOR-203-TMP', 17, 'Temperature', 'Celsius'),
('EQ-MOTOR-203-CUR', 17, 'Current', 'Amps'),

-- Asset 18 (Quality Control Scanner) Sensors
('EQ-SCAN-203-TMP', 18, 'Temperature', 'Celsius'),
('EQ-SCAN-203-LUX', 18, 'Light Intensity', 'Lux'),
('EQ-SCAN-203-FPS', 18, 'Scan Rate', 'FPS');

-- Populate Facts using IDs from Dimensions
-- Asset Telemetry (24 hourly readings per asset on 9/22/2025)
-- Using a CTE to generate hourly data for all 18 assets
INSERT INTO FCT_ASSET_TELEMETRY (ASSET_ID, DATE_SK, RECORDED_AT, TEMPERATURE_C, VIBRATION_MM_S, PRESSURE_PSI, HEALTH_SCORE, FAILURE_PROBABILITY, RUL_DAYS, IS_ANOMALOUS)
WITH hourly_base AS (
    SELECT 
        asset_id,
        20250922 as date_sk,
        DATEADD(HOUR, h.hour_offset, '2025-09-22 00:00:00'::TIMESTAMP_NTZ) as recorded_at,
        h.hour_offset
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS ASSET_ID FROM TABLE(GENERATOR(ROWCOUNT => 18))) a
    CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS HOUR_OFFSET FROM TABLE(GENERATOR(ROWCOUNT => 24))) h
)
SELECT 
    asset_id,
    date_sk,
    recorded_at,
    -- Generate realistic sensor data based on asset type and time (controlled ranges)
    CASE 
        WHEN asset_id IN (1,4,7) THEN ROUND(60 + (hour_offset * 0.5) + UNIFORM(0, 10, RANDOM()), 2)  -- Pumps run warmer
        WHEN asset_id IN (2,5,8,11,14,17) THEN ROUND(55 + (hour_offset * 0.3) + UNIFORM(0, 8, RANDOM()), 2)  -- Motors
        WHEN asset_id IN (10,13,16) THEN ROUND(50 + (hour_offset * 0.2) + UNIFORM(0, 6, RANDOM()), 2)  -- Robots
        WHEN asset_id = 15 THEN ROUND(200 + (hour_offset * 2) + UNIFORM(0, 20, RANDOM()), 2)  -- Furnace much hotter
        ELSE ROUND(45 + (hour_offset * 0.4) + UNIFORM(0, 5, RANDOM()), 2)  -- Other equipment
    END as temperature_c,
    
    -- Vibration data (rotating equipment has higher vibration) - controlled precision
    CASE 
        WHEN asset_id IN (1,2,4,5,7,8,11,14,17) THEN ROUND(0.3 + UNIFORM(0, 0.4, RANDOM()), 2)  -- Rotating equipment
        WHEN asset_id IN (10,13,16) THEN ROUND(0.1 + UNIFORM(0, 0.2, RANDOM()), 2)  -- Robots (less vibration)
        ELSE ROUND(0.05 + UNIFORM(0, 0.1, RANDOM()), 2)  -- Static equipment
    END as vibration_mm_s,
    
    -- Pressure data (only for pumps, compressors, and pneumatic systems) - controlled range
    CASE 
        WHEN asset_id IN (1,3,4,7,9,12) THEN ROUND(140 + UNIFORM(0, 20, RANDOM()), 2)  -- Equipment with pressure sensors
        ELSE NULL
    END as pressure_psi,
    
    -- Health score (degrades slightly over time, with some variation) - ensure 0-100 range
    ROUND(GREATEST(75, LEAST(100, 100 - (hour_offset * 0.2) - UNIFORM(0, 5, RANDOM()))), 2) as health_score,
    
    -- Failure probability (increases slightly with lower health) - ensure 0-1 range
    ROUND(LEAST(0.99, GREATEST(0.01, (100 - GREATEST(75, LEAST(100, 100 - (hour_offset * 0.2) - UNIFORM(0, 5, RANDOM())))) / 500.0)), 2) as failure_probability,
    
    -- Remaining useful life (decreases over time) - ensure positive integer
    GREATEST(1, ROUND(400 - (hour_offset * 0.5) - UNIFORM(0, 20, RANDOM()), 0))::INTEGER as rul_days,
    
    -- Mark as anomalous based on controlled thresholds
    CASE 
        WHEN GREATEST(75, LEAST(100, 100 - (hour_offset * 0.2) - UNIFORM(0, 5, RANDOM()))) < 85 THEN TRUE
        WHEN asset_id IN (1,2,4,5,7,8,11,14,17) AND (0.3 + UNIFORM(0, 0.4, RANDOM())) > 1.5 THEN TRUE
        ELSE FALSE
    END as is_anomalous
FROM hourly_base;

-- Maintenance Log (3 events per asset = 54 total maintenance events)
INSERT INTO FCT_MAINTENANCE_LOG (ASSET_ID, WO_TYPE_ID, ACTION_DATE_SK, COMPLETED_DATE, DOWNTIME_HOURS, PARTS_COST, LABOR_COST, FAILURE_FLAG, TECHNICIAN_ID, FAILURE_CODE_ID, TECHNICIAN_NOTES) VALUES
-- Asset 1 Maintenance History
(1, 3, 20250815, '2025-08-15', 2.5, 150.00, 400.00, FALSE, 1, NULL, 'Routine preventive maintenance - lubrication and inspection completed'),
(1, 2, 20250901, '2025-09-01', 1.5, 75.00, 300.00, FALSE, 2, NULL, 'Predictive maintenance based on vibration analysis'),
(1, 1, 20250923, '2025-09-23', 4.0, 250.00, 600.00, TRUE, 1, 1, 'Emergency repair - bearing misalignment corrected'),

-- Asset 2 Maintenance History  
(2, 3, 20250810, '2025-08-10', 3.0, 200.00, 450.00, FALSE, 2, NULL, 'Scheduled motor maintenance - winding inspection and cleaning'),
(2, 4, 20250825, '2025-08-25', 0.5, 0.00, 150.00, FALSE, 3, NULL, 'Routine inspection - no issues found'),
(2, 2, 20250910, '2025-09-10', 2.0, 125.00, 350.00, FALSE, 1, NULL, 'Predictive maintenance - bearing replacement based on temperature trends'),

-- Asset 3 Maintenance History
(3, 3, 20250805, '2025-08-05', 4.0, 300.00, 500.00, FALSE, 4, NULL, 'Air compressor preventive maintenance - filter and oil change'),
(3, 1, 20250820, '2025-08-20', 6.0, 450.00, 800.00, TRUE, 2, 6, 'Compressor head failure - complete rebuild required'),
(3, 4, 20250905, '2025-09-05', 1.0, 25.00, 200.00, FALSE, 3, NULL, 'Post-repair inspection and performance verification'),

-- Asset 4 Maintenance History
(4, 3, 20250812, '2025-08-12', 2.0, 175.00, 375.00, FALSE, 5, NULL, 'Hydraulic system preventive maintenance'),
(4, 2, 20250828, '2025-08-28', 1.0, 50.00, 250.00, FALSE, 6, NULL, 'Predictive maintenance - seal replacement'),
(4, 4, 20250915, '2025-09-15', 0.5, 0.00, 125.00, FALSE, 4, NULL, 'Routine hydraulic pressure inspection'),

-- Asset 5 Maintenance History
(5, 3, 20250808, '2025-08-08', 2.5, 180.00, 400.00, FALSE, 2, NULL, 'Drive motor preventive maintenance'),
(5, 4, 20250822, '2025-08-22', 0.5, 0.00, 150.00, FALSE, 3, NULL, 'Electrical connection inspection'),
(5, 2, 20250908, '2025-09-08', 1.5, 90.00, 300.00, FALSE, 5, NULL, 'Predictive maintenance based on current analysis'),

-- Asset 6 Maintenance History
(6, 3, 20250814, '2025-08-14', 1.5, 120.00, 275.00, FALSE, 1, NULL, 'Cooling fan preventive maintenance'),
(6, 4, 20250830, '2025-08-30', 0.5, 0.00, 125.00, FALSE, 4, NULL, 'Fan blade inspection and cleaning'),
(6, 2, 20250912, '2025-09-12', 1.0, 60.00, 225.00, FALSE, 6, NULL, 'Bearing lubrication based on vibration monitoring'),

-- Asset 7 Maintenance History
(7, 3, 20250807, '2025-08-07', 3.0, 220.00, 425.00, FALSE, 3, NULL, 'Circulation pump preventive maintenance'),
(7, 1, 20250824, '2025-08-24', 5.0, 400.00, 750.00, TRUE, 1, 5, 'Pump impeller failure - emergency replacement'),
(7, 4, 20250906, '2025-09-06', 1.0, 0.00, 175.00, FALSE, 2, NULL, 'Post-repair flow rate verification'),

-- Asset 8 Maintenance History
(8, 3, 20250811, '2025-08-11', 2.5, 190.00, 380.00, FALSE, 4, NULL, 'Conveyor motor assembly maintenance'),
(8, 4, 20250826, '2025-08-26', 0.5, 0.00, 140.00, FALSE, 5, NULL, 'Torque sensor calibration check'),
(8, 2, 20250911, '2025-09-11', 1.5, 85.00, 290.00, FALSE, 6, NULL, 'Predictive maintenance - coupling replacement'),

-- Asset 9 Maintenance History
(9, 3, 20250809, '2025-08-09', 2.0, 160.00, 320.00, FALSE, 2, NULL, 'Control valve system preventive maintenance'),
(9, 4, 20250823, '2025-08-23', 1.0, 30.00, 180.00, FALSE, 3, NULL, 'Valve position calibration'),
(9, 2, 20250907, '2025-09-07', 1.5, 75.00, 270.00, FALSE, 1, NULL, 'Actuator maintenance based on position drift'),

-- Asset 10 Maintenance History
(10, 3, 20250813, '2025-08-13', 4.0, 350.00, 600.00, FALSE, 7, NULL, 'Robot arm preventive maintenance - full service'),
(10, 4, 20250829, '2025-08-29', 1.5, 0.00, 250.00, FALSE, 8, NULL, 'Joint calibration and accuracy verification'),
(10, 2, 20250914, '2025-09-14', 2.0, 180.00, 400.00, FALSE, 9, NULL, 'Servo motor replacement based on performance monitoring'),

-- Asset 11 Maintenance History
(11, 3, 20250816, '2025-08-16', 3.0, 210.00, 450.00, FALSE, 4, NULL, 'Conveyor drive system maintenance'),
(11, 1, 20250831, '2025-08-31', 6.0, 500.00, 900.00, TRUE, 7, 2, 'Motor bearing failure - emergency replacement'),
(11, 4, 20250916, '2025-09-16', 1.0, 0.00, 200.00, FALSE, 8, NULL, 'Post-repair vibration analysis'),

-- Asset 12 Maintenance History
(12, 3, 20250806, '2025-08-06', 2.5, 200.00, 400.00, FALSE, 5, NULL, 'Pneumatic press preventive maintenance'),
(12, 4, 20250821, '2025-08-21', 1.0, 40.00, 180.00, FALSE, 6, NULL, 'Force sensor calibration'),
(12, 2, 20250904, '2025-09-04', 1.5, 95.00, 320.00, FALSE, 4, NULL, 'Cylinder seal replacement based on pressure drop'),

-- Asset 13 Maintenance History
(13, 3, 20250817, '2025-08-17', 4.5, 380.00, 650.00, FALSE, 9, NULL, 'Welding robot preventive maintenance'),
(13, 4, 20250902, '2025-09-02', 1.5, 25.00, 220.00, FALSE, 10, NULL, 'Welding tip inspection and replacement'),
(13, 2, 20250918, '2025-09-18', 2.5, 150.00, 420.00, FALSE, 8, NULL, 'Power supply maintenance based on voltage monitoring'),

-- Asset 14 Maintenance History
(14, 3, 20250804, '2025-08-04', 2.5, 185.00, 390.00, FALSE, 1, NULL, 'Material handling motor maintenance'),
(14, 4, 20250819, '2025-08-19', 0.5, 0.00, 130.00, FALSE, 2, NULL, 'Speed sensor inspection'),
(14, 2, 20250903, '2025-09-03', 1.5, 80.00, 280.00, FALSE, 3, NULL, 'Belt tensioner adjustment based on speed variance'),

-- Asset 15 Maintenance History
(15, 3, 20250818, '2025-08-18', 6.0, 600.00, 800.00, FALSE, 7, NULL, 'Heat treatment furnace major maintenance'),
(15, 4, 20250901, '2025-09-01', 2.0, 100.00, 300.00, FALSE, 8, NULL, 'Temperature sensor calibration and gas flow check'),
(15, 1, 20250920, '2025-09-20', 8.0, 800.00, 1200.00, TRUE, 9, 11, 'Heating element failure - emergency replacement'),

-- Asset 16 Maintenance History
(16, 3, 20250803, '2025-08-03', 3.5, 280.00, 520.00, FALSE, 4, NULL, 'Packaging robot preventive maintenance'),
(16, 4, 20250817, '2025-08-17', 1.0, 15.00, 160.00, FALSE, 5, NULL, 'End effector inspection and calibration'),
(16, 2, 20250901, '2025-09-01', 2.0, 120.00, 350.00, FALSE, 6, NULL, 'Drive motor maintenance based on speed monitoring'),

-- Asset 17 Maintenance History
(17, 3, 20250802, '2025-08-02', 2.0, 170.00, 360.00, FALSE, 10, NULL, 'Sorting system motor maintenance'),
(17, 4, 20250816, '2025-08-16', 0.5, 0.00, 120.00, FALSE, 1, NULL, 'Current monitoring system check'),
(17, 2, 20250830, '2025-08-30', 1.5, 70.00, 260.00, FALSE, 2, NULL, 'Vibration damper replacement'),

-- Asset 18 Maintenance History
(18, 3, 20250801, '2025-08-01', 2.5, 200.00, 380.00, FALSE, 3, NULL, 'Quality control scanner preventive maintenance'),
(18, 4, 20250815, '2025-08-15', 1.0, 50.00, 180.00, FALSE, 7, NULL, 'Lens cleaning and light calibration'),
(18, 2, 20250829, '2025-08-29', 1.5, 85.00, 290.00, FALSE, 8, NULL, 'Image sensor replacement based on scan quality degradation');

-- Production Log (Daily production data for all assets)
INSERT INTO FCT_PRODUCTION_LOG (ASSET_ID, DATE_SK, PRODUCTION_DATE, PLANNED_RUNTIME_HOURS, ACTUAL_RUNTIME_HOURS, UNITS_PRODUCED, UNITS_SCRAPPED) VALUES
-- Production data for 9/22/2025 - All assets
(1, 20250922, '2025-09-22', 24.0, 23.5, 1250, 15),   -- Primary Coolant Pump - good performance
(2, 20250922, '2025-09-22', 24.0, 24.0, 2400, 5),    -- Conveyor Drive Motor - excellent
(3, 20250922, '2025-09-22', 24.0, 22.8, 1140, 12),   -- Air Compressor - minor issues
(4, 20250922, '2025-09-22', 24.0, 23.2, 1160, 8),    -- Hydraulic Pump - good
(5, 20250922, '2025-09-22', 24.0, 23.8, 1190, 6),    -- Main Drive Motor - good
(6, 20250922, '2025-09-22', 24.0, 23.9, 1195, 4),    -- Cooling Fan - excellent
(7, 20250922, '2025-09-22', 24.0, 23.1, 1155, 10),   -- Process Circulation Pump - good
(8, 20250922, '2025-09-22', 24.0, 23.6, 1180, 7),    -- Conveyor Motor Assembly - good
(9, 20250922, '2025-09-22', 24.0, 23.4, 1170, 9),    -- Control Valve System - good
(10, 20250922, '2025-09-22', 20.0, 19.8, 990, 3),    -- Assembly Robot - excellent quality
(11, 20250922, '2025-09-22', 20.0, 19.5, 975, 8),    -- Conveyor Drive System - good
(12, 20250922, '2025-09-22', 20.0, 19.2, 960, 12),   -- Pneumatic Press - minor quality issues
(13, 20250922, '2025-09-22', 20.0, 19.7, 985, 5),    -- Welding Robot - good
(14, 20250922, '2025-09-22', 20.0, 19.4, 970, 9),    -- Material Handling Motor - good
(15, 20250922, '2025-09-22', 20.0, 18.5, 925, 15),   -- Heat Treatment Furnace - quality issues
(16, 20250922, '2025-09-22', 18.0, 17.8, 890, 6),    -- Packaging Robot - good
(17, 20250922, '2025-09-22', 18.0, 17.6, 880, 8),    -- Sorting System Motor - good
(18, 20250922, '2025-09-22', 18.0, 17.9, 895, 4),    -- Quality Control Scanner - excellent

-- Production data for 9/23/2025 - Day with some maintenance impacts
(1, 20250923, '2025-09-23', 24.0, 20.0, 980, 35),    -- Primary Coolant Pump - emergency repair impact
(2, 20250923, '2025-09-23', 24.0, 23.8, 2380, 8),    -- Conveyor Drive Motor - slight decline
(3, 20250923, '2025-09-23', 24.0, 23.5, 1175, 10),   -- Air Compressor - improved after maint
(4, 20250923, '2025-09-23', 24.0, 23.0, 1150, 12),   -- Hydraulic Pump - slight decline
(5, 20250923, '2025-09-23', 24.0, 23.6, 1180, 9),    -- Main Drive Motor - stable
(6, 20250923, '2025-09-23', 24.0, 23.7, 1185, 6),    -- Cooling Fan - good
(7, 20250923, '2025-09-23', 24.0, 19.0, 950, 25),    -- Process Circulation Pump - post-failure impact
(8, 20250923, '2025-09-23', 24.0, 23.4, 1170, 8),    -- Conveyor Motor Assembly - stable
(9, 20250923, '2025-09-23', 24.0, 23.2, 1160, 11),   -- Control Valve System - slight decline
(10, 20250923, '2025-09-23', 20.0, 19.6, 980, 5),    -- Assembly Robot - good
(11, 20250923, '2025-09-23', 20.0, 16.0, 800, 40),   -- Conveyor Drive System - bearing failure impact
(12, 20250923, '2025-09-23', 20.0, 19.0, 950, 15),   -- Pneumatic Press - continued quality issues
(13, 20250923, '2025-09-23', 20.0, 19.5, 975, 7),    -- Welding Robot - stable
(14, 20250923, '2025-09-23', 20.0, 19.2, 960, 11),   -- Material Handling Motor - slight decline
(15, 20250923, '2025-09-23', 20.0, 12.0, 600, 50),   -- Heat Treatment Furnace - heating element failure
(16, 20250923, '2025-09-23', 18.0, 17.6, 880, 8),    -- Packaging Robot - stable
(17, 20250923, '2025-09-23', 18.0, 17.4, 870, 10),   -- Sorting System Motor - slight decline
(18, 20250923, '2025-09-23', 18.0, 17.7, 885, 6);    -- Quality Control Scanner - good


-- Insert into GOLD Layer
USE SCHEMA HYPERFORGE.GOLD;
INSERT INTO AGG_ASSET_HOURLY_HEALTH (HOUR_TIMESTAMP, ASSET_ID, AVG_TEMPERATURE_C, MAX_VIBRATION_MM_S, STDDEV_PRESSURE_PSI, LATEST_HEALTH_SCORE, AVG_FAILURE_PROBABILITY, MIN_RUL_DAYS) VALUES
('2025-09-22 10:00:00', 1, 65.2, 0.51, 1.2, 98.5, 0.02, 365),
('2025-09-22 11:00:00', 1, 66.1, 0.55, 1.1, 98.2, 0.03, 364),
('2025-09-23 08:00:00', 1, 75.8, 2.15, 5.8, 35.1, 0.85, 7), -- Health score drops significantly with failure prediction
('2025-09-22 10:00:00', 2, 68.5, 0.32, NULL, 99.1, 0.01, 400),
('2025-09-23 08:00:00', 2, 69.2, 0.34, NULL, 98.8, 0.02, 399);

INSERT INTO ML_FEATURE_STORE (OBSERVATION_DATE_SK, ASSET_ID, AVG_TEMP_LAST_24H, VIBRATION_STDDEV_7D, PRESSURE_TREND_7D, CYCLES_SINCE_LAST_PM, DAYS_SINCE_LAST_FAILURE, OEM_FAILURE_RATE_EST, DOWNTIME_IMPACT_RISK, FAILED_IN_NEXT_7_DAYS) VALUES
(20250923, 1, 71.5, 0.87, 2.3, 8500, 180, 0.15, 63750.00, TRUE), -- This asset failed, high risk due to downtime impact
(20250923, 2, 68.8, 0.12, NULL, 12000, 365, 0.08, 20000.00, FALSE); -- Motor asset performing well

/*************************************************************************************************/
-- Step 3: Create Stage and Semantic View for Cortex Analyst
/*************************************************************************************************/

USE SCHEMA HYPERFORGE.GOLD;

-- Create a stage for uploading the semantic view definition
CREATE STAGE IF NOT EXISTS SEMANTIC_VIEW_STAGE
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Stage for semantic view YAML definitions';

-- Note: The YAML file upload and semantic view creation are handled by the deploy.sh script
-- This ensures proper file staging and semantic view creation in the correct sequence

SELECT 'HYPERFORGE database, data, and Cortex Analyst semantic view created successfully.' AS status;

