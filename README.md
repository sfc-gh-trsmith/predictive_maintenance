

```mermaid
graph TD
    subgraph "Data Sources"
        DS_IOT["1. IoT Sensors <br/>(Vibration, Temp, Pressure)"] -- "Streaming Data" --> BRONZE_RAW_IOT
        DS_CMMS["2. CMMS/ERP Systems <br/>(Maintenance Logs, Work Orders)"] -- "Batch/API" --> BRONZE_RAW_MAINT
        DS_EQUIP_MASTER["3. Equipment Master Data <br/>(Specs, Location, OEM)"] -- "Batch" --> BRONZE_RAW_EQUIP
        DS_OEM["4. OEM Warranty Data <br/>(External APIs)"] -- "API Fetch" --> BRONZE_RAW_OEM
    end

    subgraph "Snowflake Bronze Layer (Raw Data Lake)"
        BRONZE_RAW_IOT["BRONZE: RAW_IOT_TELEMETRY <br/>(VARIANT, TS)"]
        BRONZE_RAW_MAINT["BRONZE: RAW_MAINTENANCE_LOGS <br/>(VARIANT)"]
        BRONZE_RAW_EQUIP["BRONZE: RAW_EQUIPMENT_MASTER <br/>(VARIANT)"]
        BRONZE_RAW_OEM["BRONZE: RAW_OEM_DATA <br/>(VARIANT)"]
    end

    subgraph "Data Transformation & Integration (ELT)"
        T_IOT["ELT Process: Parse IoT Data"]
        T_MAINT["ELT Process: Clean & Structure Logs"]
        T_EQUIP["ELT Process: Normalize Equipment"]
        T_OEM["ELT Process: Map OEM Data"]
    end

    subgraph "Snowflake Silver Layer (Conformed Star Schema)"
        SILVER_DIM_DATE["SILVER: DIM_DATE"]
        SILVER_DIM_EQUIPMENT["SILVER: DIM_EQUIPMENT"]
        SILVER_DIM_SENSOR["SILVER: DIM_SENSOR"]
        SILVER_DIM_MAINT_TYPE["SILVER: DIM_MAINTENANCE_TYPE"]
        SILVER_FCT_SENSOR["SILVER: FCT_SENSOR_READINGS"]
        SILVER_FCT_MAINT["SILVER: FCT_MAINTENANCE_ACTIONS"]

        BRONZE_RAW_IOT -- "Stream/Task" --> T_IOT
        BRONZE_RAW_MAINT -- "Stream/Task" --> T_MAINT
        BRONZE_RAW_EQUIP -- "Stream/Task" --> T_EQUIP
        BRONZE_RAW_OEM -- "Stream/Task" --> T_OEM
        T_IOT --> SILVER_FCT_SENSOR
        T_MAINT --> SILVER_FCT_MAINT
        T_EQUIP --> SILVER_DIM_EQUIPMENT
        T_OEM -.-> SILVER_DIM_EQUIPMENT

        SILVER_FCT_SENSOR -- "FKs" --> SILVER_DIM_EQUIPMENT
        SILVER_FCT_SENSOR -- "FKs" --> SILVER_DIM_SENSOR
        SILVER_FCT_SENSOR -- "FKs" --> SILVER_DIM_DATE

        SILVER_FCT_MAINT -- "FKs" --> SILVER_DIM_EQUIPMENT
        SILVER_FCT_MAINT -- "FKs" --> SILVER_DIM_MAINT_TYPE
        SILVER_FCT_MAINT -- "FKs" --> SILVER_DIM_DATE
    end

    subgraph "Analytics & AI Core"
        FEATURE_ENG["Feature Engineering <br/>(Aggregations, Rolling Averages, <br/>Time Since Last Event)"]
        ML_MODEL_TRAIN["ML Model Training <br/>(Time-to-Failure, Classification)"]
        PREDICTION_SERVICE["Prediction Service <br/>(Real-time Inference)"]
    end

    subgraph "Snowflake Gold Layer (Curated & Application Specific)"
        GOLD_ML_FEATURES["GOLD: ML_FEATURE_STORE <br/>(Wide table for ML)"]
        GOLD_AGG_HEALTH["GOLD: AGG_EQUIPMENT_HOURLY_HEALTH <br/>(Aggregated for Dashboards)"]
        
        SILVER_FCT_SENSOR -- "Aggregation" --> GOLD_AGG_HEALTH
        SILVER_FCT_MAINT -- "Aggregation" --> GOLD_AGG_HEALTH
        SILVER_DIM_EQUIPMENT -- "Used for Joins" --> GOLD_AGG_HEALTH
        SILVER_FCT_SENSOR -- "Feature Engineering" --> FEATURE_ENG
        SILVER_FCT_MAINT -- "Feature Engineering" --> FEATURE_ENG
        FEATURE_ENG -- "Creates" --> GOLD_ML_FEATURES
        GOLD_ML_FEATURES -- "Used by" --> ML_MODEL_TRAIN
        ML_MODEL_TRAIN -- "Deploys" --> PREDICTION_SERVICE
        PREDICTION_SERVICE -- "Outputs Predictions" --> GOLD_AGG_HEALTH
    end

    subgraph "User Experience (Streamlit App) & Persona Workflows"
        UX_DASH_EXEC["UX: Executive Dashboard"]
        UX_DASH_OPS["UX: Operations Dashboard"]
        UX_DASH_RELIAB["UX: Reliability Workbench"]
        UX_CHATBOT["UX: Snowflake Intelligence Chatbot"]

        VALERIE("Valerie Vance - Strategist") -- "Reviews Aggregate Health, Costs" --> UX_DASH_EXEC
        MARCUS("Marcus Thorne - Operator") -- "Monitors Alerts, Dispatches" --> UX_DASH_OPS
        LENA("Dr. Lena Petrova - Analyst") -- "Investigates Failures, Validates Models" --> UX_DASH_RELIAB

        UX_DASH_EXEC -- "Queries" --> GOLD_AGG_HEALTH
        UX_DASH_OPS -- "Queries" --> GOLD_AGG_HEALTH
        UX_DASH_OPS -- "Drill-down Query" --> SILVER_FCT_SENSOR
        UX_DASH_OPS -- "Action: Create WO" --> DS_CMMS
        UX_DASH_RELIAB -- "Deep Query" --> GOLD_ML_FEATURES
        UX_DASH_RELIAB -- "Deep Query" --> SILVER_FCT_SENSOR
        UX_DASH_RELIAB -- "Model Feedback" --> ML_MODEL_TRAIN
        UX_CHATBOT -- "Natural Language Query" --> SNOWFLAKE_QUERY_ENGINE["Snowflake Query Engine"]
        SNOWFLAKE_QUERY_ENGINE -- "Accesses" --> GOLD_AGG_HEALTH
    end

    ```

## Authentication for Snowflake REST (Cortex Analyst and Intelligence Agent)

This app uses Snowflake Programmatic Access Tokens (PAT) exclusively for HTTP calls to Snowflake REST endpoints. Tokens are resolved with the following precedence (first non-empty wins):

1. `SNOWFLAKE_TOKEN`
2. `SNOWFLAKE_CONNECTIONS_<CONNECTION_NAME>_TOKEN` (if `features.connection_name` is provided in `secrets.toml`)
3. `SNOWFLAKE_TOKEN_FILE_PATH` (contents of the file)
4. `st.secrets["snowflake"]["personal_access_token"]`
5. `st.secrets["snowflake"]["token_file_path"]` (contents of the file)

Headers sent to Snowflake:

```python
{
  "Authorization": "Bearer <token>",
  "Content-Type": "application/json",
  "Accept": "application/json" | "text/event-stream",
  "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN"
}
```

The base URL is derived from account: `https://<account-with-dashes>.snowflakecomputing.com`.

Reference: Snowflake credential environment variables and PAT guidance in the docs: `https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials`.
