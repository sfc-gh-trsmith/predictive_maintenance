-- ============================================================================
-- HyperForge Predictive Maintenance Intelligence Agent Setup
-- ============================================================================
-- This script creates the Snowflake Intelligence Agent for predictive maintenance
-- operations, building on the existing HyperForge database structure.

USE ROLE HYPERFORGE_ROLE;
USE DATABASE HYPERFORGE;
USE WAREHOUSE HYPERFORGE_WH;

-- ============================================================================
-- Intelligence Agent Creation
-- ============================================================================

-- Create the Intelligence Agent with tools for maintenance operations
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT
AS 
$$
{
  "name": "HyperForge Predictive Maintenance Assistant",
  "description": "Intelligent assistant for predictive maintenance operations, asset health monitoring, and maintenance workflow automation",
  "instructions": "You are an expert predictive maintenance assistant for HyperForge manufacturing operations. You help users analyze asset health, predict failures, create maintenance work orders, and optimize maintenance schedules. You have access to comprehensive manufacturing data including asset telemetry, maintenance history, production logs, and failure predictions. When users ask questions, provide clear, actionable insights and offer to take maintenance actions when appropriate. Always prioritize safety and equipment reliability.",
  
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "query_asset_health",
        "description": "Query current health status and failure predictions for manufacturing assets",
        "parameters": {
          "type": "object",
          "properties": {
            "asset_id": {
              "type": "integer",
              "description": "Specific asset ID to query (optional - if not provided, returns top at-risk assets)"
            },
            "limit": {
              "type": "integer", 
              "description": "Maximum number of assets to return (default: 10)",
              "default": 10
            }
          }
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "create_maintenance_work_order",
        "description": "Create a maintenance work order for specified assets",
        "parameters": {
          "type": "object",
          "properties": {
            "asset_id": {
              "type": "integer",
              "description": "Asset ID requiring maintenance"
            },
            "asset_name": {
              "type": "string",
              "description": "Name of the asset"
            },
            "priority": {
              "type": "string",
              "enum": ["Low", "Medium", "High", "Critical"],
              "description": "Priority level for the work order",
              "default": "Medium"
            },
            "work_type": {
              "type": "string",
              "enum": ["Preventive Maintenance", "Corrective Maintenance", "Predictive Maintenance", "Emergency Repair"],
              "description": "Type of maintenance work",
              "default": "Preventive Maintenance"
            },
            "description": {
              "type": "string",
              "description": "Detailed description of the maintenance work required"
            }
          },
          "required": ["asset_id"]
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "get_asset_failure_prediction",
        "description": "Get failure predictions and risk assessment for assets",
        "parameters": {
          "type": "object",
          "properties": {
            "days_ahead": {
              "type": "integer",
              "description": "Number of days ahead to predict failures",
              "default": 7
            },
            "threshold": {
              "type": "number",
              "description": "Minimum failure probability threshold (0.0 to 1.0)",
              "default": 0.5
            }
          }
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "schedule_preventive_maintenance",
        "description": "Schedule preventive maintenance for one or more assets",
        "parameters": {
          "type": "object",
          "properties": {
            "asset_ids": {
              "type": "array",
              "items": {"type": "integer"},
              "description": "List of asset IDs to schedule maintenance for"
            },
            "schedule_date": {
              "type": "string",
              "description": "Preferred date for maintenance (YYYY-MM-DD format, optional)"
            },
            "maintenance_type": {
              "type": "string",
              "enum": ["Preventive", "Predictive", "Condition-Based"],
              "description": "Type of preventive maintenance",
              "default": "Preventive"
            }
          },
          "required": ["asset_ids"]
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "get_maintenance_history",
        "description": "Retrieve maintenance history for assets",
        "parameters": {
          "type": "object",
          "properties": {
            "asset_id": {
              "type": "integer",
              "description": "Specific asset ID (optional - if not provided, shows all recent maintenance)"
            },
            "days_back": {
              "type": "integer",
              "description": "Number of days back to retrieve history",
              "default": 30
            }
          }
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "calculate_downtime_risk",
        "description": "Calculate financial impact and risk of potential equipment downtime",
        "parameters": {
          "type": "object",
          "properties": {
            "time_horizon_days": {
              "type": "integer",
              "description": "Time horizon for risk calculation in days",
              "default": 30
            }
          }
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "get_oee_metrics",
        "description": "Get Overall Equipment Effectiveness (OEE) metrics for assets",
        "parameters": {
          "type": "object",
          "properties": {
            "days_back": {
              "type": "integer",
              "description": "Number of days back to calculate OEE metrics",
              "default": 7
            }
          }
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "trigger_maintenance_alert",
        "description": "Trigger immediate maintenance alerts for critical conditions",
        "parameters": {
          "type": "object",
          "properties": {
            "alert_type": {
              "type": "string",
              "enum": ["Critical Asset Condition", "Imminent Failure", "Safety Concern", "Production Impact"],
              "description": "Type of maintenance alert",
              "default": "Critical Asset Condition"
            },
            "asset_ids": {
              "type": "array",
              "items": {"type": "integer"},
              "description": "List of asset IDs affected by the alert"
            },
            "message": {
              "type": "string",
              "description": "Alert message describing the condition",
              "default": "Immediate attention required"
            }
          },
          "required": ["asset_ids", "message"]
        }
      }
    }
  ],
  
  "semantic_model": "@HYPERFORGE.GOLD.SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml",
  
  "settings": {
    "max_iterations": 10,
    "enable_tools": true,
    "response_format": "markdown"
  }
}
$$;

-- ============================================================================
-- Grant permissions for the Intelligence Agent
-- ============================================================================

-- Grant usage on the Intelligence Agent to the HyperForge role
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT TO ROLE HYPERFORGE_ROLE;

-- Grant execute permissions (if needed for agent operations)
GRANT EXECUTE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT TO ROLE HYPERFORGE_ROLE;

-- ============================================================================
-- Verification and Testing
-- ============================================================================

-- Verify the agent was created successfully
DESCRIBE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT;

-- Show agent details
SHOW AGENTS LIKE 'HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT' IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS;

-- ============================================================================
-- Configuration Notes
-- ============================================================================

/*
This Intelligence Agent provides the following capabilities:

1. **Asset Health Monitoring**
   - Query current health scores and failure predictions
   - Identify assets at risk of failure
   - Monitor critical performance indicators

2. **Maintenance Operations**
   - Create maintenance work orders
   - Schedule preventive maintenance
   - Track maintenance history and costs

3. **Predictive Analytics**
   - Failure prediction with configurable time horizons
   - Risk assessment and financial impact calculation
   - OEE metrics and performance analysis

4. **Alert Management**
   - Trigger critical maintenance alerts
   - Notify maintenance teams of urgent conditions
   - Prioritize maintenance activities

The agent uses the existing HyperForge semantic model and can access all 
manufacturing data through the established data warehouse structure.

To use this agent in the application:
1. Update secrets.toml with agent configuration
2. Set use_intelligence = true in features section
3. The unified_assistant.py will route requests to this agent

Example configuration in secrets.toml:
[features]
use_intelligence = true
intelligence_agent = "SNOWFLAKE_INTELLIGENCE.AGENTS.HYPERFORGE_PREDICTIVE_MAINTENANCE_AGENT"
fallback_to_cortex = true
*/
