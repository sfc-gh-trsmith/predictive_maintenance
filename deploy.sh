#!/bin/bash

echo "Deploying HyperForge Predictive Maintenance Database..."

# Step 1: Run the main SQL script to create database, schemas, and tables
snow sql --connection snowflake_demo_hyperforge -f setup/HYPERFORGE_PREDICTIVE_MAINTENANCE.sql
[ $? -ne 0 ] && echo "Failed to create database, schemas, and tables" && exit 1

# # Step 2: Upload the semantic view YAML definition to the stage
snow sql --connection snowflake_demo_hyperforge --database HYPERFORGE --role HYPERFORGE_ROLE --schema GOLD -q "PUT file://setup/HYPERFORGE_SV.yaml @SEMANTIC_VIEW_STAGE AUTO_COMPRESS=FALSE;"
[ $? -ne 0 ] && echo "Failed to upload the semantic view YAML definition to the stage" && exit 1

# # Step 3: Create the semantic view using the uploaded YAML
snow sql --connection snowflake_demo_hyperforge --database HYPERFORGE --role HYPERFORGE_ROLE --schema GOLD -q "
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('HYPERFORGE.GOLD', (SELECT  LISTAGG(*, '\n')  from @SEMANTIC_VIEW_STAGE/HYPERFORGE_SV.yaml) , TRUE);
"
[ $? -ne 0 ] && echo "Failed to create the semantic view" && exit 1

# # Step 4: Grant permissions on the semantic view
snow sql --connection snowflake_demo_hyperforge --database HYPERFORGE --role HYPERFORGE_ROLE --schema GOLD -q "GRANT USAGE ON SEMANTIC VIEW HYPERFORGE_SV TO ROLE HYPERFORGE_ROLE;"
[ $? -ne 0 ] && echo "Failed to grant permissions on the semantic view" && exit 1

echo "Deployment completed successfully!"
