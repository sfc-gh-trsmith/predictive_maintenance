-- Create the correct stage for Cortex Analyst semantic model
USE DATABASE HYPERFORGE;
USE SCHEMA GOLD;
USE ROLE HYPERFORGE_ROLE;

-- Create the stage that Cortex Analyst expects
CREATE STAGE IF NOT EXISTS HYPERFORGE_SV
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Stage for Cortex Analyst semantic model HYPERFORGE_SV.yaml';

-- Grant necessary permissions
GRANT USAGE ON STAGE HYPERFORGE_SV TO ROLE HYPERFORGE_ROLE;
GRANT READ ON STAGE HYPERFORGE_SV TO ROLE HYPERFORGE_ROLE;

-- Show the stage info
DESCRIBE STAGE HYPERFORGE_SV;

-- Upload the semantic model file to the correct stage
-- Note: This PUT command should be run from where the YAML file is located
PUT file://HYPERFORGE_SV.yaml @HYPERFORGE_SV AUTO_COMPRESS=FALSE;

-- List files in the stage to verify upload
LIST @HYPERFORGE_SV;

-- Test that the file is accessible
SELECT $1 FROM @HYPERFORGE_SV/HYPERFORGE_SV.yaml LIMIT 5;

SELECT 'Stage HYPERFORGE_SV created and semantic model uploaded successfully!' AS STATUS;




