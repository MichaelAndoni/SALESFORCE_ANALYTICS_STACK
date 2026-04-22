-- snowflake/01_storage_integration.sql
--
-- Run this ONCE as ACCOUNTADMIN after the CDK stack is deployed.
--
-- The storage integration creates a Snowflake-managed IAM identity that
-- Snowflake uses to assume the SnowflakeRole deployed by CDK.
--
-- Step-by-step:
--   1. Deploy CDK → note SnowflakeRoleArn and LandingBucketName outputs
--   2. Run this file as ACCOUNTADMIN
--   3. Run: DESCRIBE INTEGRATION SF_S3_INTEGRATION;
--      → copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
--   4. Update the CDK SnowflakeRole trust policy with those values (see note)
--   5. Re-run CDK deploy (or update trust policy via AWS Console / CLI)

USE ROLE ACCOUNTADMIN;

-- ── Storage integration ────────────────────────────────────────────────────
-- Replace the bucket name with the CDK output value.
CREATE STORAGE INTEGRATION IF NOT EXISTS SF_S3_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::732341677246:role/sf-ingest-dev-snowflake-access'  -- CDK output: SnowflakeRoleArn
    STORAGE_ALLOWED_LOCATIONS = ('s3://sf-ingest-dev-landing-732341677246/salesforce/')
;

-- Inspect to get the Snowflake-managed IAM user details
DESCRIBE INTEGRATION SF_S3_INTEGRATION;
-- Copy:
--   STORAGE_AWS_IAM_USER_ARN   → update CDK SnowflakeRole trusted entity
--   STORAGE_AWS_EXTERNAL_ID    → update CDK SnowflakeRole trust condition

-- After updating the IAM trust policy, verify connectivity:
-- SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('SF_S3_INTEGRATION');


-- ── SQS notification integration ───────────────────────────────────────────
-- Allows Snowpipe to receive S3 event notifications from the SQS queue.
-- Replace the SQS ARN with the CDK output value.
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS SF_SQS_INTEGRATION
    ENABLED            = TRUE
    TYPE               = QUEUE
    NOTIFICATION_PROVIDER = AWS_SQS
    DIRECTION          = INBOUND
    AWS_SQS_ARN        = 'arn:aws:sqs:us-east-1:732341677246:sf-ingest-dev-snowpipe-notifications'  -- CDK output: SnowpipeQueueArn
;

DESCRIBE INTEGRATION SF_SQS_INTEGRATION;
-- Copy:
--   SF_AWS_IAM_USER_ARN and SF_AWS_EXTERNAL_ID
--   Update the SQS queue resource policy to trust these (CDK already set a
--   broad S3 principal; tighten to the Snowflake IAM user if desired).

GRANT USAGE ON INTEGRATION SF_S3_INTEGRATION      TO ROLE LOADER;
GRANT USAGE ON INTEGRATION SF_SQS_INTEGRATION     TO ROLE LOADER;
