-- snowflake/02_schema_stage_format.sql
--
-- Creates the RAW database, SALESFORCE schema, an external S3 stage,
-- and a file format for NDJSON files written by the extractor Lambda.
--
-- Run as SYSADMIN (after storage integration is set up).

USE ROLE SYSADMIN;

-- ── Database and schema ────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS RAW
    DATA_RETENTION_TIME_IN_DAYS = 1    -- minimal; raw is replayed from S3
    COMMENT = 'Raw landing zone — source-conformed data, no transforms applied';

CREATE SCHEMA IF NOT EXISTS RAW.SALESFORCE
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Raw Salesforce objects landed by Lambda extractor via Snowpipe';

USE DATABASE RAW;
USE SCHEMA   SALESFORCE;


-- ── File format ────────────────────────────────────────────────────────────
-- NDJSON: one JSON object per line.
-- STRIP_OUTER_ARRAY = FALSE because each line is already a standalone object.
-- NULL_IF handles Salesforce's "null" string representation.
CREATE OR REPLACE FILE FORMAT FF_SALESFORCE_NDJSON
    TYPE                = 'JSON'
    COMPRESSION         = 'AUTO'         -- handles gzip if we ever compress
    STRIP_OUTER_ARRAY   = FALSE          -- NDJSON: one object per line
    STRIP_NULL_VALUES   = FALSE          -- preserve explicit nulls
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT         = 'AUTO'
    TIMESTAMP_FORMAT    = 'AUTO'
    NULL_IF             = ('', 'null', 'NULL', 'None')
    COMMENT = 'NDJSON format for Salesforce extractor Lambda output';


-- ── External stage ─────────────────────────────────────────────────────────
-- Points at the S3 prefix where Lambda writes files.
-- Snowpipe will list this stage to discover files (supplemented by SQS events).
CREATE OR REPLACE STAGE STG_SALESFORCE_LANDING
    STORAGE_INTEGRATION = SF_S3_INTEGRATION
    URL                 = 's3://sf-ingest-prod-landing-ACCOUNT_ID/salesforce/raw/'
    FILE_FORMAT         = FF_SALESFORCE_NDJSON
    COMMENT             = 'External stage pointing at Lambda S3 landing prefix';

-- Validate the stage is accessible
LIST @STG_SALESFORCE_LANDING;


-- ── Grants ─────────────────────────────────────────────────────────────────
GRANT USAGE  ON DATABASE RAW                     TO ROLE LOADER;
GRANT USAGE  ON DATABASE RAW                     TO ROLE TRANSFORMER;
GRANT USAGE  ON SCHEMA   RAW.SALESFORCE          TO ROLE LOADER;
GRANT USAGE  ON SCHEMA   RAW.SALESFORCE          TO ROLE TRANSFORMER;
GRANT USAGE  ON STAGE    RAW.SALESFORCE.STG_SALESFORCE_LANDING TO ROLE LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW.SALESFORCE TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW.SALESFORCE TO ROLE TRANSFORMER;
