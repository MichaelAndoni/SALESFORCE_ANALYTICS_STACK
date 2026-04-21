-- snowflake/06_dedup_views.sql
--
-- Lightweight deduplication views.
--
-- These views sit between the raw Snowpipe-appended tables and the dbt
-- staging layer.  The dbt _sources.yml references these views, NOT the
-- underlying tables, so dbt never sees duplicate rows regardless of how
-- many times the extractor reruns a window.
--
-- Each view selects the LATEST row per Salesforce ID based on:
--   1. SYSTEMMODSTAMP (Salesforce's authoritative "last changed" timestamp)
--   2. _LOAD_TIMESTAMP (pipeline ingest time; tiebreaker)
--
-- Performance note:
--   Snowflake's clustering on DATE_TRUNC('DAY', SYSTEMMODSTAMP) means the
--   ROW_NUMBER() window function only scans recent micro-partitions on
--   incremental dbt runs — this is fast even on multi-billion-row tables.

USE ROLE SYSADMIN;
USE DATABASE RAW;
USE SCHEMA   SALESFORCE;


CREATE OR REPLACE VIEW V_ACCOUNT AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM ACCOUNT
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_CONTACT AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM CONTACT
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_LEAD AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM LEAD
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_OPPORTUNITY AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM OPPORTUNITY
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_OPPORTUNITY_LINE_ITEM AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM OPPORTUNITY_LINE_ITEM
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_CAMPAIGN AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM CAMPAIGN
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_CAMPAIGN_MEMBER AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM CAMPAIGN_MEMBER
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_TASK AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM TASK
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_EVENT AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM EVENT
) WHERE rn = 1;

CREATE OR REPLACE VIEW V_USER AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM "USER"
) WHERE rn = 1;


-- Grant view access to TRANSFORMER role (used by dbt)
GRANT SELECT ON ALL VIEWS    IN SCHEMA RAW.SALESFORCE TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA RAW.SALESFORCE TO ROLE TRANSFORMER;
