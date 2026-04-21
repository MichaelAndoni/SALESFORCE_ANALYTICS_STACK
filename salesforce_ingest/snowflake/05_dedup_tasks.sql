-- snowflake/05_dedup_tasks.sql
--
-- Snowpipe appends rows — it does NOT deduplicate.
-- If the extractor reruns with overlapping watermarks (which it does, by design,
-- due to the 30-minute lookback buffer), the same record ID can appear multiple
-- times in RAW.SALESFORCE tables.
--
-- Two strategies to handle this:
--
--   Option A (used here): Deduplicate view in STG_ layer.
--   The dbt staging models read through a VIEW that selects the most recent
--   row per ID based on _LOAD_TIMESTAMP.  No physical dedup needed in RAW.
--   Simple, no maintenance, zero cost.  (See 06_dedup_views.sql)
--
--   Option B: Snowflake TASK that periodically merges duplicates in-place.
--   More complex but useful if downstream consumers query RAW directly.
--   This file implements Option B as an optional complement.
--
-- These tasks run 5 minutes after the top of each hour, giving Snowpipe time
-- to finish loading files written by the Lambda (which runs on the hour).

USE ROLE SYSADMIN;
USE DATABASE RAW;
USE SCHEMA   SALESFORCE;

-- Warehouse for task execution (use a small, auto-suspending warehouse)
CREATE WAREHOUSE IF NOT EXISTS DEDUP_WH
    WAREHOUSE_SIZE    = 'XSMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used only by dedup tasks; auto-suspends after 60s';

-- ── OPPORTUNITY dedup task (most important — change-tracking for pipeline) ──
CREATE OR REPLACE TASK TASK_DEDUP_OPPORTUNITY
    WAREHOUSE   = DEDUP_WH
    SCHEDULE    = 'USING CRON 5 * * * * UTC'   -- 5 min past every hour
    COMMENT     = 'Remove duplicate Opportunity rows left by Snowpipe append'
AS
CREATE OR REPLACE TABLE OPPORTUNITY AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY
                   SYSTEMMODSTAMP DESC NULLS LAST,
                   _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM OPPORTUNITY
)
WHERE rn = 1;

-- Repeat the same pattern for other high-volume / high-churn objects.
-- Lower-churn objects (User, Campaign) can tolerate the view-based approach.

CREATE OR REPLACE TASK TASK_DEDUP_ACCOUNT
    WAREHOUSE = DEDUP_WH
    SCHEDULE  = 'USING CRON 5 * * * * UTC'
AS
CREATE OR REPLACE TABLE ACCOUNT AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM ACCOUNT
) WHERE rn = 1;

CREATE OR REPLACE TASK TASK_DEDUP_LEAD
    WAREHOUSE = DEDUP_WH
    SCHEDULE  = 'USING CRON 5 * * * * UTC'
AS
CREATE OR REPLACE TABLE LEAD AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ID
               ORDER BY SYSTEMMODSTAMP DESC NULLS LAST, _LOAD_TIMESTAMP DESC
           ) AS rn
    FROM LEAD
) WHERE rn = 1;

-- Resume tasks (they start suspended by default)
ALTER TASK TASK_DEDUP_OPPORTUNITY RESUME;
ALTER TASK TASK_DEDUP_ACCOUNT     RESUME;
ALTER TASK TASK_DEDUP_LEAD        RESUME;

-- Check task run history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
--     RESULT_LIMIT => 100
-- ));
