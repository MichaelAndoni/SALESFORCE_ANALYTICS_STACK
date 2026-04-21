-- snowflake/08_monitoring_queries.sql
--
-- Operational monitoring and cost visibility queries.
-- Run ad-hoc as ACCOUNTADMIN or SYSADMIN to investigate pipeline health,
-- credit consumption, and data freshness.
--
-- Sections:
--   1. Snowpipe load history
--   2. Pipeline credit consumption
--   3. dbt query performance
--   4. Data freshness checks
--   5. Table growth tracking
--   6. Failed file investigation

USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE;      -- Snowflake's built-in metadata database
USE SCHEMA ACCOUNT_USAGE;


-- ============================================================================
-- 1. SNOWPIPE LOAD HISTORY (last 7 days)
-- ============================================================================

-- Files loaded per pipe per day
SELECT
    pipe_name,
    date_trunc('day', last_load_time)::date  AS load_date,
    count(*)                                 AS files_loaded,
    sum(row_count)                           AS rows_loaded,
    sum(error_count)                         AS errors,
    avg(datediff('second',
        first_error_timestamp,
        last_load_time))                     AS avg_file_lag_seconds
FROM snowflake.account_usage.copy_history
WHERE table_schema   = 'SALESFORCE'
  AND last_load_time >= dateadd('day', -7, current_timestamp())
GROUP BY 1, 2
ORDER BY 1, 2 DESC;


-- Files that failed to load (errors to investigate)
SELECT
    pipe_name,
    last_load_time,
    file_name,
    row_count,
    error_count,
    first_error_message,
    first_error_line_number,
    first_error_column_name
FROM snowflake.account_usage.copy_history
WHERE table_schema   = 'SALESFORCE'
  AND error_count    > 0
  AND last_load_time >= dateadd('day', -7, current_timestamp())
ORDER BY last_load_time DESC
LIMIT 100;


-- Pipe credit consumption
SELECT
    pipe_name,
    sum(credits_used)          AS total_credits,
    sum(credits_used) * 3.00   AS estimated_cost_usd   -- $3/credit standard pricing
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time >= dateadd('day', -30, current_timestamp())
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================================================
-- 2. WAREHOUSE CREDIT CONSUMPTION (last 30 days)
-- ============================================================================

SELECT
    warehouse_name,
    date_trunc('day', start_time)::date AS usage_date,
    sum(credits_used)                   AS credits_used,
    sum(credits_used) * 3.00            AS estimated_cost_usd
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= dateadd('day', -30, current_timestamp())
GROUP BY 1, 2
ORDER BY 1, 2 DESC;


-- Credit spend by warehouse and query type
SELECT
    qh.warehouse_name,
    qh.query_type,
    count(*)                       AS query_count,
    sum(qh.credits_used_cloud_services)
                                   AS cloud_service_credits,
    avg(qh.execution_time) / 1000  AS avg_execution_sec,
    max(qh.execution_time) / 1000  AS max_execution_sec
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time      >= dateadd('day', -7, current_timestamp())
  AND qh.query_tag      LIKE 'dbt%'
  AND qh.execution_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 50;


-- ============================================================================
-- 3. DBT QUERY PERFORMANCE (identify slow models)
-- ============================================================================

-- Top 20 slowest dbt model runs in the last 7 days
SELECT
    query_tag,
    query_text,
    warehouse_name,
    round(execution_time / 1000, 1)     AS execution_sec,
    round(bytes_scanned / 1073741824, 2) AS gb_scanned,
    round(rows_produced, 0)              AS rows_produced,
    start_time
FROM snowflake.account_usage.query_history
WHERE query_tag    LIKE 'dbt%'
  AND start_time  >= dateadd('day', -7, current_timestamp())
  AND query_type   = 'SELECT'
ORDER BY execution_time DESC
LIMIT 20;


-- dbt model execution time trend (P50, P95 by model)
-- Extracts model name from query_tag (format: dbt_<target>)
-- Full model names are in the query_text as table references
SELECT
    date_trunc('day', start_time)::date  AS run_date,
    count(*)                             AS queries_run,
    median(execution_time / 1000)        AS p50_sec,
    percentile_cont(0.95)
        within group (order by execution_time / 1000)
                                         AS p95_sec,
    max(execution_time / 1000)           AS max_sec
FROM snowflake.account_usage.query_history
WHERE query_tag LIKE 'dbt%'
  AND start_time >= dateadd('day', -30, current_timestamp())
GROUP BY 1
ORDER BY 1 DESC;


-- ============================================================================
-- 4. DATA FRESHNESS CHECKS
-- ============================================================================

-- Latest _load_timestamp per raw table (how current is our raw data?)
USE ROLE TRANSFORMER;
USE DATABASE RAW;
USE SCHEMA SALESFORCE;

SELECT
    'V_ACCOUNT'             AS view_name,
    max(_load_timestamp)    AS latest_load,
    datediff('minute', max(_load_timestamp), current_timestamp()) AS minutes_ago
FROM V_ACCOUNT
UNION ALL
SELECT 'V_CONTACT',         max(_load_timestamp),
       datediff('minute', max(_load_timestamp), current_timestamp()) FROM V_CONTACT
UNION ALL
SELECT 'V_LEAD',            max(_load_timestamp),
       datediff('minute', max(_load_timestamp), current_timestamp()) FROM V_LEAD
UNION ALL
SELECT 'V_OPPORTUNITY',     max(_load_timestamp),
       datediff('minute', max(_load_timestamp), current_timestamp()) FROM V_OPPORTUNITY
UNION ALL
SELECT 'V_CAMPAIGN',        max(_load_timestamp),
       datediff('minute', max(_load_timestamp), current_timestamp()) FROM V_CAMPAIGN
UNION ALL
SELECT 'V_USER',            max(_load_timestamp),
       datediff('minute', max(_load_timestamp), current_timestamp()) FROM V_USER
ORDER BY minutes_ago DESC;


-- Mart table freshness (when did dbt last build each fact?)
USE DATABASE ANALYTICS;

SELECT
    table_schema,
    table_name,
    last_altered,
    datediff('minute', last_altered, current_timestamp()) AS minutes_since_refresh,
    row_count,
    round(bytes / 1073741824, 3) AS size_gb
FROM information_schema.tables
WHERE table_schema IN ('MARTS_SALES', 'MARTS_MARKETING', 'MARTS_CORE', 'STG_SALESFORCE')
  AND table_type = 'BASE TABLE'
ORDER BY minutes_since_refresh DESC;


-- ============================================================================
-- 5. TABLE GROWTH TRACKING
-- ============================================================================

-- Raw table row counts and sizes
USE DATABASE RAW;
USE SCHEMA SALESFORCE;

SELECT
    t.table_name,
    t.row_count,
    round(t.bytes / 1073741824, 3) AS size_gb,
    t.last_altered::date           AS last_loaded_date,
    t.created::date                AS created_date
FROM information_schema.tables t
WHERE t.table_type = 'BASE TABLE'
ORDER BY t.row_count DESC;


-- Duplicate detection in raw tables (should be 0 after dedup tasks run)
-- Run this to verify the dedup tasks/views are working:
SELECT
    'OPPORTUNITY' AS object_name,
    count(*)      AS total_rows,
    count(distinct id) AS distinct_ids,
    count(*) - count(distinct id) AS duplicate_rows
FROM RAW.SALESFORCE.OPPORTUNITY
UNION ALL
SELECT 'ACCOUNT', count(*), count(distinct id), count(*) - count(distinct id)
FROM RAW.SALESFORCE.ACCOUNT
UNION ALL
SELECT 'LEAD', count(*), count(distinct id), count(*) - count(distinct id)
FROM RAW.SALESFORCE.LEAD
ORDER BY duplicate_rows DESC;


-- ============================================================================
-- 6. SNOWFLAKE TASK HISTORY (dedup tasks)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    datediff('second', scheduled_time, completed_time) AS duration_sec,
    error_code,
    error_message
FROM table(information_schema.task_history(
    scheduled_time_range_start => dateadd('hour', -48, current_timestamp()),
    result_limit => 200
))
WHERE name LIKE 'TASK_DEDUP_%'
ORDER BY scheduled_time DESC;


-- ============================================================================
-- 7. ALERT SETUP (run once to create Snowflake-native alerts)
-- ============================================================================
-- These alerts fire when data freshness exceeds thresholds,
-- sending notifications to the configured notification integration.

-- First create a notification integration pointing at your SNS topic:
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ALERT_EMAIL_INTEGRATION
--     TYPE = EMAIL
--     ENABLED = TRUE
--     ALLOWED_RECIPIENTS = ('data-eng@yourcompany.com');

-- Alert: raw data not refreshed in 3+ hours
-- CREATE OR REPLACE ALERT ALERT_RAW_DATA_STALE
--     WAREHOUSE = LOADING_WH
--     SCHEDULE  = '15 MINUTE'
--     IF (EXISTS (
--         SELECT 1
--         FROM RAW.SALESFORCE.V_OPPORTUNITY
--         HAVING datediff('hour', max(_load_timestamp), current_timestamp()) >= 3
--     ))
--     THEN CALL SYSTEM$SEND_EMAIL(
--         'ALERT_EMAIL_INTEGRATION',
--         'data-eng@yourcompany.com',
--         '⚠️ Snowflake Alert: Raw Salesforce data is stale',
--         'RAW.SALESFORCE data has not been refreshed in 3+ hours. Check the Lambda extractor and Snowpipe status.'
--     );
-- ALTER ALERT ALERT_RAW_DATA_STALE RESUME;
