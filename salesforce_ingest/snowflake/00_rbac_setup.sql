-- snowflake/00_rbac_setup.sql
--
-- Run ONCE as ACCOUNTADMIN to set up roles, warehouses, and the permission
-- hierarchy for the full Salesforce → Snowflake → dbt stack.
--
-- Role hierarchy:
--
--   SYSADMIN
--     └─ LOADER         (Lambda / Snowpipe: INSERT into RAW.SALESFORCE)
--     └─ TRANSFORMER    (dbt: SELECT on RAW, full DML on ANALYTICS)
--     └─ REPORTER       (BI tools: SELECT on ANALYTICS mart schemas only)
--
-- Warehouses:
--   LOADING_WH     XS, auto-suspend 60s   — Snowpipe + dedup tasks
--   TRANSFORMING   S,  auto-suspend 120s  — dbt production runs
--   REPORTING      XS, auto-suspend 60s   — BI tool ad-hoc queries
--
-- Dev variants mirror prod with _DEV suffix.

USE ROLE ACCOUNTADMIN;

-- ── Warehouses ────────────────────────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE    = 'XSMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'Snowpipe and dedup tasks';

CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING
    WAREHOUSE_SIZE    = 'SMALL'
    AUTO_SUSPEND      = 120
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'dbt production transforms';

CREATE WAREHOUSE IF NOT EXISTS REPORTING
    WAREHOUSE_SIZE    = 'XSMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'BI tool reads — Tableau, Looker, Metabase';

-- Dev variants
CREATE WAREHOUSE IF NOT EXISTS LOADING_WH_DEV
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING_DEV
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;


-- ── Roles ─────────────────────────────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS LOADER
    COMMENT = 'Used by Lambda/Snowpipe to write raw Salesforce data';

CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Used by dbt to read RAW and write ANALYTICS';

CREATE ROLE IF NOT EXISTS REPORTER
    COMMENT = 'Read-only access to ANALYTICS mart schemas for BI tools';

-- Dev variants
CREATE ROLE IF NOT EXISTS TRANSFORMER_DEV
    COMMENT = 'dbt developer role for ANALYTICS_DEV';


-- ── Role hierarchy ────────────────────────────────────────────────────────────

-- SYSADMIN owns all custom roles (standard Snowflake pattern)
GRANT ROLE LOADER          TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER     TO ROLE SYSADMIN;
GRANT ROLE REPORTER        TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER_DEV TO ROLE SYSADMIN;


-- ── Warehouse grants ─────────────────────────────────────────────────────────

GRANT USAGE ON WAREHOUSE LOADING_WH       TO ROLE LOADER;
GRANT USAGE ON WAREHOUSE TRANSFORMING     TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE REPORTING        TO ROLE REPORTER;
GRANT USAGE ON WAREHOUSE TRANSFORMING_DEV TO ROLE TRANSFORMER_DEV;
GRANT USAGE ON WAREHOUSE LOADING_WH_DEV   TO ROLE LOADER;


-- ── Databases ─────────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;

-- RAW: source-conformed data, no transforms
CREATE DATABASE IF NOT EXISTS RAW
    COMMENT = 'Raw landing zone — Snowpipe-loaded Salesforce data';

-- ANALYTICS: dbt output — staging, snapshots, marts
CREATE DATABASE IF NOT EXISTS ANALYTICS
    COMMENT = 'dbt-managed analytics layer';

CREATE DATABASE IF NOT EXISTS ANALYTICS_DEV
    COMMENT = 'Developer sandboxes for dbt';

-- ── Database and schema grants ───────────────────────────────────────────────

-- LOADER: needs to write to RAW.SALESFORCE
GRANT USAGE  ON DATABASE RAW               TO ROLE LOADER;
-- Schema and table grants are done in 02_schema_stage_format.sql after schema creation

-- TRANSFORMER: reads RAW, owns ANALYTICS
GRANT USAGE  ON DATABASE RAW               TO ROLE TRANSFORMER;
GRANT USAGE  ON DATABASE ANALYTICS         TO ROLE TRANSFORMER;
GRANT CREATE SCHEMA ON DATABASE ANALYTICS  TO ROLE TRANSFORMER;

-- REPORTER: read-only on ANALYTICS mart schemas (not staging or intermediate)
GRANT USAGE  ON DATABASE ANALYTICS         TO ROLE REPORTER;
-- Individual schema grants are set by dbt post-hooks via the grants macro

-- TRANSFORMER_DEV: full control over ANALYTICS_DEV
GRANT USAGE  ON DATABASE RAW               TO ROLE TRANSFORMER_DEV;
GRANT USAGE  ON DATABASE ANALYTICS_DEV    TO ROLE TRANSFORMER_DEV;
GRANT CREATE SCHEMA ON DATABASE ANALYTICS_DEV TO ROLE TRANSFORMER_DEV;


-- ── Service account users ────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- Lambda / Snowpipe service account
CREATE USER IF NOT EXISTS SVC_LOADER
    DEFAULT_ROLE      = LOADER
    DEFAULT_WAREHOUSE = LOADING_WH
    COMMENT           = 'Service account for Lambda extractor and Snowpipe';
GRANT ROLE LOADER TO USER SVC_LOADER;

-- dbt service account (uses key-pair auth — no password)
CREATE USER IF NOT EXISTS SVC_TRANSFORMER
    DEFAULT_ROLE      = TRANSFORMER
    DEFAULT_WAREHOUSE = TRANSFORMING
    COMMENT           = 'Service account for dbt production runs';
GRANT ROLE TRANSFORMER TO USER SVC_TRANSFORMER;

-- dbt dev user example (real developers get their own users)
-- CREATE USER IF NOT EXISTS DBT_MICHAEL
--     DEFAULT_ROLE      = TRANSFORMER_DEV
--     DEFAULT_WAREHOUSE = TRANSFORMING_DEV;
-- GRANT ROLE TRANSFORMER_DEV TO USER DBT_MICHAEL;


-- ── Network policy (optional, recommended for prod) ──────────────────────────
-- Restrict Lambda and dbt connections to known IP ranges.
-- Uncomment and populate ALLOWED_IP_LIST with your NAT gateway EIPs.

-- CREATE NETWORK POLICY IF NOT EXISTS NP_ALLOWED_SOURCES
--     ALLOWED_IP_LIST = (
--         '34.x.x.x/32',   -- Lambda NAT gateway EIP (prod)
--         '52.x.x.x/32',   -- GitHub Actions runner IP range
--         '10.0.0.0/8'      -- Internal VPC CIDR
--     )
--     COMMENT = 'Restricts Snowflake access to known sources';
--
-- ALTER USER SVC_LOADER       SET NETWORK_POLICY = NP_ALLOWED_SOURCES;
-- ALTER USER SVC_TRANSFORMER  SET NETWORK_POLICY = NP_ALLOWED_SOURCES;


-- ── Verification queries ─────────────────────────────────────────────────────
-- Run after setup to confirm the role hierarchy is correct.

-- SHOW ROLES;
-- SHOW WAREHOUSES;
-- SHOW GRANTS TO ROLE TRANSFORMER;
-- SHOW GRANTS TO ROLE LOADER;
-- SHOW GRANTS TO USER SVC_TRANSFORMER;
