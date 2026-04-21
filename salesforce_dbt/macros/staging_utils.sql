-- macros/staging_utils.sql
--
-- Pipeline-agnostic helpers for staging models.
--
-- The delete handling macro abstracts away the difference between:
--   • Fivetran:  _fivetran_deleted boolean column
--   • Lambda/Snowpipe:  ISDELETED boolean in the source record
-- so staging models can switch pipelines without SQL changes.

-- ---------------------------------------------------------------------------
-- is_hard_deleted(source)
--
-- Returns a SQL boolean expression that is TRUE when a row represents a
-- hard-deleted record that should be excluded from staging consumption.
--
-- source: 'fivetran' | 'lambda_snowpipe'
--
-- Usage:
--   where not {{ is_hard_deleted('lambda_snowpipe') }}
-- ---------------------------------------------------------------------------
{% macro is_hard_deleted(source=None) %}

  {%- set pipeline = source or var('pipeline_source', 'lambda_snowpipe') -%}

  {%- if pipeline == 'fivetran' -%}
    coalesce(_fivetran_deleted, false) = true

  {%- elif pipeline == 'lambda_snowpipe' -%}
    -- Lambda pipeline carries ISDELETED from Salesforce directly.
    -- The dedup views include deleted records so they flow into snapshots;
    -- staging models exclude them for downstream consumption.
    coalesce(isdeleted, false) = true

  {%- else -%}
    {{ exceptions.raise_compiler_error(
        "Unknown pipeline_source: " ~ pipeline ~
        ". Expected 'fivetran' or 'lambda_snowpipe'."
    ) }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- pipeline_loaded_at_column()
--
-- Returns the name of the column that tracks when a row was last loaded
-- by the pipeline.  Used in dbt source freshness checks and as a tiebreaker
-- in deduplication logic.
-- ---------------------------------------------------------------------------
{% macro pipeline_loaded_at_column() %}
  {%- set pipeline = var('pipeline_source', 'lambda_snowpipe') -%}
  {%- if pipeline == 'fivetran' -%}
    _fivetran_synced
  {%- else -%}
    _load_timestamp
  {%- endif -%}
{% endmacro %}
