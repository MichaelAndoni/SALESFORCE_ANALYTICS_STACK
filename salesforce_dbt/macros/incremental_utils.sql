-- macros/incremental_utils.sql
--
-- Shared helpers used across staging and marts incremental models.

-- ---------------------------------------------------------------------------
-- incremental_filter(column, lookback_days)
--
-- Generates the WHERE clause fragment for incremental runs.
-- Uses a configurable lookback window (default: var incremental_lookback_days)
-- to reprocess recently modified records, guarding against late-arriving
-- source updates and Fivetran re-syncs.
--
-- Usage:
--   {% if is_incremental() %}
--     where {{ incremental_filter('system_modstamp') }}
--   {% endif %}
-- ---------------------------------------------------------------------------
{% macro incremental_filter(column='system_modstamp', lookback_days=none) %}

  {%- set lb = lookback_days if lookback_days is not none
               else var('incremental_lookback_days', 3) -%}

  {{ column }} >= (
    select dateadd('day', -{{ lb }}, max({{ column }}))
    from {{ this }}
  )

{% endmacro %}


-- ---------------------------------------------------------------------------
-- safe_cast_numeric(column)   – returns 0.00 when value is null
-- ---------------------------------------------------------------------------
{% macro safe_amount(column) %}
  coalesce({{ column }}, 0)::number(38, 2)
{% endmacro %}
