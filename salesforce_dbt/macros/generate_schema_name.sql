-- macros/generate_schema_name.sql
--
-- Overrides dbt's default schema naming so that every model lands in its own
-- schema (e.g. STG_SALESFORCE, MARTS_SALES) rather than
-- <target_schema>_<custom_schema>.
--
-- In production the schema name is used as-is.
-- In dev the developer's target schema is prepended so that personal sandboxes
-- don't collide:  dbt_michael_stg_salesforce, dbt_michael_marts_sales, etc.

{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- set default_schema = target.schema -%}

  {%- if custom_schema_name is none -%}
    {{ default_schema }}

  {%- elif target.name == 'prod' -%}
    {#- In prod: use the custom schema name directly (no prefix) -#}
    {{ custom_schema_name | trim | upper }}

  {%- else -%}
    {#- In dev/ci: prefix with the developer's personal schema so sandboxes never collide -#}
    {{ default_schema | trim | upper }}_{{ custom_schema_name | trim | upper }}

  {%- endif -%}

{%- endmacro %}
