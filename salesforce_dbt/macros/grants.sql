-- macros/grants.sql
--
-- Post-hook macros to automatically grant SELECT to the REPORTER role on
-- every mart model after it is built.  Reference these in dbt_project.yml
-- via post-hook, or call directly from model configs.
--
-- Example model-level config:
--   {{ config(post_hook = "{{ grant_select_to_reporters() }}") }}
--
-- Example dbt_project.yml global post-hook:
--   models:
--     salesforce_dbt:
--       marts:
--         +post-hook: "{{ grant_select_to_reporters() }}"

{% macro grant_select_to_reporters(role='REPORTER') %}

  grant select on {{ this }} to role {{ role }};

{% endmacro %}


-- ---------------------------------------------------------------------------
-- grant_all_schemas_to_role
-- Run this once after a full-refresh to ensure all schemas exist and are
-- accessible.  Typically called from a post-hook on a sentinel model or
-- manually via dbt run-operation.
-- ---------------------------------------------------------------------------
{% macro grant_all_schemas_to_role(role='REPORTER', database='ANALYTICS') %}

  {% set schemas = [
    'STG_SALESFORCE',
    'SNAPSHOTS',
    'INTERMEDIATE',
    'MARTS_CORE',
    'MARTS_SALES',
    'MARTS_MARKETING'
  ] %}

  {% for schema in schemas %}
    grant usage  on schema {{ database }}.{{ schema }} to role {{ role }};
    grant select on all tables  in schema {{ database }}.{{ schema }} to role {{ role }};
    grant select on all views   in schema {{ database }}.{{ schema }} to role {{ role }};
    grant select on future tables in schema {{ database }}.{{ schema }} to role {{ role }};
    grant select on future views  in schema {{ database }}.{{ schema }} to role {{ role }};
  {% endfor %}

{% endmacro %}
