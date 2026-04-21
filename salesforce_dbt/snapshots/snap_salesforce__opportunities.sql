-- snapshots/snap_salesforce__opportunities.sql
--
-- SCD Type-2 history for Opportunity.  This is the most analytically important
-- snapshot in the project.  It enables:
--
--   • Pipeline waterfall reports: what was in each stage on any past date?
--   • Stage velocity: how long did deals spend in each stage?
--   • Forecast accuracy: what was predicted vs. actual at quarter close?
--   • Stage regression detection: did deals move backward?
--
-- Each time stage_name, amount, close_date, owner_id, or forecast_category
-- changes, a new snapshot row is created.

{% snapshot snap_salesforce__opportunities %}

{{
  config(
    unique_key = 'opportunity_id',
    strategy   = 'timestamp',
    updated_at = 'updated_at',
    invalidate_hard_deletes = true
  )
}}

select * from {{ ref('stg_salesforce__opportunities') }}

{% endsnapshot %}
