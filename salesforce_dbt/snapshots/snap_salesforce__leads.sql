-- snapshots/snap_salesforce__leads.sql
--
-- SCD Type-2 history for Lead.
-- Critical for reconstructing which stage / status a lead was in at any point,
-- and tracking the conversion funnel over time.

{% snapshot snap_salesforce__leads %}

{{
  config(
    unique_key = 'lead_id',
    strategy   = 'timestamp',
    updated_at = 'updated_at',
    invalidate_hard_deletes = true
  )
}}

select * from {{ ref('stg_salesforce__leads') }}

{% endsnapshot %}
