-- snapshots/snap_salesforce__accounts.sql
--
-- SCD Type-2 history for Account.  Every time a field changes, a new row is
-- inserted with updated dbt_valid_from / dbt_valid_to metadata.
-- The most-current record has dbt_valid_to IS NULL.
--
-- Key use-cases:
--   • Point-in-time joins from fact tables to account attributes
--   • Auditing changes to account owner, industry, tier, etc.

{% snapshot snap_salesforce__accounts %}

{{
  config(
    unique_key = 'account_id',
    strategy   = 'timestamp',
    updated_at = 'updated_at',
    invalidate_hard_deletes = true
  )
}}

select * from {{ ref('stg_salesforce__accounts') }}

{% endsnapshot %}
