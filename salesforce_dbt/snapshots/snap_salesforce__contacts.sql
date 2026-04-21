-- snapshots/snap_salesforce__contacts.sql
--
-- SCD Type-2 history for Contact.
-- Captures changes to title, department, account assignment, and owner.

{% snapshot snap_salesforce__contacts %}

{{
  config(
    unique_key = 'contact_id',
    strategy   = 'timestamp',
    updated_at = 'updated_at',
    invalidate_hard_deletes = true
  )
}}

select * from {{ ref('stg_salesforce__contacts') }}

{% endsnapshot %}
