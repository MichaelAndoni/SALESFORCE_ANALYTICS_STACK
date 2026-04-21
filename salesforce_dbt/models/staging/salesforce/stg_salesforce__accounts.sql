-- models/staging/salesforce/stg_salesforce__accounts.sql
--
-- Incrementally stages Salesforce Account records from the raw source.
-- Strategy: MERGE on account_id.
-- Soft-deleted records (is_deleted OR _fivetran_deleted) are excluded so that
-- downstream models never see ghost rows.  Deleted IDs still flow through
-- the snapshot layer for audit purposes.

{{
  config(
    unique_key = 'account_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'account') }}

  {% if is_incremental() %}
    -- Reprocess records modified within the lookback window.
    -- The 3-day buffer handles Fivetran re-syncs and late-arriving CDC events.
    where system_modstamp >= (
      select dateadd('day', -{{ var('incremental_lookback_days') }},
                     max(updated_at))
      from {{ this }}
    )
  {% endif %}

),

cleaned as (

  select
    -- ── Primary key ──────────────────────────────────────────────────────
    id                                                    as account_id,

    -- ── Core descriptors ─────────────────────────────────────────────────
    name                                                  as account_name,
    type                                                  as account_type,
    industry,
    sub_industry__c                                       as sub_industry,   -- common custom field
    rating,
    account_source,
    description,
    website,

    -- ── Firmographics ─────────────────────────────────────────────────────
    annual_revenue::number(38, 2)                         as annual_revenue,
    number_of_employees::integer                          as number_of_employees,

    -- ── Geography ────────────────────────────────────────────────────────
    billing_street,
    billing_city,
    billing_state,
    billing_postal_code,
    billing_country,
    billing_latitude::float                               as billing_latitude,
    billing_longitude::float                              as billing_longitude,

    -- ── Hierarchy ─────────────────────────────────────────────────────────
    parent_id,
    masterrecord_id                                       as master_record_id,

    -- ── Ownership ─────────────────────────────────────────────────────────
    owner_id,

    -- ── Phone / Contact ───────────────────────────────────────────────────
    phone,
    fax,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    created_date::timestamp_ntz                           as created_at,
    last_modified_date::timestamp_ntz                     as last_modified_at,
    system_modstamp::timestamp_ntz                        as updated_at,      -- canonical incremental key
    _fivetran_synced::timestamp_ntz                       as fivetran_synced_at

  from source

  -- Exclude soft-deleted records from downstream consumption.
  -- Snapshot layer captures history including deletes.
  where coalesce(is_deleted, false) = false
    and coalesce(_fivetran_deleted, false) = false

)

select * from cleaned
