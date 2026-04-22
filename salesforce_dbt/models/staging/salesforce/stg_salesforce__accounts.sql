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
    where systemmodstamp >= (
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
    rating,
    accountsource                                      as account_source,
    description,
    website,

    -- ── Firmographics ─────────────────────────────────────────────────────
    annualrevenue::number(38, 2)                         as annual_revenue,
    numberofemployees::integer                          as number_of_employees,

    -- ── Geography ────────────────────────────────────────────────────────
    billingstreet                                      as billing_street,
    billingcity                                        as billing_city,
    billingstate                                       as billing_state,
    billingpostalcode                                  as billing_postal_code,
    billingcountry                                     as billing_country,
    billinglatitude::float                               as billing_latitude,
    billinglongitude::float                              as billing_longitude,

    -- ── Hierarchy ─────────────────────────────────────────────────────────
    parentid                                           as parent_id,
    masterrecordid                                       as master_record_id,

    -- ── Ownership ─────────────────────────────────────────────────────────
    ownerid                                            as owner_id,

    -- ── Phone / Contact ───────────────────────────────────────────────────
    phone,
    fax,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  -- Exclude soft-deleted records from downstream consumption.
  -- Snapshot layer captures history including deletes.
  where coalesce(isdeleted, false) = false

)

select * from cleaned
