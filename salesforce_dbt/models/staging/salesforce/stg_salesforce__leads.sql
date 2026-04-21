-- models/staging/salesforce/stg_salesforce__leads.sql

{{
  config(
    unique_key = 'lead_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'lead') }}

  {% if is_incremental() %}
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
    id                                                    as lead_id,

    -- ── Identity ──────────────────────────────────────────────────────────
    first_name,
    last_name,
    name                                                  as full_name,
    salutation,
    title,
    email,
    phone,
    mobile_phone,

    -- ── Company ───────────────────────────────────────────────────────────
    company,
    industry,
    annual_revenue::number(38, 2)                         as annual_revenue,
    number_of_employees::integer                          as number_of_employees,
    website,

    -- ── Qualification ─────────────────────────────────────────────────────
    status,
    lead_source,
    rating,

    -- ── Address ───────────────────────────────────────────────────────────
    street,
    city,
    state,
    postal_code,
    country,

    -- ── Conversion metadata ───────────────────────────────────────────────
    is_converted::boolean                                 as is_converted,
    converted_date::date                                  as converted_date,
    converted_account_id,
    converted_contact_id,
    converted_opportunity_id,

    -- ── Ownership ─────────────────────────────────────────────────────────
    owner_id,

    -- ── Campaign attribution ──────────────────────────────────────────────
    campaign_id                                           as original_campaign_id,

    -- ── Marketing opt-out ─────────────────────────────────────────────────
    has_opted_out_of_email::boolean                       as has_opted_out_of_email,
    do_not_call::boolean                                  as do_not_call,

    -- ── Scoring / enrichment (common custom fields) ───────────────────────
    lead_score__c::integer                                as lead_score,
    lead_grade__c                                         as lead_grade,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    created_date::timestamp_ntz                           as created_at,
    last_modified_date::timestamp_ntz                     as last_modified_at,
    system_modstamp::timestamp_ntz                        as updated_at,
    last_activity_date::date                              as last_activity_date,
    _fivetran_synced::timestamp_ntz                       as fivetran_synced_at

  from source

  where coalesce(is_deleted, false) = false
    and coalesce(_fivetran_deleted, false) = false

)

select * from cleaned
