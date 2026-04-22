-- models/staging/salesforce/stg_salesforce__leads.sql

{{
  config(
    unique_key = 'lead_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'lead') }}

  {% if is_incremental() %}
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
    id                                                    as lead_id,

    -- ── Identity ──────────────────────────────────────────────────────────
    firstname                                          as first_name,
    lastname                                           as last_name,
    name                                                  as full_name,
    salutation,
    title,
    email,
    phone,
    mobilephone                                        as mobile_phone,

    -- ── Company ───────────────────────────────────────────────────────────
    company,
    industry,
    annualrevenue::number(38, 2)                         as annual_revenue,
    numberofemployees::integer                          as number_of_employees,
    website,

    -- ── Qualification ─────────────────────────────────────────────────────
    status,
    leadsource                                         as lead_source,
    rating,

    -- ── Address ───────────────────────────────────────────────────────────
    street,
    city,
    state,
    postalcode                                         as postal_code,
    country,

    -- ── Conversion metadata ───────────────────────────────────────────────
    isconverted::boolean                                 as is_converted,
    converteddate::date                                  as converted_date,
    convertedaccountid                                 as converted_account_id,
    convertedcontactid                                 as converted_contact_id,
    convertedopportunityid                             as converted_opportunity_id,

    -- ── Ownership ─────────────────────────────────────────────────────────
    ownerid                                            as owner_id,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at,
    lastactivitydate::date                              as last_activity_date

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
