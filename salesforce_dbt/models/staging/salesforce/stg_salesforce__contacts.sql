-- models/staging/salesforce/stg_salesforce__contacts.sql

{{
  config(
    unique_key = 'contact_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'contact') }}

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
    id                                                    as contact_id,

    -- ── Name ──────────────────────────────────────────────────────────────
    firstname                                          as first_name,
    lastname                                           as last_name,
    name                                                  as full_name,
    salutation,

    -- ── Role ──────────────────────────────────────────────────────────────
    title,
    department,

    -- ── Relationships ─────────────────────────────────────────────────────
    accountid                                          as account_id,
    reportstoid                                        as reports_to_id,
    ownerid                                            as owner_id,

    -- ── Contact details ───────────────────────────────────────────────────
    email,
    phone,
    mobilephone                                        as mobile_phone,
    fax,

    -- ── Lead source (how this person was originally acquired) ──────────────
    leadsource                                         as lead_source,

    -- ── Mailing address ───────────────────────────────────────────────────
    mailingstreet                                      as mailing_street,
    mailingcity                                        as mailing_city,
    mailingstate                                       as mailing_state,
    mailingpostalcode                                  as mailing_postal_code,
    mailingcountry                                     as mailing_country,

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
