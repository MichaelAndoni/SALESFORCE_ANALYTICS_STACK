-- models/staging/salesforce/stg_salesforce__contacts.sql

{{
  config(
    unique_key = 'contact_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'contact') }}

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
    id                                                    as contact_id,

    -- ── Name ──────────────────────────────────────────────────────────────
    first_name,
    last_name,
    name                                                  as full_name,
    salutation,

    -- ── Role ──────────────────────────────────────────────────────────────
    title,
    department,

    -- ── Relationships ─────────────────────────────────────────────────────
    account_id,
    reports_to_id,
    owner_id,

    -- ── Contact details ───────────────────────────────────────────────────
    email,
    phone,
    mobile_phone,
    fax,
    linkedin_url__c                                       as linkedin_url,

    -- ── Lead source (how this person was originally acquired) ──────────────
    lead_source,

    -- ── Mailing address ───────────────────────────────────────────────────
    mailing_street,
    mailing_city,
    mailing_state,
    mailing_postal_code,
    mailing_country,

    -- ── Marketing / engagement flags ──────────────────────────────────────
    has_opted_out_of_email::boolean                       as has_opted_out_of_email,
    do_not_call::boolean                                  as do_not_call,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    birthdate::date                                       as birth_date,
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
