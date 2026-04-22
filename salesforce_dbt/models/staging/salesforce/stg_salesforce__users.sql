-- models/staging/salesforce/stg_salesforce__users.sql

{{
  config(
    unique_key = 'user_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'user') }}

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
    id                                                    as user_id,

    -- ── Identity ──────────────────────────────────────────────────────────
    firstname                                          as first_name,
    lastname                                           as last_name,
    name                                                  as full_name,
    email,
    username,
    alias,
    title,
    department,
    division,
    phone,
    mobilephone                                        as mobile_phone,

    -- ── Org hierarchy ─────────────────────────────────────────────────────
    managerid                                          as manager_id,
    userroleid                                         as user_role_id,
    profileid                                          as profile_id,

    -- ── Locale ────────────────────────────────────────────────────────────
    timezonesidkey                                     as timezone,
    localesidkey                                        as locale,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isactive::boolean                                    as is_active,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at,
    lastlogindate::timestamp_ntz                        as last_login_at

  from source

  -- NOTE: We keep inactive users so historical records retain rep names

)

select * from cleaned
