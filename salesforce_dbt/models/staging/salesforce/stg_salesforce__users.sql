-- models/staging/salesforce/stg_salesforce__users.sql

{{
  config(
    unique_key = 'user_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'user') }}

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
    id                                                    as user_id,

    -- ── Identity ──────────────────────────────────────────────────────────
    first_name,
    last_name,
    name                                                  as full_name,
    email,
    username,
    alias,
    title,
    department,
    division,
    phone,
    mobile_phone,

    -- ── Org hierarchy ─────────────────────────────────────────────────────
    manager_id,
    user_role_id,
    profile_id,

    -- ── Locale ────────────────────────────────────────────────────────────
    time_zone_sid_key                                     as timezone,
    locale_sid_key                                        as locale,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_active::boolean                                    as is_active,
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    created_date::timestamp_ntz                           as created_at,
    last_modified_date::timestamp_ntz                     as last_modified_at,
    system_modstamp::timestamp_ntz                        as updated_at,
    last_login_date::timestamp_ntz                        as last_login_at,
    _fivetran_synced::timestamp_ntz                       as fivetran_synced_at

  from source

  where coalesce(_fivetran_deleted, false) = false
  -- NOTE: We keep inactive users so historical records retain rep names

)

select * from cleaned
