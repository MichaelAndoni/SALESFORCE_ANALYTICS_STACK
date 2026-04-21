-- models/staging/salesforce/stg_salesforce__events.sql

{{
  config(
    unique_key = 'event_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'event') }}

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
    id                                                    as event_id,
    subject,
    type                                                  as event_type,

    -- ── Who / What ────────────────────────────────────────────────────────
    who_id,
    what_id,
    owner_id,

    -- ── Schedule ──────────────────────────────────────────────────────────
    activity_date_time::timestamp_ntz                     as start_datetime,
    end_date_time::timestamp_ntz                          as end_datetime,
    duration_in_minutes::integer                          as duration_minutes,
    location,
    description,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_all_day_event::boolean                             as is_all_day,
    is_private::boolean                                   as is_private,
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    created_date::timestamp_ntz                           as created_at,
    last_modified_date::timestamp_ntz                     as last_modified_at,
    system_modstamp::timestamp_ntz                        as updated_at,
    _fivetran_synced::timestamp_ntz                       as fivetran_synced_at

  from source

  where coalesce(is_deleted, false) = false
    and coalesce(_fivetran_deleted, false) = false

)

select * from cleaned
