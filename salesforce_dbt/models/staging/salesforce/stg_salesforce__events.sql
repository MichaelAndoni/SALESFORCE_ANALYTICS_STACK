-- models/staging/salesforce/stg_salesforce__events.sql

{{
  config(
    unique_key = 'event_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'event') }}

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
    id                                                    as event_id,
    subject,

    -- ── Who / What ────────────────────────────────────────────────────────
    whoid                                              as who_id,
    whatid                                             as what_id,
    ownerid                                            as owner_id,

    -- ── Schedule ──────────────────────────────────────────────────────────
    activitydatetime::timestamp_ntz                     as start_datetime,
    enddatetime::timestamp_ntz                          as end_datetime,
    durationinminutes::integer                          as duration_minutes,
    location,
    description,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isalldayevent::boolean                             as is_all_day,
    isprivate::boolean                                   as is_private,
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
