-- models/staging/salesforce/stg_salesforce__tasks.sql

{{
  config(
    unique_key = 'task_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'task') }}

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
    id                                                    as task_id,
    subject,
    type                                                  as task_type,          -- Call, Email, Meeting, etc.
    status                                                as task_status,        -- Not Started, In Progress, Completed, etc.
    priority                                              as task_priority,

    -- ── Who / What (polymorphic lookups) ──────────────────────────────────
    who_id                                                as who_id,             -- Lead or Contact
    what_id                                               as what_id,            -- Account, Opportunity, etc.
    owner_id,

    -- ── Activity details ──────────────────────────────────────────────────
    activity_date::date                                   as activity_date,
    description,
    call_type,
    call_duration_in_seconds::integer                     as call_duration_seconds,
    call_disposition,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_completed::boolean                                 as is_completed,
    is_high_priority::boolean                             as is_high_priority,
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
