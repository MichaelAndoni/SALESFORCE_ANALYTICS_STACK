-- models/staging/salesforce/stg_salesforce__tasks.sql

{{
  config(
    unique_key = 'task_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'task') }}

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
    id                                                    as task_id,
    subject,
    status                                                as task_status,
    priority                                              as task_priority,

    -- ── Who / What (polymorphic lookups) ──────────────────────────────────
    whoid                                                as who_id,
    whatid                                               as what_id,
    ownerid                                            as owner_id,

    -- ── Activity details ──────────────────────────────────────────────────
    activitydate::date                                   as activity_date,
    description,

    -- ── Flags ─────────────────────────────────────────────────────────────
    ishighpriority::boolean                             as is_high_priority,
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
