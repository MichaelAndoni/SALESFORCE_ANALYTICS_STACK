-- models/staging/salesforce/stg_salesforce__campaigns.sql

{{
  config(
    unique_key = 'campaign_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'campaign') }}

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
    id                                                    as campaign_id,
    name                                                  as campaign_name,
    type                                                  as campaign_type,      -- Email, Webinar, Event, etc.
    status                                                as campaign_status,    -- Planned, In Progress, Completed, Aborted
    parent_id                                             as parent_campaign_id,
    owner_id,

    -- ── Schedule ──────────────────────────────────────────────────────────
    start_date::date                                      as start_date,
    end_date::date                                        as end_date,

    -- ── Budget & Costs ────────────────────────────────────────────────────
    budgeted_cost::number(38, 2)                          as budgeted_cost,
    actual_cost::number(38, 2)                            as actual_cost,

    -- ── Targets ───────────────────────────────────────────────────────────
    expected_revenue::number(38, 2)                       as expected_revenue,
    expected_response::number(5, 2)                       as expected_response_pct,
    number_sent::integer                                  as number_sent,

    -- ── Actuals (Salesforce auto-roll-ups) ────────────────────────────────
    number_of_leads::integer                              as number_of_leads,
    number_of_converted_leads::integer                    as number_of_converted_leads,
    number_of_contacts::integer                           as number_of_contacts,
    number_of_responses::integer                          as number_of_responses,
    number_of_opportunities::integer                      as number_of_opportunities,
    number_of_won_opportunities::integer                  as number_of_won_opportunities,
    amount_all_opportunities::number(38, 2)               as amount_all_opportunities,
    amount_won_opportunities::number(38, 2)               as amount_won_opportunities,

    -- ── UTM / channel (common custom fields) ──────────────────────────────
    utm_source__c                                         as utm_source,
    utm_medium__c                                         as utm_medium,
    utm_content__c                                        as utm_content,
    channel__c                                            as channel,            -- Paid, Organic, Partner, etc.

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_active::boolean                                    as is_active,
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
