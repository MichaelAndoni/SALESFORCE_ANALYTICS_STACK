-- models/staging/salesforce/stg_salesforce__campaigns.sql

{{
  config(
    unique_key = 'campaign_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'campaign') }}

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
    id                                                    as campaign_id,
    name                                                  as campaign_name,
    type                                                  as campaign_type,
    status                                                as campaign_status,
    parentid                                             as parent_campaign_id,
    ownerid                                            as owner_id,

    -- ── Schedule ──────────────────────────────────────────────────────────
    startdate::date                                      as start_date,
    enddate::date                                        as end_date,

    -- ── Budget & Costs ────────────────────────────────────────────────────
    budgetedcost::number(38, 2)                          as budgeted_cost,
    actualcost::number(38, 2)                            as actual_cost,

    -- ── Targets ───────────────────────────────────────────────────────────
    expectedrevenue::number(38, 2)                       as expected_revenue,
    expectedresponse::number(5, 2)                       as expected_response_pct,
    numbersent::integer                                  as number_sent,

    -- ── Actuals (Salesforce auto-roll-ups) ────────────────────────────────
    numberofleads::integer                              as number_of_leads,
    numberofconvertedleads::integer                    as number_of_converted_leads,
    numberofcontacts::integer                           as number_of_contacts,
    numberofresponses::integer                          as number_of_responses,
    numberofopportunities::integer                      as number_of_opportunities,
    numberofwonopportunities::integer                  as number_of_won_opportunities,
    amountallopportunities::number(38, 2)               as amount_all_opportunities,
    amountwonopportunities::number(38, 2)               as amount_won_opportunities,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isactive::boolean                                    as is_active,
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    lastmodifieddate::timestamp_ntz                     as last_modified_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
