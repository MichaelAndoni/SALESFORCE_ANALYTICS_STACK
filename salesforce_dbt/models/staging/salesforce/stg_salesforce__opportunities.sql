-- models/staging/salesforce/stg_salesforce__opportunities.sql

{{
  config(
    unique_key = 'opportunity_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'opportunity') }}

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
    id                                                    as opportunity_id,

    -- ── Core descriptors ─────────────────────────────────────────────────
    name                                                  as opportunity_name,
    type                                                  as opportunity_type,
    description,
    nextstep                                           as next_step,

    -- ── Relationships ─────────────────────────────────────────────────────
    accountid                                          as account_id,
    ownerid                                            as owner_id,
    campaignid                                         as campaign_id,

    -- ── Pipeline stage ────────────────────────────────────────────────────
    stagename                                          as stage_name,
    upper(forecastcategory)                              as forecast_category,

    -- ── Financials ────────────────────────────────────────────────────────
    amount::number(38, 2)                                 as amount,
    coalesce(
      expectedrevenue,
      probability / 100.0 * amount
    )::number(38, 2)                                      as expected_revenue,
    probability::number(5, 2)                             as probability_pct,
    totalopportunityquantity::number(38, 4)             as total_quantity,

    -- ── Dates ─────────────────────────────────────────────────────────────
    closedate::date                                      as close_date,
    fiscalquarter::integer                               as fiscal_quarter,
    fiscalyear::integer                                  as fiscal_year,

    -- ── Status flags ──────────────────────────────────────────────────────
    isclosed::boolean                                    as is_closed,
    iswon::boolean                                       as is_won,
    (isclosed and not iswon)::boolean                   as is_lost,
    hasopportunitylineitem::boolean                    as has_line_items,

    -- ── Lead source / attribution ─────────────────────────────────────────
    leadsource                                         as lead_source,

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
