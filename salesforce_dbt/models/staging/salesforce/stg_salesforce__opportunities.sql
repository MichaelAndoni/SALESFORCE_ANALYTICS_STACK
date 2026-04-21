-- models/staging/salesforce/stg_salesforce__opportunities.sql
--
-- The Opportunity is the center of gravity for both the sales pipeline and
-- bookings fact tables.  Extra care is taken here to:
--   • Normalize stage and forecast category to uppercase enums
--   • Derive a clean is_closed / is_won pair
--   • Compute expected_revenue when not populated by Salesforce
--   • Surface the fiscal period (if your org uses standard Salesforce fiscal)

{{
  config(
    unique_key = 'opportunity_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'opportunity') }}

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
    id                                                    as opportunity_id,

    -- ── Core descriptors ─────────────────────────────────────────────────
    name                                                  as opportunity_name,
    type                                                  as opportunity_type,    -- New Business, Renewal, Upsell, etc.
    description,
    next_step,

    -- ── Relationships ─────────────────────────────────────────────────────
    account_id,
    owner_id,
    campaign_id,

    -- ── Pipeline stage ────────────────────────────────────────────────────
    stage_name,
    upper(forecast_category)                              as forecast_category,  -- Omitted, Pipeline, BestCase, Commit, Closed

    -- ── Financials ────────────────────────────────────────────────────────
    amount::number(38, 2)                                 as amount,
    -- expected_revenue: use Salesforce value if present, else probability × amount
    coalesce(
      expected_revenue,
      probability / 100.0 * amount
    )::number(38, 2)                                      as expected_revenue,
    probability::number(5, 2)                             as probability_pct,
    total_opportunity_quantity::number(38, 4)             as total_quantity,

    -- ── Dates ─────────────────────────────────────────────────────────────
    close_date::date                                      as close_date,
    -- Fiscal period fields (only populated when using Salesforce fiscal year)
    fiscal_quarter::integer                               as fiscal_quarter,
    fiscal_year::integer                                  as fiscal_year,

    -- ── Status flags ──────────────────────────────────────────────────────
    is_closed::boolean                                    as is_closed,
    is_won::boolean                                       as is_won,
    -- Derived: lost = closed but not won
    (is_closed and not is_won)::boolean                   as is_lost,
    has_opportunity_line_item::boolean                    as has_line_items,

    -- ── Lead source / attribution ─────────────────────────────────────────
    lead_source,

    -- ── Custom fields (common across most orgs) ───────────────────────────
    competitor__c                                         as primary_competitor,
    reason_won_lost__c                                    as reason_won_lost,
    arr__c::number(38, 2)                                 as arr,               -- Annual Recurring Revenue
    mrr__c::number(38, 2)                                 as mrr,               -- Monthly Recurring Revenue
    tcv__c::number(38, 2)                                 as tcv,               -- Total Contract Value
    contract_start_date__c::date                          as contract_start_date,
    contract_end_date__c::date                            as contract_end_date,
    contract_term_months__c::integer                      as contract_term_months,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
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
