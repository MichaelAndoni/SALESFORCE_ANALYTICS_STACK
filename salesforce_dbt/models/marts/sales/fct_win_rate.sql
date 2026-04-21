-- models/marts/sales/fct_win_rate.sql
--
-- GRAIN: One row per closed Opportunity (won OR lost).
--
-- Purpose: Win-rate analysis across every dimension — rep, team, segment,
-- industry, lead source, opportunity type, deal size band, and fiscal period.
-- Built directly from fct_bookings (won) and the closed-lost opps from staging.
--
-- Downstream: win_rate = sum(is_won) / count(*)  grouped by any dimension.

{{
  config(
    unique_key = 'win_rate_id'
  )
}}

with closed_opportunities as (
  -- All closed opportunities: won + lost
  select * from {{ ref('int_salesforce__opportunity_enriched') }}
  where is_closed = true
),

dim_accounts as (
  select account_id, account_sk, employee_segment, revenue_segment
  from {{ ref('dim_account') }}
),

dim_users as (
  select user_id, user_sk
  from {{ ref('dim_user') }}
),

final as (

  select
    -- ── Surrogate / natural keys ───────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['o.opportunity_id']) }}
                                                          as win_rate_id,
    o.opportunity_id,

    -- ── FK references ─────────────────────────────────────────────────────
    da.account_sk,
    du.user_sk                                            as owner_sk,
    o.close_date                                          as close_date_id,
    o.created_at::date                                    as created_date_id,
    o.account_id,
    o.owner_id,
    o.campaign_id,

    -- ── Outcome flags ─────────────────────────────────────────────────────
    o.is_won,
    o.is_lost,
    o.is_won::integer                                     as won_count,          -- 1/0 for easy sum()
    o.is_lost::integer                                    as lost_count,

    -- ── Deal info ─────────────────────────────────────────────────────────
    o.opportunity_name,
    o.opportunity_type,
    o.stage_name                                          as final_stage_name,   -- last stage before close
    o.lead_source,
    o.primary_competitor,
    o.reason_won_lost,

    -- ── Financials ────────────────────────────────────────────────────────
    o.amount,
    o.arr,

    -- ── Deal size band ────────────────────────────────────────────────────
    case
      when o.amount < 10000                then 'Small  (< $10K)'
      when o.amount < 50000                then 'Mid    ($10K–$50K)'
      when o.amount < 250000               then 'Large  ($50K–$250K)'
      when o.amount >= 250000              then 'Enterprise ($250K+)'
      else 'Unknown'
    end                                                   as deal_size_band,

    -- ── Account context ───────────────────────────────────────────────────
    o.account_name,
    o.industry,
    o.billing_country,
    da.employee_segment,
    da.revenue_segment,

    -- ── Owner context ─────────────────────────────────────────────────────
    o.owner_name,
    o.owner_department,

    -- ── Velocity ──────────────────────────────────────────────────────────
    datediff('day', o.created_at, o.close_date)           as days_to_close,

    -- ── Fiscal period ─────────────────────────────────────────────────────
    o.fiscal_quarter,
    o.fiscal_year,
    'FY' || o.fiscal_year::varchar || '-Q' || o.fiscal_quarter::varchar
                                                          as fiscal_quarter_label,

    -- ── Metadata ──────────────────────────────────────────────────────────
    o.close_date,
    o.created_at,
    o.updated_at,
    current_timestamp()                                   as dbt_updated_at

  from closed_opportunities o
  left join dim_accounts da on o.account_id = da.account_id
  left join dim_users    du on o.owner_id   = du.user_id

)

select * from final
