-- models/intermediate/salesforce/int_salesforce__opportunity_enriched.sql
--
-- Enriches Opportunity with account, owner, campaign, and line-item aggregates.
-- Materialized as EPHEMERAL — no physical table; inlined into downstream models
-- at compile time.  This keeps the mart models readable without the cost of a
-- physical intermediate table.
--
-- Downstream consumers: fct_sales_pipeline, fct_bookings, fct_win_rate

with opportunities as (
  select * from {{ ref('stg_salesforce__opportunities') }}
),

accounts as (
  select
    account_id,
    account_name,
    industry,
    account_type,
    billing_country,
    billing_state,
    annual_revenue,
    number_of_employees
  from {{ ref('stg_salesforce__accounts') }}
),

users as (
  select
    user_id,
    full_name   as owner_name,
    department  as owner_department,
    manager_id  as owner_manager_id
  from {{ ref('stg_salesforce__users') }}
),

campaigns as (
  select
    campaign_id,
    campaign_name,
    campaign_type,
    null as channel
  from {{ ref('stg_salesforce__campaigns') }}
),

-- Roll up line items to opportunity grain
line_item_agg as (
  select
    opportunity_id,
    count(*)                    as line_item_count,
    sum(total_price)            as line_items_total,
    cast(null as number(38, 2))                                          as arr_line_items,
    cast(null as number(38, 2))                                          as services_line_items,
    listagg(distinct product_name, ', ')                                  as products_purchased
  from {{ ref('stg_salesforce__opportunity_line_items') }}
  group by 1
),

enriched as (
  select
    -- ── Opportunity core ─────────────────────────────────────────────
    o.opportunity_id,
    o.opportunity_name,
    o.opportunity_type,
    o.stage_name,
    o.forecast_category,
    o.amount,
    o.expected_revenue,
    o.probability_pct,
    o.total_quantity,
    null                                                  as arr,
    null                                                  as mrr,
    null                                                  as tcv,
    o.lead_source,
    null                                                  as primary_competitor,
    null                                                  as reason_won_lost,
    o.is_closed,
    o.is_won,
    o.is_lost,
    o.has_line_items,

    -- ── Dates ────────────────────────────────────────────────────────
    o.close_date,
    o.fiscal_quarter,
    o.fiscal_year,
    o.created_at,
    o.updated_at,
    null                                                  as contract_start_date,
    null                                                  as contract_end_date,
    null                                                  as contract_term_months,
    -- Age of the opportunity in days (from create to close or today)
    datediff('day', o.created_at,
             coalesce(case when o.is_closed then o.close_date end,
                      current_date()))                    as age_days,

    -- ── Account context ──────────────────────────────────────────────
    o.account_id,
    a.account_name,
    a.industry,
    a.account_type,
    a.billing_country,
    a.billing_state,
    a.annual_revenue                                      as account_annual_revenue,
    a.number_of_employees                                 as account_employees,

    -- ── Owner context ─────────────────────────────────────────────────
    o.owner_id,
    u.owner_name,
    u.owner_department,
    u.owner_manager_id,

    -- ── Campaign attribution ──────────────────────────────────────────
    o.campaign_id,
    c.campaign_name,
    c.campaign_type,
    null                                                  as campaign_channel,

    -- ── Line item aggregates ──────────────────────────────────────────
    coalesce(li.line_item_count, 0)                       as line_item_count,
    li.line_items_total,
    li.arr_line_items,
    li.services_line_items,
    li.products_purchased

  from opportunities o
  left join accounts   a  on o.account_id  = a.account_id
  left join users      u  on o.owner_id    = u.user_id
  left join campaigns  c  on o.campaign_id = c.campaign_id
  left join line_item_agg li on o.opportunity_id = li.opportunity_id
)

select * from enriched
