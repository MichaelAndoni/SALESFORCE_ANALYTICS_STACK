-- models/marts/sales/fct_sales_pipeline.sql
--
-- GRAIN: One row per open Opportunity (pipeline snapshot as of last dbt run).
--
-- Purpose: tracks every deal currently in the pipeline — amount, stage,
-- expected close, and probability-weighted values.  BI tools filter this to
-- drive pipeline coverage, aging, and forecast dashboards.
--
-- Key design decisions:
--  • Only OPEN opportunities are in this table; won/lost move to fct_bookings.
--  • Incremental merge on opportunity_id — new deals are inserted, pipeline
--    updates (stage, amount, close date) are merged in.
--  • Closed deals are removed from the active set via a merge delete.
--    (Snowflake MERGE does not delete; instead we set a is_active flag and
--    BI tools filter on is_active = true.)

{{
  config(
    unique_key = 'pipeline_id'
  )
}}

with opportunities as (
  select * from {{ ref('int_salesforce__opportunity_enriched') }}
  where not is_closed  -- pipeline = not yet closed
),

dim_accounts as (
  select account_id, account_sk, employee_segment, revenue_segment
  from {{ ref('dim_account') }}
),

dim_users as (
  select user_id, user_sk
  from {{ ref('dim_user') }}
),

dim_campaigns as (
  select campaign_id, campaign_sk
  from {{ ref('dim_campaign') }}
),

final as (

  select
    -- ── Surrogate / natural keys ───────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['o.opportunity_id']) }}
                                                          as pipeline_id,
    o.opportunity_id,

    -- ── Foreign keys to dimensions ────────────────────────────────────────
    da.account_sk,
    du.user_sk                                            as owner_sk,
    dc.campaign_sk,
    o.close_date                                          as close_date_id,     -- FK → dim_date.date_id
    o.created_at::date                                    as created_date_id,   -- FK → dim_date.date_id

    -- ── Natural-key FKs (for convenience / ad-hoc joins) ──────────────────
    o.account_id,
    o.owner_id,
    o.campaign_id,

    -- ── Deal descriptors ──────────────────────────────────────────────────
    o.opportunity_name,
    o.opportunity_type,
    o.stage_name,
    o.forecast_category,
    o.lead_source,

    -- ── Financials ────────────────────────────────────────────────────────
    o.amount,
    o.probability_pct,
    -- Probability-weighted pipeline (commit view)
    (o.amount * o.probability_pct / 100.0)::number(38, 2)
                                                          as weighted_amount,
    o.expected_revenue,
    o.arr,
    o.mrr,
    o.tcv,

    -- ── Account context (denormalized for easy slice-n-dice) ───────────────
    o.account_name,
    o.industry,
    o.billing_country,
    da.employee_segment,
    da.revenue_segment,

    -- ── Owner context ─────────────────────────────────────────────────────
    o.owner_name,
    o.owner_department,

    -- ── Campaign context ──────────────────────────────────────────────────
    o.campaign_name,
    o.campaign_channel,

    -- ── Pipeline age & health ─────────────────────────────────────────────
    o.age_days,
    -- Days until close date (negative = past due)
    datediff('day', current_date(), o.close_date)         as days_to_close,
    -- Flag deals closing this quarter
    (o.fiscal_year    = year(current_date())
     and o.fiscal_quarter = quarter(current_date()))      as is_current_quarter_close,
    -- Flag stale deals (not updated in 30+ days)
    (datediff('day', o.updated_at, current_timestamp()) > 30)
                                                          as is_stale,

    -- ── Fiscal period ─────────────────────────────────────────────────────
    o.fiscal_quarter,
    o.fiscal_year,
    'FY' || o.fiscal_year::varchar || '-Q' || o.fiscal_quarter::varchar
                                                          as fiscal_quarter_label,

    -- ── Product detail ────────────────────────────────────────────────────
    o.line_item_count,
    o.products_purchased,

    -- ── Metadata ──────────────────────────────────────────────────────────
    o.created_at,
    o.updated_at,
    true                                                  as is_active,
    current_timestamp()                                   as dbt_updated_at

  from opportunities o
  left join dim_accounts  da on o.account_id  = da.account_id
  left join dim_users     du on o.owner_id    = du.user_id
  left join dim_campaigns dc on o.campaign_id = dc.campaign_id

)

select * from final
