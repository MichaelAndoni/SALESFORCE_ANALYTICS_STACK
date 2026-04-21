-- models/marts/sales/fct_bookings.sql
--
-- GRAIN: One row per closed-won Opportunity (a "booking").
--
-- This is the authoritative bookings ledger.  New bookings are inserted when
-- is_won transitions to true.  A booking record is never updated or deleted
-- after it is written — if an opportunity is reopened (rare), the is_active
-- flag is cleared in the merge.
--
-- Key metrics: ARR booked, TCV booked, ACV, bookings by segment / region /
-- rep / campaign, average deal size, and time-to-close.

{{
  config(
    unique_key = 'booking_id'
  )
}}

with won_opportunities as (
  select * from {{ ref('int_salesforce__opportunity_enriched') }}
  where is_won = true
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

-- For bookings we want the close_date as the booking date
final as (

  select
    -- ── Surrogate / natural keys ───────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['o.opportunity_id']) }}
                                                          as booking_id,
    o.opportunity_id,

    -- ── Foreign keys ──────────────────────────────────────────────────────
    da.account_sk,
    du.user_sk                                            as owner_sk,
    dc.campaign_sk,
    o.close_date                                          as booking_date_id,    -- FK → dim_date.date_id
    o.created_at::date                                    as created_date_id,

    -- ── Natural-key FKs ───────────────────────────────────────────────────
    o.account_id,
    o.owner_id,
    o.campaign_id,

    -- ── Deal descriptors ──────────────────────────────────────────────────
    o.opportunity_name,
    o.opportunity_type,
    o.stage_name,
    o.lead_source,
    o.primary_competitor,
    o.reason_won_lost,

    -- ── Core financials ───────────────────────────────────────────────────
    o.amount                                              as booking_amount,
    o.arr                                                 as arr_booked,
    o.mrr                                                 as mrr_booked,
    o.tcv                                                 as tcv_booked,
    o.total_quantity,
    o.line_item_count,
    o.arr_line_items,
    o.services_line_items,
    o.products_purchased,

    -- ── Contract metadata ─────────────────────────────────────────────────
    o.contract_start_date,
    o.contract_end_date,
    o.contract_term_months,

    -- ── Account context ───────────────────────────────────────────────────
    o.account_name,
    o.industry,
    o.billing_country,
    o.billing_state,
    o.account_annual_revenue,
    o.account_employees,
    da.employee_segment,
    da.revenue_segment,

    -- ── Owner context ─────────────────────────────────────────────────────
    o.owner_id,
    o.owner_name,
    o.owner_department,

    -- ── Campaign attribution ──────────────────────────────────────────────
    o.campaign_id,
    o.campaign_name,
    o.campaign_type,
    o.campaign_channel,

    -- ── Sales velocity metrics ────────────────────────────────────────────
    -- Days from opportunity creation to close
    datediff('day', o.created_at, o.close_date)           as days_to_close,
    -- Days in pipeline (created to last stage update)
    o.age_days,

    -- ── Fiscal period ─────────────────────────────────────────────────────
    o.fiscal_quarter,
    o.fiscal_year,
    'FY' || o.fiscal_year::varchar || '-Q' || o.fiscal_quarter::varchar
                                                          as fiscal_quarter_label,

    -- ── Booking classification ────────────────────────────────────────────
    case o.opportunity_type
      when 'New Business'  then 'New Logo'
      when 'Renewal'       then 'Renewal'
      when 'Upsell'        then 'Expansion'
      when 'Cross-Sell'    then 'Expansion'
      else 'Other'
    end                                                   as booking_category,

    -- ── Metadata ──────────────────────────────────────────────────────────
    o.close_date,
    o.created_at,
    o.updated_at,
    current_timestamp()                                   as dbt_updated_at

  from won_opportunities o
  left join dim_accounts  da on o.account_id  = da.account_id
  left join dim_users     du on o.owner_id    = du.user_id
  left join dim_campaigns dc on o.campaign_id = dc.campaign_id

)

select * from final
