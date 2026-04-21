-- analyses/pipeline_coverage_by_quarter.sql
--
-- Ad-hoc analysis: Pipeline coverage ratio by fiscal quarter.
-- Coverage = open pipeline / bookings target.
-- This is not a dbt model (won't be materialized) — run with dbt compile
-- then execute the compiled SQL directly.

with pipeline as (
  select
    fiscal_quarter_label,
    sum(amount)          as open_pipeline,
    sum(weighted_amount) as weighted_pipeline,
    count(*)             as deal_count
  from {{ ref('fct_sales_pipeline') }}
  group by 1
),

bookings as (
  select
    fiscal_quarter_label,
    sum(booking_amount)  as bookings,
    count(*)             as won_deals,
    sum(arr_booked)      as arr_booked
  from {{ ref('fct_bookings') }}
  group by 1
),

-- Hardcode targets or join to a seed/table
-- Example: 3× coverage target
coverage as (
  select
    coalesce(p.fiscal_quarter_label, b.fiscal_quarter_label) as fiscal_quarter_label,
    coalesce(p.open_pipeline, 0)                              as open_pipeline,
    coalesce(p.weighted_pipeline, 0)                          as weighted_pipeline,
    coalesce(p.deal_count, 0)                                 as open_deals,
    coalesce(b.bookings, 0)                                   as bookings,
    coalesce(b.won_deals, 0)                                  as won_deals,
    coalesce(b.arr_booked, 0)                                 as arr_booked,
    -- Coverage ratio (pipeline / bookings so far this quarter)
    case
      when coalesce(b.bookings, 0) = 0 then null
      else round(p.open_pipeline / b.bookings, 2)
    end                                                       as coverage_ratio
  from pipeline    p
  full outer join bookings b
    using (fiscal_quarter_label)
)

select
  fiscal_quarter_label,
  open_pipeline,
  weighted_pipeline,
  open_deals,
  bookings,
  won_deals,
  arr_booked,
  coverage_ratio,
  case
    when coverage_ratio >= 3.0 then '✅ Healthy'
    when coverage_ratio >= 2.0 then '⚠️  At Risk'
    when coverage_ratio is not null then '🔴 Critical'
    else '—'
  end as coverage_health
from coverage
order by fiscal_quarter_label
