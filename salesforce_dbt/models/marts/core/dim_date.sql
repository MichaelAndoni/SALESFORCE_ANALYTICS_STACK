-- models/marts/core/dim_date.sql
--
-- Calendar dimension covering the full analytical date range.
-- Built using dbt_date's get_date_dimension macro for Snowflake.
-- Materialized as a TABLE (tiny, static-ish, fast joins).

{{
  config(materialized = 'table')
}}

with date_spine as (

  {{
    dbt_date.get_date_dimension(
      var('date_spine_start'),
      var('date_spine_end')
    )
  }}

),

enhanced as (

  select
    -- ── Primary key ───────────────────────────────────────────────────────
    date_day                                              as date_id,          -- YYYY-MM-DD date, used as FK in fact tables

    -- ── Calendar attributes ───────────────────────────────────────────────
    date_day,
    prior_date_day                                        as prior_date,
    next_date_day                                         as next_date,

    day_of_week,
    day_of_week_name,
    day_of_week_name_short,
    day_of_month,
    day_of_year,

    week_of_year,
    week_start_date,
    week_end_date,

    month_of_year,
    month_name,
    month_name_short,
    month_start_date,
    month_end_date,

    quarter_of_year,
    quarter_start_date,
    quarter_end_date,

    year_number,

    -- ── Fiscal calendar (adjust offset to match your org's fiscal year) ────
    -- Default assumes fiscal year = calendar year (offset 0).
    -- If your fiscal year starts in February, set fiscal_month_offset = 1.
    month_of_year                                         as fiscal_month,
    quarter_of_year                                       as fiscal_quarter,
    year_number                                           as fiscal_year,

    -- Fiscal quarter label e.g. "FY2024-Q3"
    'FY' || year_number::varchar || '-Q' || quarter_of_year::varchar
                                                          as fiscal_quarter_label,

    -- ── Boolean flags ─────────────────────────────────────────────────────
    is_weekend,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,

    -- ── Rolling period helpers (relative to today) ─────────────────────────
    datediff('day',  date_day, current_date())            as days_ago,
    datediff('week', date_day, current_date())            as weeks_ago,
    datediff('month',date_day, current_date())            as months_ago,
    datediff('year', date_day, current_date())            as years_ago,

    (date_day <= current_date())                          as is_past_or_today,
    (date_day  = current_date())                          as is_today,
    (date_day  > current_date())                          as is_future

  from date_spine

)

select * from enhanced
