-- models/marts/core/dim_date.sql
--
-- Calendar dimension covering the full analytical date range.
-- Uses Snowflake's native GENERATOR function — no recursive CTEs,
-- works on Snowflake Standard edition.

{%- set start = modules.datetime.datetime.strptime(var('date_spine_start'), '%Y-%m-%d').date() -%}
{%- set end   = modules.datetime.datetime.strptime(var('date_spine_end'),   '%Y-%m-%d').date() -%}
{%- set n_days = (end - start).days + 1 -%}

{{
  config(materialized = 'table')
}}

with date_spine as (

  select
    dateadd(day, seq4(), '{{ var("date_spine_start") }}'::date) as date_day
  from table(generator(rowcount => {{ n_days }}))

),

enhanced as (

  select
    -- ── Primary key ───────────────────────────────────────────────────────
    date_day                                              as date_id,

    -- ── Calendar attributes ───────────────────────────────────────────────
    date_day,
    dateadd(day, -1, date_day)::date                      as prior_date,
    dateadd(day,  1, date_day)::date                      as next_date,

    dayofweekiso(date_day)                                as day_of_week,
    decode(dayofweekiso(date_day),
      1,'Monday', 2,'Tuesday', 3,'Wednesday', 4,'Thursday',
      5,'Friday', 6,'Saturday', 7,'Sunday'
    )                                                     as day_of_week_name,
    dayname(date_day)                                     as day_of_week_name_short,
    dayofmonth(date_day)                                  as day_of_month,
    dayofyear(date_day)                                   as day_of_year,

    weekofyear(date_day)                                  as week_of_year,
    date_trunc('week',  date_day)::date                   as week_start_date,
    dateadd(day, 6, date_trunc('week', date_day))::date   as week_end_date,

    month(date_day)                                       as month_of_year,
    decode(month(date_day),
      1,'January', 2,'February', 3,'March',    4,'April',
      5,'May',     6,'June',     7,'July',     8,'August',
      9,'September', 10,'October', 11,'November', 12,'December'
    )                                                     as month_name,
    monthname(date_day)                                   as month_name_short,
    date_trunc('month', date_day)::date                   as month_start_date,
    last_day(date_day)                                    as month_end_date,

    quarter(date_day)                                     as quarter_of_year,
    date_trunc('quarter', date_day)::date                 as quarter_start_date,
    last_day(date_trunc('quarter', date_day), 'quarter')  as quarter_end_date,

    year(date_day)                                        as year_number,

    -- ── Fiscal calendar (offset = 0: fiscal year = calendar year) ─────────
    month(date_day)                                       as fiscal_month,
    quarter(date_day)                                     as fiscal_quarter,
    year(date_day)                                        as fiscal_year,
    'FY' || year(date_day)::varchar
      || '-Q' || quarter(date_day)::varchar               as fiscal_quarter_label,

    -- ── Boolean flags ─────────────────────────────────────────────────────
    dayofweekiso(date_day) in (6, 7)                      as is_weekend,
    date_day = date_trunc('month',   date_day)::date      as is_month_start,
    date_day = last_day(date_day)                         as is_month_end,
    date_day = date_trunc('quarter', date_day)::date      as is_quarter_start,
    date_day = last_day(date_trunc('quarter', date_day), 'quarter')
                                                          as is_quarter_end,
    date_day = date_trunc('year',    date_day)::date      as is_year_start,
    date_day = last_day(date_trunc('year', date_day), 'year')
                                                          as is_year_end,

    -- ── Rolling period helpers (relative to today) ─────────────────────────
    datediff('day',   date_day, current_date())           as days_ago,
    datediff('week',  date_day, current_date())           as weeks_ago,
    datediff('month', date_day, current_date())           as months_ago,
    datediff('year',  date_day, current_date())           as years_ago,

    (date_day <= current_date())                          as is_past_or_today,
    (date_day  = current_date())                          as is_today,
    (date_day  > current_date())                          as is_future

  from date_spine

)

select * from enhanced
