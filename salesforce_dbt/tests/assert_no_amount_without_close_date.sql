-- tests/assert_no_amount_without_close_date.sql
--
-- Generic singular test: every open opportunity in fct_sales_pipeline
-- must have a close date.  A null close date breaks fiscal quarter bucketing
-- and should be caught before it reaches the mart layer.

select
    opportunity_id,
    opportunity_name,
    amount,
    close_date_id
from {{ ref('fct_sales_pipeline') }}
where close_date_id is null
  and amount > 0
