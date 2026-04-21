-- tests/assert_closed_won_not_in_pipeline.sql
--
-- Booking IDs must never appear in the open pipeline fact table.
-- A deal that is closed-won is in fct_bookings; it must be absent from
-- fct_sales_pipeline (which filters to is_closed = false).

select
    b.opportunity_id
from {{ ref('fct_bookings') }}         b
join {{ ref('fct_sales_pipeline') }}   p
  on b.opportunity_id = p.opportunity_id
