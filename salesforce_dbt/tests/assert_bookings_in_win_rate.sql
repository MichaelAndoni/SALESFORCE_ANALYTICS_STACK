-- tests/assert_bookings_in_win_rate.sql
--
-- Every won booking in fct_bookings must have a corresponding row
-- in fct_win_rate.  If a won opportunity is missing from win_rate
-- it means the is_closed filter in fct_win_rate failed.

select
    b.opportunity_id
from {{ ref('fct_bookings') }}   b
left join {{ ref('fct_win_rate') }} w
  on b.opportunity_id = w.opportunity_id
where w.opportunity_id is null
