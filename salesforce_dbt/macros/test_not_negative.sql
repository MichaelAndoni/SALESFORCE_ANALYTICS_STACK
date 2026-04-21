-- macros/test_not_negative.sql
--
-- Generic dbt test that asserts a numeric column contains no negative values.
--
-- Usage in schema.yml:
--   columns:
--     - name: amount
--       tests:
--         - not_negative

{% test not_negative(model, column_name) %}

select
    {{ column_name }},
    count(*) as failure_count
from {{ model }}
where {{ column_name }} < 0
group by 1
having count(*) > 0

{% endtest %}
