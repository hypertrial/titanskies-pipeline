{% test non_negative_no2(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
