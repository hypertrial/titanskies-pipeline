{% test no_duplicate_grain(model, grain_columns) %}
select
    {{ grain_columns | join(', ') }},
    count(*) as duplicate_count
from {{ model }}
group by {{ grain_columns | join(', ') }}
having count(*) > 1
{% endtest %}
