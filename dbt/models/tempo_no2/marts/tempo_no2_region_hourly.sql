select *
from {{ ref('int_tempo_no2_region_hourly') }}
where region_type != 'country'
