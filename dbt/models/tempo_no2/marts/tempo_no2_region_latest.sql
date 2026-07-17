with ranked as (
    select
        *,
        row_number() over (
            partition by canonical_region_id
            order by observation_hour desc
        ) as rn
    from {{ ref('int_tempo_no2_region_hourly') }}
    where is_analysis_ready and region_type != 'country'
)

select
    canonical_region_id,
    country_code,
    region_type,
    observation_hour as latest_observation_hour,
    no2_mean as latest_no2_mean,
    valid_area_km2 as latest_valid_area_km2,
    total_area_km2 as latest_total_area_km2,
    coverage_fraction as latest_coverage_fraction,
    date_diff('hour', observation_hour, current_timestamp) as data_age_hours
from ranked
where rn = 1
