with hourly as (
    select * from {{ ref('int_tempo_no2_std_region_hourly') }}
)

select
    country_code,
    observation_hour,
    max(no2_mean) filter (where region_type = 'country') as no2_mean,
    max(no2_median) filter (where region_type = 'country') as no2_median,
    max(no2_p90) filter (where region_type = 'country') as no2_p90,
    max(coverage_fraction) filter (where region_type = 'country') as coverage_fraction,
    max(valid_pixel_count) filter (where region_type = 'country') as valid_pixel_count,
    max(total_pixel_count) filter (where region_type = 'country') as total_pixel_count,
    max(valid_area_km2) filter (where region_type = 'country') as valid_area_km2,
    max(total_area_km2) filter (where region_type = 'country') as total_area_km2,
    max(quality_flag_accepted) filter (
        where region_type = 'country'
    ) as quality_flag_accepted,
    max(source_granule_count) filter (
        where region_type = 'country'
    ) as source_granule_count,
    max(all_granules_validated) filter (
        where region_type = 'country'
    ) as all_granules_validated,
    max(is_analysis_ready) filter (
        where region_type = 'country'
    ) as is_analysis_ready,
    count(*) filter (
        where
        region_type in ('county', 'census_subdivision', 'municipality')
        and is_analysis_ready
    ) as analysis_ready_region_count,
    count(*) filter (
        where region_type in ('county', 'census_subdivision', 'municipality')
    ) as region_count
from hourly
group by 1, 2
having count(*) filter (where region_type = 'country') = 1
