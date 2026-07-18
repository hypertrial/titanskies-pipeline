{{ config(materialized='view') }}

with contract as (
    select *
    from {{ ref('tempo_no2_std_contract') }}
    where contract_key = 'default'
),

hourly as (
    select *
    from {{ ref('int_tempo_no2_std_region_hourly') }}
),

latest_hourly as (
    select * exclude (recency_rank)
    from (
        select
            *,
            row_number() over (
                partition by canonical_region_id
                order by observation_hour desc
            ) as recency_rank
        from hourly
    ) as ranked
    where recency_rank = 1
),

zero_valid_issues as (
    select
        hourly.canonical_region_id,
        hourly.observation_hour,
        hourly.valid_area_km2,
        hourly.total_area_km2,
        hourly.coverage_fraction,
        'zero_valid' as issue_type,
        'error' as severity,
        'No valid accepted-area observations' as message
    from hourly
    where hourly.valid_pixel_count = 0
),

coverage_issues as (
    select
        hourly.canonical_region_id,
        hourly.observation_hour,
        hourly.valid_area_km2,
        hourly.total_area_km2,
        hourly.coverage_fraction,
        'low_coverage' as issue_type,
        'warn' as severity,
        'Regional coverage below contract floor' as message
    from hourly
    cross join contract
    where
        hourly.valid_pixel_count > 0
        and hourly.coverage_fraction < contract.min_region_coverage
),

stale_issues as (
    select
        hourly.canonical_region_id,
        hourly.observation_hour,
        hourly.valid_area_km2,
        hourly.total_area_km2,
        hourly.coverage_fraction,
        'stale' as issue_type,
        case
            when
                date_diff('hour', hourly.observation_hour, current_timestamp)
                >= contract.stale_hours_error then 'error'
            when
                date_diff('hour', hourly.observation_hour, current_timestamp)
                >= contract.stale_hours_warn then 'warn'
            else 'info'
        end as severity,
        'Observation older than freshness contract' as message
    from latest_hourly as hourly
    cross join contract
    where
        date_diff('hour', hourly.observation_hour, current_timestamp)
        >= contract.stale_hours_warn
)

select * from zero_valid_issues
union all
select * from coverage_issues
union all
select * from stale_issues
