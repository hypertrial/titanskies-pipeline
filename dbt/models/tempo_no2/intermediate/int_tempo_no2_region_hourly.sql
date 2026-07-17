{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['canonical_region_id', 'observation_hour'],
    on_schema_change='fail'
) }}

with contract as (
    select *
    from {{ ref('tempo_no2_contract') }}
    where contract_key = 'default'
),

hourly as (
    select *
    from {{ ref('stg_tempo_no2_region_hour_aggregates') }}
    {% if is_incremental() %}
        where
            revision > coalesce(
                (select max(destination.revision) from {{ this }} as destination), 0
            )
            or (select contract.contract_version from contract)
            <> coalesce(
                (
                    select max(destination.contract_version)
                    from {{ this }} as destination
                ), ''
            )
    {% endif %}
),

registry as (
    select
        canonical_region_id,
        timezone
    from {{ ref('stg_tempo_no2_region_registry') }}
)

select
    hourly.canonical_region_id,
    hourly.country_code,
    hourly.region_type,
    hourly.observation_hour,
    registry.timezone,
    hourly.no2_mean,
    hourly.no2_median,
    hourly.no2_p90,
    hourly.valid_pixel_count,
    hourly.total_pixel_count,
    hourly.valid_area_km2,
    hourly.total_area_km2,
    hourly.coverage_fraction,
    hourly.quality_flag_accepted,
    hourly.source_granule_count,
    hourly.all_granules_validated,
    hourly.revision,
    hourly.geometry_version,
    hourly.ingested_at,
    contract.contract_version,
    timezone(
        registry.timezone,
        timezone('UTC', hourly.observation_hour)
    ) as local_observation_hour,
    (
        hourly.quality_flag_accepted
        and hourly.all_granules_validated
        and hourly.coverage_fraction >= contract.min_region_coverage
    ) as is_analysis_ready
from hourly
cross join contract
inner join registry
    on hourly.canonical_region_id = registry.canonical_region_id
