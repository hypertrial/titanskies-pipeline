{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['canonical_region_id', 'observation_hour'],
    on_schema_change='fail'
) }}

with contract as (
    select
        contract_version,
        anomaly_baseline_days,
        anomaly_min_baseline_samples
    from {{ ref('tempo_no2_std_contract') }}
    where contract_key = 'default'
),

hourly as (
    select * from {{ ref('int_tempo_no2_std_region_hourly') }}
),

changed as (
    select hourly.*
    from hourly
    cross join contract
    {% if is_incremental() %}
        where
            hourly.revision > coalesce(
                (
                    select max(destination.source_revision)
                    from {{ this }} as destination
                ), 0
            )
            or contract.contract_version
            <> coalesce(
                (
                    select max(destination.contract_version)
                    from {{ this }} as destination
                ), ''
            )
    {% endif %}
),

affected as (
    select distinct current_hour.*
    from hourly as current_hour
    inner join changed
        on
            current_hour.canonical_region_id = changed.canonical_region_id
            and current_hour.observation_hour >= changed.observation_hour
    cross join contract
    where
        current_hour.observation_hour <= changed.observation_hour
        + (contract.anomaly_baseline_days * interval '1 day')
),

baseline as (
    select
        current_hour.canonical_region_id,
        current_hour.observation_hour,
        current_hour.timezone,
        current_hour.local_observation_hour,
        current_hour.no2_mean,
        current_hour.is_analysis_ready,
        current_hour.revision as source_revision,
        contract.contract_version,
        contract.anomaly_min_baseline_samples,
        count(prior_hour.no2_mean) as baseline_sample_count,
        median(prior_hour.no2_mean) as baseline_median,
        mad(prior_hour.no2_mean) as baseline_mad
    from affected as current_hour
    cross join contract
    left join hourly as prior_hour
        on
            current_hour.canonical_region_id = prior_hour.canonical_region_id
            and prior_hour.is_analysis_ready
            and prior_hour.observation_hour >= current_hour.observation_hour
            - (contract.anomaly_baseline_days * interval '1 day')
            and current_hour.observation_hour > prior_hour.observation_hour
            and extract(hour from prior_hour.local_observation_hour)
            = extract(hour from current_hour.local_observation_hour)
    group by all
)

select
    canonical_region_id,
    observation_hour,
    timezone,
    local_observation_hour,
    no2_mean,
    baseline_sample_count,
    baseline_median,
    baseline_mad,
    is_analysis_ready,
    source_revision,
    contract_version,
    no2_mean - baseline_median as no2_difference,
    case
        when not is_analysis_ready then null
        when baseline_sample_count < anomaly_min_baseline_samples then null
        when baseline_mad is null or baseline_mad = 0 then null
        else (no2_mean - baseline_median) / (1.4826 * baseline_mad)
    end as robust_z_score
from baseline
