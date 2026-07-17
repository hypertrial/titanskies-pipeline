select
    canonical_region_id,
    observation_hour,
    timezone,
    local_observation_hour,
    no2_mean,
    baseline_sample_count,
    baseline_median,
    baseline_mad,
    no2_difference,
    robust_z_score,
    is_analysis_ready
from {{ ref('int_tempo_no2_region_anomalies') }}
where canonical_region_id not in ('US', 'CA', 'MX')
