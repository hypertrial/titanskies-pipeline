with contract as (
    select *
    from {{ ref('riverpulse_events_contract') }}
    where contract_key = 'default'
),

provenance as (
    select
        observation_revision_id,
        count(*) as source_snapshot_count,
        to_json(list(snapshot_id order by snapshot_id)) as source_snapshot_ids,
        to_json(list(artifact_uri order by artifact_uri)) as source_artifact_uris
    from {{ ref('stg_riverpulse_events_snapshot_provenance') }}
    group by observation_revision_id
)

select
    observations.*,
    contract.contract_version,
    contract.field_contract_version,
    coalesce(
        observations.collection_name = contract.collection_name
        and observations.collection_version = contract.collection_version
        and observations.sword_version = contract.sword_version,
        false
    ) as has_matching_science_versions,
    coalesce(
        observations.reach_quality = contract.accepted_reach_quality,
        false
    ) as is_official_good_quality,
    coalesce(
        (observations.reach_quality_bits & 2) != 0,
        false
    ) as has_classification_quality_suspect,
    coalesce(
        (observations.reach_quality_bits & 4) != 0,
        false
    ) as has_geolocation_quality_suspect,
    coalesce(
        (observations.reach_quality_bits & 8) != 0,
        false
    ) as has_water_fraction_suspect,
    coalesce(
        (observations.reach_quality_bits & 128) != 0,
        false
    ) as has_bright_land,
    coalesce(
        (observations.reach_quality_bits & 1024) != 0,
        false
    ) as has_few_area_observations,
    coalesce(
        (observations.reach_quality_bits & 2048) != 0,
        false
    ) as has_few_wse_observations,
    coalesce(
        (observations.reach_quality_bits & 8192) != 0,
        false
    ) as has_far_range_suspect,
    coalesce(
        (observations.reach_quality_bits & 16384) != 0,
        false
    ) as has_near_range_suspect,
    coalesce(
        (observations.reach_quality_bits & 32768) != 0,
        false
    ) as has_partially_observed,
    coalesce(
        (observations.reach_quality_bits & 262144) != 0,
        false
    ) as has_classification_quality_degraded,
    coalesce(
        (observations.reach_quality_bits & 524288) != 0,
        false
    ) as has_geolocation_quality_degraded,
    coalesce(
        (observations.reach_quality_bits & 268435456) != 0,
        false
    ) as has_no_observations,
    coalesce(
        observations.collection_name = contract.collection_name
        and observations.collection_version = contract.collection_version
        and observations.sword_version = contract.sword_version
        and observations.reach_quality = contract.accepted_reach_quality
        and isfinite(observations.wse)
        and isfinite(observations.wse_u),
        false
    ) as is_wse_ready,
    coalesce(
        observations.collection_name = contract.collection_name
        and observations.collection_version = contract.collection_version
        and observations.sword_version = contract.sword_version
        and observations.reach_quality = contract.accepted_reach_quality
        and isfinite(observations.width)
        and isfinite(observations.width_u),
        false
    ) as is_width_ready,
    coalesce(
        observations.collection_name = contract.collection_name
        and observations.collection_version = contract.collection_version
        and observations.sword_version = contract.sword_version
        and observations.reach_quality = contract.accepted_reach_quality
        and isfinite(observations.slope)
        and isfinite(observations.slope_u),
        false
    ) as is_slope_ready,
    coalesce(
        observations.collection_name = contract.collection_name
        and observations.collection_version = contract.collection_version
        and observations.sword_version = contract.sword_version
        and observations.reach_quality = contract.accepted_reach_quality
        and isfinite(observations.wse)
        and isfinite(observations.wse_u)
        and isfinite(observations.width)
        and isfinite(observations.width_u)
        and isfinite(observations.slope)
        and isfinite(observations.slope_u),
        false
    ) as is_analysis_ready,
    coalesce(provenance.source_snapshot_count, 0) as source_snapshot_count,
    coalesce(provenance.source_snapshot_ids, '[]') as source_snapshot_ids,
    coalesce(provenance.source_artifact_uris, '[]') as source_artifact_uris
from {{ ref('stg_riverpulse_events_observation_revisions') }} as observations
cross join contract
left join provenance
    on observations.observation_revision_id = provenance.observation_revision_id
