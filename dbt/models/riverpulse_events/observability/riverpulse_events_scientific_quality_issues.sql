with observations as (
    select *
    from {{ ref('riverpulse_events_observations') }}
)

select
    observation_id,
    observation_revision_id,
    reach_id,
    observation_time,
    'science_version_mismatch' as issue_type,
    'collection, collection version, or SWORD version differs from the contract' as issue_detail
from observations
where not has_matching_science_versions

union all

select
    observation_id,
    observation_revision_id,
    reach_id,
    observation_time,
    'source_quality_not_good' as issue_type,
    'official reach quality classification is not good' as issue_detail
from observations
where not is_official_good_quality

union all

select
    observation_id,
    observation_revision_id,
    reach_id,
    observation_time,
    'wse_not_ready' as issue_type,
    'WSE or its uncertainty is unavailable or non-finite' as issue_detail
from observations
where not is_wse_ready

union all

select
    observation_id,
    observation_revision_id,
    reach_id,
    observation_time,
    'width_not_ready' as issue_type,
    'width or its uncertainty is unavailable or non-finite' as issue_detail
from observations
where not is_width_ready

union all

select
    observation_id,
    observation_revision_id,
    reach_id,
    observation_time,
    'slope_not_ready' as issue_type,
    'slope or its uncertainty is unavailable or non-finite' as issue_detail
from observations
where not is_slope_ready
