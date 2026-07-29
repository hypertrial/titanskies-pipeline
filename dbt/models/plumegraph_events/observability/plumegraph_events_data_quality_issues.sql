select
    pixels.pixel_revision_id as issue_identity,
    'pixel_not_analysis_ready' as issue_type,
    case
        when pixels.quality_flag != contract.quality_flag_good then 'quality_flag'
        when pixels.cloud_fraction >= contract.max_cloud_fraction
            then 'cloud_fraction'
        when pixels.no2_vertical_column is null then 'missing_vcd'
        when pixels.no2_uncertainty is null or pixels.no2_uncertainty <= 0
            then 'invalid_uncertainty'
        when pixels.geometry_wkb is null then 'invalid_geometry'
        when pixels.collection_version != contract.collection_version
            then 'collection_version'
        when pixels.source_snapshot_id is null then 'missing_provenance'
        else 'unknown'
    end as issue_reason
from {{ ref('int_plumegraph_events_current_pixels') }} as pixels
cross join {{ ref('plumegraph_events_contract') }} as contract
where
    contract.contract_key = 'default'
    and (
        pixels.quality_flag != contract.quality_flag_good
        or pixels.cloud_fraction >= contract.max_cloud_fraction
        or pixels.no2_vertical_column is null
        or pixels.no2_uncertainty is null
        or pixels.no2_uncertainty <= 0
        or pixels.geometry_wkb is null
        or pixels.collection_version != contract.collection_version
        or pixels.source_snapshot_id is null
    )
