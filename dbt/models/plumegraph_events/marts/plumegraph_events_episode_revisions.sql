with lineage as (
    select
        to_episode_revision_id,
        to_json(
            list(
                struct_pack(
                    from_episode_revision_id := from_episode_revision_id,
                    relation_type := relation_type,
                    temporal_overlap := temporal_overlap,
                    mean_geometry_iou := mean_geometry_iou
                )
                order by relation_type, from_episode_revision_id
            )
        ) as lineage_json
    from {{ source('plumegraph_events_raw', 'episode_lineage') }}
    group by to_episode_revision_id
),

tracking as (
    select
        episode_revision_id,
        to_json(
            list(
                struct_pack(
                    tracking_edge_id := tracking_edge_id,
                    from_component_id := from_component_id,
                    to_component_id := to_component_id,
                    from_time := from_time,
                    to_time := to_time,
                    gap_hours := gap_hours,
                    geometry_iou := geometry_iou,
                    advection_residual_km := advection_residual_km,
                    concentration_ratio := concentration_ratio
                )
                order by from_time, to_time, tracking_edge_id
            )
        ) as tracking_edges_json
    from {{ source('plumegraph_events_raw', 'episode_tracking_edges') }}
    group by episode_revision_id
)

select
    episodes.*,
    runs.analysis_region_id,
    runs.partition_date,
    runs.input_manifest_sha256,
    runs.status as analysis_run_status,
    coalesce(lineage.lineage_json, '[]') as lineage_json,
    coalesce(tracking.tracking_edges_json, '[]') as tracking_edges_json
from {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
inner join {{ source('plumegraph_events_ops', 'analysis_runs') }} as runs
    on episodes.analysis_run_id = runs.analysis_run_id
left join lineage
    on episodes.episode_revision_id = lineage.to_episode_revision_id
left join tracking
    on episodes.episode_revision_id = tracking.episode_revision_id
