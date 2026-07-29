select
    links.episode_revision_id,
    links.source_type,
    links.source_snapshot_id,
    links.input_identity,
    snapshots.request_id,
    snapshots.source_identity,
    snapshots.source_revision_at,
    snapshots.artifact_uri,
    snapshots.content_sha256,
    snapshots.source_etag,
    snapshots.source_lineage_json,
    snapshots.schema_fingerprint,
    snapshots.collected_at,
    artifacts.normalized_artifact_id,
    artifacts.artifact_uri as normalized_artifact_uri,
    artifacts.content_sha256 as normalized_content_sha256,
    artifacts.schema_fingerprint as normalized_schema_fingerprint,
    artifacts.row_count as normalized_row_count,
    requests.source_version,
    requests.request_contract_version,
    runs.contract_version,
    runs.algorithm_version,
    runs.input_manifest_sha256,
    runs.analysis_run_id
from {{ source('plumegraph_events_raw', 'provenance_links') }} as links
inner join {{ ref('stg_plumegraph_events_source_snapshots') }} as snapshots
    on links.source_snapshot_id = snapshots.snapshot_id
inner join {{ ref('stg_plumegraph_events_source_requests') }} as requests
    on snapshots.request_id = requests.request_id
left join {{ source('plumegraph_events_ops', 'normalized_artifacts') }} as artifacts
    on snapshots.snapshot_id = artifacts.source_snapshot_id
inner join {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
    on links.episode_revision_id = episodes.episode_revision_id
inner join {{ source('plumegraph_events_ops', 'analysis_runs') }} as runs
    on
        episodes.analysis_run_id = runs.analysis_run_id
        and runs.status = 'success'
