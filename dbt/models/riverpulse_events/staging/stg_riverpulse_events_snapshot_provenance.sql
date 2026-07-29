select
    links.observation_revision_id,
    snapshots.snapshot_id,
    snapshots.request_id,
    snapshots.response_sha256,
    snapshots.artifact_uri,
    snapshots.http_status,
    snapshots.row_count as snapshot_row_count,
    snapshots.collected_at as snapshot_collected_at
from {{ source('riverpulse_events_raw', 'observation_snapshot_links') }} as links
inner join {{ source('riverpulse_events_ops', 'source_snapshots') }} as snapshots
    on links.snapshot_id = snapshots.snapshot_id
