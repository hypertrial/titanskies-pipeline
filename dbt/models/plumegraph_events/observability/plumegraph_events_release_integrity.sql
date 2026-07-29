select
    release_id,
    evidence_format,
    release_version,
    analysis_manifest_sha256,
    validation_run_id,
    manifest_sha256,
    episode_count,
    published_at
from {{ source('plumegraph_events_ops', 'release_manifests') }}
