select
    connector,
    source_identity,
    count(*) as snapshot_revision_count,
    min(collected_at) as first_collected_at,
    max(collected_at) as last_collected_at,
    count(distinct schema_fingerprint) as schema_fingerprint_count
from {{ ref('stg_plumegraph_events_source_snapshots') }}
group by connector, source_identity
