select
    connector,
    status,
    count(*) as request_count,
    sum(attempts) as total_attempts,
    min(window_start) as earliest_window_start,
    max(window_end) as latest_window_end,
    max(updated_at) as last_updated_at
from {{ ref('stg_plumegraph_events_source_requests') }}
group by connector, status
