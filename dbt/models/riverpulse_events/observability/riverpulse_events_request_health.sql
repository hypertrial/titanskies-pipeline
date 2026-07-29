select
    request_id,
    reach_id,
    collection_name,
    window_start,
    window_end,
    status,
    attempts,
    http_status,
    row_count,
    error_message,
    planned_at,
    started_at,
    finished_at,
    date_diff('second', started_at, finished_at) as request_duration_seconds,
    status = 'success' as is_success,
    status = 'failed' as needs_operator_action
from {{ ref('stg_riverpulse_events_source_requests') }}
