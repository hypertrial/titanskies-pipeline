select *
from {{ source('plumegraph_events_ops', 'validation_runs') }}
