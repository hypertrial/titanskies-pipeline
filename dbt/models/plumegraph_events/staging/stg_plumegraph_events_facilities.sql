select *
from {{ source('plumegraph_events_raw', 'facilities') }}
