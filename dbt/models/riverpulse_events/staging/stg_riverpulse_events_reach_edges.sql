select *
from {{ source('riverpulse_events_raw', 'reach_edges') }}
