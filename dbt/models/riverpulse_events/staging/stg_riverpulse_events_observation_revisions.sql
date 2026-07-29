select *
from {{ source('riverpulse_events_raw', 'observation_revisions') }}
