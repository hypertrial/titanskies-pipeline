select *
from {{ source('riverpulse_events_raw', 'discharge_revisions') }}
