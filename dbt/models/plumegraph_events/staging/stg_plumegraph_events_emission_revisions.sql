select *
from {{ source('plumegraph_events_raw', 'hourly_emission_revisions') }}
