select *
from {{ source('plumegraph_events_raw', 'retrieval_pixel_revisions') }}
