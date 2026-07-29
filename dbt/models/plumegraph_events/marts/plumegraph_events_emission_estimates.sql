select estimates.*
from {{ source('plumegraph_events_raw', 'emission_estimate_revisions') }} as estimates
inner join {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
    on estimates.episode_revision_id = episodes.episode_revision_id
