select geometries.*
from {{ source('plumegraph_events_raw', 'episode_geometries') }} as geometries
inner join {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
    on geometries.episode_revision_id = episodes.episode_revision_id
