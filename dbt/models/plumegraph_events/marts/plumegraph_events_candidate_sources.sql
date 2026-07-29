select
    candidates.*,
    not candidates.is_cohort as is_alternative_source
from {{ source('plumegraph_events_raw', 'candidate_source_revisions') }} as candidates
inner join {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
    on candidates.episode_revision_id = episodes.episode_revision_id
