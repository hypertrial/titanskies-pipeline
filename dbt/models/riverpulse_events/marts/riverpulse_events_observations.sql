select revisions.*
from {{ ref('riverpulse_events_observation_revisions') }} as revisions
inner join {{ ref('int_riverpulse_events_current_observations') }} as current_revisions
    on
        revisions.observation_revision_id
        = current_revisions.observation_revision_id
