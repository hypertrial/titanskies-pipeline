select
    pixels.*,
    links.episode_revision_id,
    links.evidence_role,
    links.filter_reason,
    links.enhancement_molecules_cm2
from {{ source('plumegraph_events_raw', 'episode_pixel_links') }} as links
inner join {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
    on links.episode_revision_id = episodes.episode_revision_id
inner join {{ ref('stg_plumegraph_events_pixel_revisions') }} as pixels
    on links.pixel_revision_id = pixels.pixel_revision_id
