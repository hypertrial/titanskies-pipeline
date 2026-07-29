select
    facilities.*,
    not facilities.is_cohort as is_alternative_source
from {{ ref('stg_plumegraph_events_facilities') }} as facilities
