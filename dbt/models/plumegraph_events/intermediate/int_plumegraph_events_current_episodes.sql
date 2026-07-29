select
    episodes.*,
    runs.analysis_region_id,
    runs.partition_date,
    runs.input_manifest_sha256,
    generations.promoted_at
from {{ source('plumegraph_events_raw', 'episode_revisions') }} as episodes
inner join {{ source('plumegraph_events_ops', 'analysis_runs') }} as runs
    on episodes.analysis_run_id = runs.analysis_run_id
inner join {{ source('plumegraph_events_ops', 'current_generations') }} as generations
    on
        runs.analysis_run_id = generations.analysis_run_id
        and runs.analysis_region_id = generations.analysis_region_id
        and runs.partition_date = generations.partition_date
where runs.status = 'success'
qualify row_number() over (
    partition by episodes.plume_id
    order by
        runs.partition_date desc,
        episodes.created_at desc,
        episodes.episode_revision_id desc
) = 1
