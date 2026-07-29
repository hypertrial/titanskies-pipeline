select
    runs.analysis_region_id,
    runs.partition_date,
    runs.analysis_run_id,
    runs.status,
    runs.episode_count,
    runs.error_message,
    generations.analysis_run_id = runs.analysis_run_id as is_current
from {{ source('plumegraph_events_ops', 'analysis_runs') }} as runs
left join {{ source('plumegraph_events_ops', 'current_generations') }} as generations
    on
        runs.analysis_region_id = generations.analysis_region_id
        and runs.partition_date = generations.partition_date
