select
    run_id,
    job_name,
    step,
    status,
    started_at,
    finished_at,
    rows_written,
    message
from {{ source('tempo_no2_std_ops', 'pipeline_run_events') }}
