select
    validation_run_id,
    benchmark_version,
    expected_calibration_error,
    probability_enabled,
    passed,
    completed_at
from {{ source('plumegraph_events_ops', 'validation_runs') }}
qualify row_number() over (
    order by completed_at desc, validation_run_id desc
) = 1
