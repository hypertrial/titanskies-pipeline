# Enable the schedule

All scope schedules ship **stopped by default**. Opt in only after a
validated manual full-pipeline run for that scope. Set the matching flag in
`.env`:

```dotenv
# Opt in per scope after a validated manual run (both default false).
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=true
# TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=true
# RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED=true
# PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED=true
```

Restart Dagster and enable the schedule(s) you opted into:

- `tempo_no2_hourly_pipeline_schedule` → NRT full pipeline
- `tempo_no2_std_pipeline_schedule` → standard full pipeline
- `riverpulse_events_pipeline_schedule` → Sunday 03:00 UTC RiverPulse full pipeline
- `plumegraph_events_daily_pipeline_schedule` → daily 06:00 UTC PlumeGraph full pipeline

Each targets one discovery, pending ingestion, and incremental dbt
publication for its scope. Register production geography or the production
SWORD network once before enabling a schedule. RiverPulse network bootstrap
is deliberately excluded from the weekly selection; enable it only after the
production build, one-reach smoke, science-phase backfill, idempotent rerun,
and observability review pass.

PlumeGraph cohort bootstrap and immutable release publication are deliberately
excluded from its daily selection. Enable it only after cohort/contract
approval, one-facility smoke, the complete 2024 backfill, an idempotent rerun,
held-out validation, and observability review.
