# Enable the schedule

Both scope schedules ship **stopped by default**. Opt in only after a
validated manual full-pipeline run for that scope. Set the matching flag in
`.env`:

```dotenv
# Opt in per scope after a validated manual run (both default false).
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=true
# TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=true
```

Restart Dagster and enable the schedule(s) you opted into:

- `tempo_no2_hourly_pipeline_schedule` → NRT full pipeline
- `tempo_no2_std_pipeline_schedule` → standard full pipeline

Each targets one discovery, pending ingestion, and incremental dbt
publication for its scope. Register production geography for that scope once
before enabling its schedule.
