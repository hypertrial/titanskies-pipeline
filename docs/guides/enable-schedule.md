# Enable the schedule

Set in `.env`:

```dotenv
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=true
```

Restart Dagster and turn on `tempo_no2_hourly_pipeline_schedule`. It targets
the full pipeline, including one discovery, pending ingestion, and incremental
dbt publication. Register production geography once before enabling it.
