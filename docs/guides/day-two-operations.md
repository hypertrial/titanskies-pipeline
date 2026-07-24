# Day-two operations

Use this guide after a successful demo or first live materialization for the
scope you intend to operate. Schedules stay disabled until discovery, hourly
ingest, and dbt look healthy for that scope.

## Daily cadence

1. Keep both schedule flags off until you intentionally enable a scope:

   ```dotenv
   TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=false
   TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=false
   ```

2. Prefer **one writer** at a time against the configured DuckDB path
   (`DUCKDB_PATH` / `DUCKDB_NAME`). Stop competing Dagster runs, compact
   jobs, and writeable DuckDB sessions before starting another writer.

3. After each successful run, inspect observability for **that scope**:
   - NRT: `tempo_no2_observability.tempo_no2_granule_observability` and
     `tempo_no2_data_quality`
   - Standard: `tempo_no2_std_observability.tempo_no2_std_granule_observability`
     and `tempo_no2_std_data_quality`

4. Treat freshness (`stale` rows) as an operator signal. It does not change
   `is_analysis_ready`, and `*_region_latest` already filters analysis-ready
   non-country regions without exposing that flag.

## Retention (per scope)

Prune processed NetCDF files with the matching retention variable; the DuckDB
ledger remains durable:

| Scope | Raw directory | Retention |
| --- | --- | --- |
| NRT (`tempo:no2`) | `TEMPO_NO2_RAW_DATA_DIR` | `TEMPO_NO2_RAW_RETENTION_DAYS` (default `30`) |
| Standard (`tempo:no2_std`) | `TEMPO_NO2_STD_RAW_DATA_DIR` | `TEMPO_NO2_STD_RAW_RETENTION_DAYS` (default `30`) |

Retention keys off `processed_at`. Paths outside the configured raw root are
rejected rather than deleted.

## Geography rebuild policy

Rebuild production geography only when the pinned geometry generation,
checksum, or source pins change. Use
`scripts/build_region_artifacts.py`, atomically replace
`TEMPO_GEOGRAPHY_MANIFEST_PATH`, then rematerialize
`tempo/no2/ops/region_registry` and (if operating standard)
`tempo/no2_std/ops/region_registry`. Do not hand-edit Parquet artifacts or
mix files from different generations. A geometry-version change is a clean
warehouse boundary.

## When to enable each schedule

Enable a schedule only after a validated **manual** full-pipeline run for
that scope:

| Flag | Schedule | Enable when |
| --- | --- | --- |
| `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED` | `tempo_no2_hourly_pipeline_schedule` | NRT discovery → ingest → dbt succeeded manually and observability looks healthy |
| `TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED` | `tempo_no2_std_pipeline_schedule` | Standard discovery → ingest → dbt succeeded manually; opt in explicitly (ships disabled) |

Restart Dagster after changing flags. See
[Enable the schedule](enable-schedule.md) and
[Live readiness](live-readiness.md).

## Weekly live-smoke note

On an operator-owned machine with Earthdata credentials, run
`uv run make live-smoke` at least weekly when you operate live ingestion, and
always before tagging a release. Live CMR/NetCDF validation stays local;
GitHub Actions does not run live smoke. Retain only sanitized diagnostics
when sharing failures.

## Related guides

- [Backfill 30 days](backfill-30-days.md)
- [Validate and recover](validate-and-recover.md)
- [Troubleshooting](troubleshooting.md)
- [Configuration](../reference/configuration.md)
