# Day-two operations

Use this guide after a successful demo or first live materialization.

1. Keep schedules disabled until discovery and hourly jobs succeed manually.
2. Prefer one writer at a time against the configured DuckDB path.
3. Inspect `tempo_no2_observability` (and `tempo_no2_std_observability` when
   used) after each successful run.
4. Run geography rebuilds only when the pinned geometry generation changes.
5. Prune processed NetCDF files according to `TEMPO_NO2_RAW_RETENTION_DAYS`
   (and `TEMPO_NO2_STD_RAW_RETENTION_DAYS` for the standard scope); the DuckDB
   ledger remains durable.

See [Enable the schedule](enable-schedule.md),
[Live readiness](live-readiness.md), and
[Backfill 30 days](backfill-30-days.md).
