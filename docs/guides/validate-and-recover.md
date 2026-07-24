# Validate and recover

## Validation checklist

1. Query `tempo_no2_observability.tempo_no2_granule_observability` for recent
   processing status.
2. Confirm analysis-ready rows exist in the mart you care about.
3. For live operators, run
   `uv run python scripts/run_live_smoke.py --mode discovery` before enabling
   schedules.
4. Use `make live-smoke` only with operator-owned Earthdata credentials.

## Recovery

1. Stop Dagster and any DuckDB clients before warehouse recovery.
2. Preserve the raw download directory and reviewed geography source cache.
3. Move a corrupt database and its WAL files out of the working path.
4. Initialize a clean warehouse and rerun ingestion/dbt. Never copy partially
   derived tables into the replacement.
5. Populated older major derived warehouses intentionally fail at startup;
   follow the matching upgrade guide under Get started.

Failed granules are recorded in `tempo_no2_ops.granule_inventory`. Correct
authentication, network, NetCDF, or geography issues and rerun the hourly job;
failed rows are automatically reselected and redownloaded.

See [Troubleshooting](troubleshooting.md) for common failure modes.
