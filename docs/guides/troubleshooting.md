# Troubleshooting

## Missing geography artifacts

Live runs require production artifacts: install the `geo` extra, run
`python scripts/build_region_artifacts.py --output-dir artifacts/geo`, then
materialize `tempo/no2/ops/region_registry`. Synthetic artifacts are accepted
only by demo and test paths.

If checksums or the configured geometry version changed, rebuild a complete
generation and atomically replace the manifest. Do not edit generated Parquet
files or mix files from different generations.

## Earthdata authentication or stale discovery

Configure NASA Earthdata Login through `~/.netrc` or
`EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` in `.env`. Lowercase aliases are
accepted for backward compatibility. Confirm the CMR concept ID and discovery
lookback, then run the discovery job manually before enabling a schedule.

Run `uv run python scripts/run_live_smoke.py --mode discovery` locally to
distinguish authentication failures from product or CMR schema drift.
Downloaded data and the disposable warehouse remain on the operator machine.

## Empty or stale marts

Check `tempo_no2_observability.tempo_no2_granule_observability`, then confirm
recent granules reached `processed`. Run `tempo_no2_dbt_build` after ingestion
if raw state is current but marts are not. `tempo_no2_data_quality` explains
zero-valid, low-coverage, and stale observations without hiding their rows.

## Failed granules

The run records every attempted granule before failing the batch. Inspect the
durable error:

```sql
select granule_id, download_status, validation_status, processing_status,
       error_message, updated_at
from tempo_no2_ops.granule_inventory
where processing_status = 'failed'
order by updated_at desc;
```

Correct authentication, network, NetCDF, or geography issues and run the
hourly job again. Failed rows are automatically selected, downloaded from
scratch, and have `error_message` cleared after success.

## Raw storage does not shrink

Retention considers `processed_at`, not observation time. Confirm the row is
processed, older than `TEMPO_NO2_RAW_RETENTION_DAYS`, and its `local_path` is
inside `TEMPO_NO2_RAW_DATA_DIR`. Unsafe out-of-root paths intentionally fail
the job rather than being deleted. The DuckDB ledger is retained even when a
processed NetCDF file is pruned.

## Corrupt or incompatible warehouse

Stop Dagster and any DuckDB clients before recovery. Preserve the raw download
directory and reviewed geography source cache, move the corrupt database and
its WAL files out of the working path, then initialize a clean warehouse and
rerun ingestion/dbt. Never copy partially derived tables into the replacement.

Populated v0.1/v0.2 warehouses intentionally fail at startup. Follow the v0.3
upgrade guide. A changed production geometry version also requires a clean
warehouse because region identities and overlap weights are one contract.

## Live source drift

Run `uv run python scripts/run_live_smoke.py --mode discovery` locally first,
inspect its sanitized schema diagnostics, and update parser contracts and
fixtures in one reviewed change. Use `make live-smoke` only with operator-owned
Earthdata credentials. Do not commit the downloaded payload while diagnosing
drift.
