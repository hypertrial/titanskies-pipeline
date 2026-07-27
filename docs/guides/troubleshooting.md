# Troubleshooting

Symptom → diagnostic → fix for common local-operator failures. Prefer one
writer against the configured DuckDB path. See
[Validate and recover](validate-and-recover.md) for broader recovery steps.

## DuckDB lock / one writer

**Symptom.** Dagster, `duckdb` CLI, or a Make target fails with a lock /
database-is-locked style error, or a second writer appears to hang.

**Diagnostic.** List processes holding the warehouse (`titanskies.duckdb` or
your `DUCKDB_PATH`). Confirm only one intended writer is active. Read-only
inspection is fine; concurrent writers are not.

**Fix.** Stop the competing Dagster run, UI write session, or script. Retry
after the first writer exits. Do not delete WAL files while a process still
owns the database. Prefer one writer at a time for day-two operations.

## Empty or stale marts (NRT and std)

**Symptom.** Public marts are empty, hours behind discovery, or `data_age_hours`
looks large while you expected fresh rows.

**Diagnostic.**

1. Confirm the scope you expect: `make demo` builds **NRT only**; standard
   marts appear only after an explicit `tempo:no2_std` discovery/ingest and
   dbt run.
2. Inspect granule status for that scope:

```sql
select granule_id, processing_status, acquisition_start, processed_at,
       error_message
from tempo_no2_observability.tempo_no2_granule_observability
order by acquisition_start desc
limit 25;
```

For standard, use `tempo_no2_std_observability.tempo_no2_std_granule_observability`.

3. Check advisory freshness issues:

```sql
select canonical_region_id, observation_hour, issue_type, severity
from tempo_no2_observability.tempo_no2_data_quality
where issue_type = 'stale'
order by observation_hour desc;
```

**Fix.** If granules never reached `processed`, repair ingest (auth, NetCDF,
geography) and rerun the scope hourly/full job. If raw is current but marts
are not, run that scope's dbt build (`tempo_no2_dbt_build` /
`tempo_no2_std_dbt_build`). Remember: freshness is observability-only;
`is_analysis_ready` does not encode stale age, and `*_region_latest` already
filters analysis-ready non-country rows without exposing that flag.

## Earthdata / CMR auth and discovery

**Symptom.** Discovery returns zero granules, 401/403-class failures, or
earthaccess cannot authenticate.

**Diagnostic.** Confirm NASA Earthdata Login via `~/.netrc` or
`EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` in `.env` (lowercase aliases are
also accepted). Verify the CMR concept ID and lookback for the scope you are
running (`TEMPO_NO2_CMR_CONCEPT_ID` vs `TEMPO_NO2_STD_CMR_CONCEPT_ID`).

**Fix.** Correct credentials, then run discovery manually before enabling a
schedule:

```bash
uv run python scripts/run_live_smoke.py --mode discovery
```

Use this to separate authentication failures from product or CMR schema
drift. Downloaded data and disposable warehouses stay on the operator machine;
do not commit them.

## Failed granules and retry

**Symptom.** The hourly job fails after attempting granules; marts miss recent
hours.

**Diagnostic.** Inspect the durable inventory error for the scope:

```sql
select granule_id, download_status, validation_status, processing_status,
       error_message, updated_at
from tempo_no2_ops.granule_inventory
where processing_status = 'failed'
order by updated_at desc;
```

Use `tempo_no2_std_ops.granule_inventory` for the standard scope.

**Fix.** Correct authentication, network, NetCDF layout, or geography issues,
then rerun the hourly (or full) job for that scope. Failed rows are
automatically re-selected, downloaded from scratch, and have `error_message`
cleared after success. The batch records every attempted granule before
failing, so fix the shared cause once rather than cherry-picking IDs.

## Revised granule still shows old NO₂

**Symptom.** Inventory `cmr_revision_at` is newer than `processed_at`, or you
rediscovered a settling standard window, but marts still show the first
processed values for that hour.

**Diagnostic.** Compare revision and processing timestamps for the scope:

```sql
select granule_id, cmr_revision_at, processed_at, processing_status,
       checksum_sha256
from tempo_no2_ops.granule_inventory
where cmr_revision_at is not null
  and processed_at is not null
  and cmr_revision_at > processed_at
order by cmr_revision_at desc
limit 25;
```

Use `tempo_no2_std_ops.granule_inventory` for the standard scope.

**Fix.** Rerun that scope's discovery then hourly/full ingest. Discovery
requeues processed granules when CMR `revision-date` advances, deletes any
stale local NetCDF under the scope raw directory, and clears checksum/path
fields; ingest then downloads again and replaces the region-hour row.
Download-URL-only refreshes do not requeue.

## Geography checksum / registry

**Symptom.** Ingest or registry materialization fails on checksum,
geometry version, or missing production artifacts.

**Diagnostic.** Confirm `TEMPO_GEOGRAPHY_MANIFEST_PATH` points at a complete
generation. Live runs require production artifacts (`uv sync` with the `geo`
extra), not only synthetic demo fixtures. Compare
`geometry_version` / `geometry_checksum` in
`tempo_no2_marts.tempo_region_registry` (or
`tempo_no2_std_marts.tempo_no2_std_region_registry`) with the manifest
generation you intended to load.

**Fix.** Rebuild a complete generation and atomically replace the manifest:

```bash
python scripts/build_region_artifacts.py --output-dir artifacts/geo
```

Then materialize `tempo/no2/ops/region_registry` (and
`tempo/no2_std/ops/region_registry` if operating standard). Do not edit
generated Parquet files or mix files from different generations. A changed
production geometry version requires a clean warehouse because region
identities and overlap weights are one contract.

## Wrong scope / demo vs std emptiness

**Symptom.** Standard marts or observability relations are missing or empty
after `make demo`, or queries against `tempo_no2_std_*` fail with catalog
errors.

**Diagnostic.** `make demo` is NRT-only: it seeds both contract CSVs but runs
dbt with `--select tag:tempo,tag:no2`. Standard-scope raw/ops schemas
bootstrap empty; std marts are not produced by the demo path.

**Fix.** Run an explicit standard discovery/ingest and dbt path when you need
std marts. Keep NRT and std queries on their own schemas and registry FQNs
(`tempo_region_registry` vs `tempo_no2_std_region_registry`). Do not expect
demo SQL that targets std relations to populate without that work.

## Corrupt or pre-0.4 warehouse

**Symptom.** Startup refuses the warehouse, schema version errors mention a
0.4 rebuild, or derived tables look partially upgraded.

**Diagnostic.** Confirm whether the DuckDB file predates schema `0.4` or is
corrupt (failed compact, killed writer, mixed generations). Populated
pre-0.4 warehouses intentionally fail at startup.

**Fix.** Stop Dagster and any DuckDB clients. Preserve the raw download
directories and reviewed geography source cache. Move the corrupt/old
database and its WAL/SHM files out of the working path. Point `DUCKDB_PATH`
at a new file, initialize a clean warehouse, register geography, and rerun
ingestion/dbt per scope. Follow the
[v0.3 to v0.4 upgrade guide](../getting-started/upgrade-v04.md) (earlier
rebuild notes: [v0.3](../getting-started/upgrade-v03.md),
[v0.2](../getting-started/upgrade-v02.md)). Never copy partially derived
tables into the replacement.

## Raw storage does not shrink

**Symptom.** NetCDF disk use stays high after retention settings look correct.

**Diagnostic.** Retention considers `processed_at`, not observation time.
Confirm the row is processed, older than the scope retention variable
(`TEMPO_NO2_RAW_RETENTION_DAYS` or `TEMPO_NO2_STD_RAW_RETENTION_DAYS`), and
its `local_path` is inside that scope's raw data directory.

**Fix.** Correct retention env vars and rerun ingest so prune runs. Unsafe
out-of-root paths intentionally fail the job rather than being deleted. The
DuckDB ledger is retained even when a processed NetCDF file is pruned.

## Live source drift

**Symptom.** Discovery or parsers break after an upstream CMR/product change.

**Diagnostic.** Run
`uv run python scripts/run_live_smoke.py --mode discovery` locally and inspect
sanitized schema diagnostics.

**Fix.** Update parser contracts and fixtures in one reviewed change. Use
`make live-smoke` only with operator-owned Earthdata credentials. Do not
commit downloaded payloads while diagnosing drift.
