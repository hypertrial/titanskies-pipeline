# Upgrade from v0.3 to v0.4

v0.4 adds a second, independent TEMPO NO2 scope, `tempo:no2_std` (standard,
V04), alongside the existing `tempo:no2` (NRT) scope. It also bumps the
warehouse schema version to `0.4`, which is a rebuild boundary: it does not
migrate a populated v0.3 (or earlier) DuckDB file in place.

1. Stop `tempo_no2_hourly_pipeline_schedule` and any operator backfills.
2. Back up the old DuckDB file.
3. Upgrade the checkout and dependencies (`uv sync --locked --extra dev --extra geo`).
4. Point `DUCKDB_PATH` at a new file and run `init_duck_db()` (or any pipeline
   entry point that calls it). This bootstraps raw/ops schemas, tables, and
   sequences for **both** scopes: `tempo_no2_raw`/`tempo_no2_ops` and
   `tempo_no2_std_raw`/`tempo_no2_std_ops`. dbt mart and observability schemas
   appear later when you run dbt. Attempting to reuse a populated pre-0.4
   warehouse raises an error telling you a schema 0.4 rebuild is required.
5. Materialize `tempo/no2/ops/region_registry` (and, if you plan to run the
   standard scope, `tempo/no2_std/ops/region_registry`) against pinned
   production geography.
6. Run the desired discovery lookback/backfill for each scope you intend to
   operate. NRT and standard scopes are fully independent: separate raw
   directories (`TEMPO_NO2_RAW_DATA_DIR` / `TEMPO_NO2_STD_RAW_DATA_DIR`),
   separate CMR concept IDs, separate contracts
   (`dbt/seeds/tempo_no2_contract.csv` / `dbt/seeds/tempo_no2_std_contract.csv`),
   and separate `hour_revision` sequences.
7. Run `tempo_no2_full_pipeline` (and `tempo_no2_std_full_pipeline` if
   adopting the standard scope) and validate administrative, country,
   anomaly, native-grid, granule-observability, and data-quality relations
   for each scope you run.
8. Re-enable `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=true` and restart
   Dagster to resume the NRT schedule. The new standard schedule,
   `tempo_no2_std_pipeline_schedule`, ships **disabled by default**
   (`TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=false`); opt in explicitly once
   you have validated standard-scope backfills.

## What did not change

- `make demo` remains NRT-only. It seeds both contract CSVs but runs dbt with
  `--select tag:tempo,tag:no2`, so only the NRT marts are built. Standard-scope
  raw/ops schemas are bootstrapped empty and are not seeded by the demo.
- NRT collection, concept ID, DOI, and NetCDF layout are unchanged.
- The v0.3 TEMPO grid contract (native coordinates, overlap weights) is
  assumed identical for the standard V04 product; see
  [TEMPO product notes](../concepts/tempo-product-notes.md).

## What is new

- `tempo:no2_std` scope: CMR concept ID `C3685896708-LARC_CLOUD`
  (TEMPO_NO2_L3 V04, standard, DOI `10.5067/IS-40E/TEMPO/NO2_L3.004`).
- Wider default discovery lookback for the standard scope (24 hours versus 8
  for NRT), reflecting slower-settling standard granules.
- Chunked discovery windows: `sync_granule_discovery` (and the
  `tempo_no2_granule_discovery` / `tempo_no2_std_granule_discovery` op
  configs) accept an explicit `window_start_utc`/`window_end_utc` pair that
  overrides `lookback_hours`, for scripted multi-day backfills. See the
  [chunked backfill guide](../guides/backfill-30-days.md).
- Four new jobs (`tempo_no2_std_granule_discovery`,
  `tempo_no2_std_hourly_ingest`, `tempo_no2_std_dbt_build`,
  `tempo_no2_std_full_pipeline`) and one new schedule
  (`tempo_no2_std_pipeline_schedule`).
- A parallel `dbt/models/tempo_no2_std/` model tree publishing the same six
  marts and two observability models as NRT, under a `tempo_no2_std_*` prefix
  and an independent `dbt/seeds/tempo_no2_std_contract.csv` quality contract.

Do not copy v0.3 derived tables into the v0.4 database for either scope. v0.4
keeps v0.3's exact pooled region-hour rows and raw hour-revision incremental
watermark; it only adds the parallel standard scope.
