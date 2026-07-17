# Upgrade from v0.2 to v0.3

v0.3 is a derived-warehouse rebuild boundary. It does not migrate a populated
v0.1/v0.2 DuckDB file in place.

1. Stop `tempo_no2_hourly_ingest_schedule` and any operator backfills.
2. Back up the old DuckDB file.
3. Upgrade the checkout and dependencies.
4. Build pinned production geography. Existing `.cache/geo_sources` downloads
   remain reusable; the build publishes a new immutable generation and atomic
   manifest.
5. Point `DUCKDB_PATH` at a new file and materialize
   `tempo/no2/ops/region_registry`.
6. Run the desired discovery lookback/backfill. Existing raw NetCDF files may
   be copied or linked into `TEMPO_NO2_RAW_DATA_DIR`; every restored sibling is
   checksum-verified before pooling.
7. Run `tempo_no2_full_pipeline` and validate administrative, country, anomaly,
   native-grid, granule-observability, and data-quality relations.
8. Replace the old schedule variable with
   `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=true`, restart Dagster, and
   enable `tempo_no2_hourly_pipeline_schedule`.

Do not copy v0.2 derived tables into the v0.3 database. v0.3 replaces
per-granule aggregates with exact pooled region-hour rows and uses raw hour
revisions as the incremental dbt watermark.
