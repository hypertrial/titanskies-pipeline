# Configuration

Key environment variables:

- `DUCKDB_NAME` / `DUCKDB_PATH`
- `DBT_PROFILES_DIR`
- `EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` (or lowercase `earthdata_username` / `earthdata_password`)
- `TEMPO_NO2_CMR_CONCEPT_ID`
- `TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS`
- `TEMPO_NO2_RAW_DATA_DIR`
- `TEMPO_NO2_RAW_RETENTION_DAYS`
- `TEMPO_GEOGRAPHY_MANIFEST_PATH` (defaults to `artifacts/geo/tempo_geography_artifacts.json`)
- `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED`

Quality thresholds and accepted flags live only in
`dbt/seeds/tempo_no2_contract.csv`. Its `contract_version` invalidates
incremental hourly/anomaly results. Change that reviewed contract rather than
adding environment-specific quality policy.
