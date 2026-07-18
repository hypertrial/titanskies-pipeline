# Configuration

Key environment variables:

- `DUCKDB_NAME` / `DUCKDB_PATH`
- `DBT_PROFILES_DIR`
- `EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` (or lowercase `earthdata_username` / `earthdata_password`)
- `TEMPO_GEOGRAPHY_MANIFEST_PATH` (defaults to `artifacts/geo/tempo_geography_artifacts.json`)

TEMPO NO2 NRT (`tempo:no2`, CMR `C3685668637-LARC_CLOUD`):

- `TEMPO_NO2_CMR_CONCEPT_ID`
- `TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS` (default `8`)
- `TEMPO_NO2_RAW_DATA_DIR` (default `data/raw/tempo_no2_nrt`)
- `TEMPO_NO2_RAW_RETENTION_DAYS` (default `30`)
- `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED`

TEMPO NO2 Standard (`tempo:no2_std`, CMR `C3685896708-LARC_CLOUD`, TEMPO_NO2_L3
V04 standard product, DOI `10.5067/IS-40E/TEMPO/NO2_L3.004`). Standard
granules settle more slowly than NRT, so the default lookback and retention
windows are wider:

- `TEMPO_NO2_STD_CMR_CONCEPT_ID`
- `TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS` (default `24`)
- `TEMPO_NO2_STD_RAW_DATA_DIR` (default `data/raw/tempo_no2_std`)
- `TEMPO_NO2_STD_RAW_RETENTION_DAYS` (default `30`)
- `TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED` (default `false`; the standard
  schedule ships disabled and must be opted into explicitly)

Quality thresholds and accepted flags live only in
`dbt/seeds/tempo_no2_contract.csv` (NRT) and `dbt/seeds/tempo_no2_std_contract.csv`
(standard). Each contract's `contract_version` invalidates that scope's
incremental hourly/anomaly results independently. Change the reviewed
contract for the affected scope rather than adding environment-specific
quality policy.

`make demo` only seeds and builds the NRT (`tempo:no2`) scope; the standard
scope's marts and observability tables are created but remain empty until a
standard discovery/ingest run populates them.
