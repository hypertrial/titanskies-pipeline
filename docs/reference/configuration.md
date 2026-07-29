# Configuration

`.env.example` is the executable inventory for local configuration. Copy it to
`.env` for operator overrides. Quality thresholds and accepted flags live only
in `dbt/seeds/tempo_no2_contract.csv` (NRT) and
`dbt/seeds/tempo_no2_std_contract.csv` (standard) — not in environment
variables. RiverPulse scientific versions and accepted quality classes live
only in `dbt/seeds/riverpulse_events_contract.csv`. PlumeGraph scientific
versions, detection thresholds, calibration gates, and emission variants live
only in `dbt/seeds/plumegraph_events_contract.csv`.

Unset variables use the defaults below. If a numeric or date variable is
**set** but not parseable (integer, float, or ISO `YYYY-MM-DD`), settings load
raises `ValueError` naming that variable instead of silently falling back.
Runtime helpers such as `get_tempo_scope_settings()` read the current process
environment on each call. Import-time `TEMPO_NO2_*` module constants remain for
`from settings import *` compatibility; Dagster job/schedule definitions snapshot
settings once at process import, so env changes require a reload.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DUCKDB_NAME` | `titanskies.duckdb` | Warehouse filename under the repository root when `DUCKDB_PATH` is unset |
| `DUCKDB_PATH` | unset | Absolute warehouse path; takes precedence over `DUCKDB_NAME` |
| `DBT_PROFILES_DIR` | repository `dbt/profiles` | Optional absolute dbt profiles directory |
| `EARTHDATA_USERNAME` | unset | NASA Earthdata Login username for live granule download (`earthaccess`; lowercase `earthdata_username` also accepted) |
| `EARTHDATA_PASSWORD` | unset | NASA Earthdata Login password (`earthaccess`; lowercase `earthdata_password` also accepted). Prefer `~/.netrc` when possible |
| `TEMPO_NO2_CMR_CONCEPT_ID` | `C3685668637-LARC_CLOUD` | CMR concept ID for TEMPO NO2 L3 NRT; do not change without product review |
| `TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS` | `8` | Routine NRT discovery window (hours). Backfills use Dagster run config |
| `TEMPO_GEOGRAPHY_MANIFEST_PATH` | `artifacts/geo/tempo_geography_artifacts.json` | Atomic geography manifest from `scripts/build_region_artifacts.py` |
| `TEMPO_NO2_RAW_DATA_DIR` | `data/raw/tempo_no2_nrt` | NRT raw NetCDF storage root |
| `TEMPO_NO2_RAW_RETENTION_DAYS` | `30` | Prune processed NRT NetCDF files older than this many days (`processed_at`) |
| `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED` | `false` | Opt-in NRT hourly full-pipeline schedule; leave false in local dev and CI |
| `TEMPO_NO2_STD_CMR_CONCEPT_ID` | `C3685896708-LARC_CLOUD` | CMR concept ID for TEMPO NO2 L3 V04 standard; do not change without product review |
| `TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS` | `24` | Routine standard discovery window (hours). Wider than NRT because standard granules settle more slowly |
| `TEMPO_NO2_STD_RAW_DATA_DIR` | `data/raw/tempo_no2_std` | Standard raw NetCDF storage root |
| `TEMPO_NO2_STD_RAW_RETENTION_DAYS` | `30` | Prune processed standard NetCDF files older than this many days (`processed_at`) |
| `TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED` | `false` | Opt-in standard pipeline schedule; ships disabled and must be enabled explicitly |
| `RIVERPULSE_NETWORK_MANIFEST_PATH` | `artifacts/riverpulse/riverpulse_network_artifacts.json` | Atomic, checksum-verified SWORD network manifest |
| `RIVERPULSE_RAW_DATA_DIR` | `data/raw/riverpulse_events` | Indefinite immutable Hydrocron response snapshot root |
| `RIVERPULSE_HYDROCRON_API_KEY` | unset | Optional `x-hydrocron-key` header; never stored in request state, snapshots, URLs, or errors |
| `RIVERPULSE_REQUEST_INTERVAL_SECONDS` | `1.0` | Minimum serial request spacing; production must remain at least one second |
| `RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED` | `false` | Opt-in Sunday 03:00 UTC discovery/ingest/dbt schedule; network bootstrap excluded |
| `PLUMEGRAPH_COHORT_MANIFEST_PATH` | `artifacts/plumegraph/plumegraph_cohort.json` | Review-gated frozen facility cohort manifest |
| `PLUMEGRAPH_RAW_DATA_DIR` | `data/raw/plumegraph_events` | Indefinite checksum-addressed source snapshot root |
| `PLUMEGRAPH_RELEASE_DIR` | `data/releases/plumegraph_events` | Immutable local evidence release root |
| `PLUMEGRAPH_DISCOVERY_LOOKBACK_DAYS` | `14` | Routine half-open rediscovery window in days |
| `PLUMEGRAPH_RAW_CACHE_RETENTION_DAYS` | `30` | Prunable full-granule cache age; retained subsets are not pruned |
| `PLUMEGRAPH_HRRR_STORE_URL` | `s3://hrrrzarr` | Public chunked HRRR archive root |
| `PLUMEGRAPH_EPA_API_KEY` | unset | CAM API `x-api-key` header; never persisted or logged |
| `PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED` | `false` | Opt-in daily 06:00 UTC pipeline schedule; bootstrap and release excluded |

## Scope notes

`make demo` remains NRT-only: it seeds both contract CSVs but runs dbt with
`--select tag:tempo,tag:no2`, so only NRT marts are built. Standard-scope
raw/ops schemas bootstrap empty; std marts and observability appear after an
explicit standard discovery/ingest and dbt run.

Each contract's `contract_version` invalidates that scope's incremental
hourly/anomaly results independently. Change the reviewed contract for the
affected scope rather than adding environment-specific quality policy.
RiverPulse collection name, collection version, SWORD version, and quality
classes cannot be overridden through environment variables.
The same rule applies to every scientific value in the PlumeGraph contract.

See [Enable the schedule](../guides/enable-schedule.md) and
[Day-two operations](../guides/day-two-operations.md).
