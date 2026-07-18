# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Added a second, independent TEMPO NO2 scope, `tempo:no2_std` (standard,
  V04, CMR `C3685896708-LARC_CLOUD`, DOI `10.5067/IS-40E/TEMPO/NO2_L3.004`),
  alongside the existing `tempo:no2` (NRT) scope. The standard scope ships
  its own settings, geography/granule/ops storage, Dagster assets/jobs
  (`tempo_no2_std_granule_discovery`, `tempo_no2_std_hourly_ingest`,
  `tempo_no2_std_dbt_build`, `tempo_no2_std_full_pipeline`), a
  disabled-by-default schedule (`tempo_no2_std_pipeline_schedule`), and a
  parallel `dbt/models/tempo_no2_std/` model tree with an independent
  `dbt/seeds/tempo_no2_std_contract.csv` quality contract.
- Added explicit `window_start_utc`/`window_end_utc` discovery-window support
  to `discover_granules`, `sync_granule_discovery`, and the granule-discovery
  op config, overriding `lookback_hours` for chunked, resumable backfills.
  See the [chunked backfill guide](docs/guides/backfill-30-days.md).
- Added [Upgrade to v0.4](docs/getting-started/upgrade-v04.md).

### Changed

- Bumped the warehouse schema version to `0.4`. `bootstrap_all_tempo_tables`
  now bootstraps schemas/tables/sequences for both scopes; a populated
  pre-0.4 warehouse now raises an error requiring a schema 0.4 rebuild rather
  than being reused in place.
- `tempo_ops_tbl`/`tempo_raw_tbl` and the DuckDB-backed sync/registry/granule
  helpers are now scope-aware (`scope: str = "no2"` by default), so existing
  NRT callers are unaffected.
- `make demo` remains NRT-only: it seeds contract CSVs but runs dbt with
  `--select tag:tempo,tag:no2`, so only NRT marts are built.

### Fixed

- Granule-discovery config now defaults `lookback_hours` to unset so ad-hoc
  standard-scope materialization uses the 24-hour standard lookback instead of
  the NRT eight-hour default.
- Discovery window parsing accepts trailing `Z` under Python 3.10 and rejects
  inverted `window_start_utc`/`window_end_utc` pairs.
- Scope-aware ingestion errors now name the active scope's region-registry
  asset and granule inventory schema.
- Documented the Chromium prerequisite for local docs render checks and
  corrected the releasing checklist so post-release CI verification targets
  the `main` commit rather than a tag push.

## [0.3.1] - 2026-07-18

This is the first public release. Releases and repository history before
0.3.1 remain private and unavailable.

### Added

- Added canonical source/licence and geography matrices, NASA TEMPO V02
  citation/DOI provenance, privacy terms, DCO sign-off, research-only and
  no-health/exposure/regulatory-advice notices, and no-endorsement language.
- Added repository-policy checks for geography-matrix completeness, synthetic
  fixtures, and forbidden tracked artifacts.
- Added the maintainer [releasing checklist](docs/development/releasing.md)
  and published the MkDocs site at
  https://hypertrial.github.io/titanskies-pipeline/.

### Changed

- Consolidated GitHub Actions into one offline runner capped at five minutes
  total. Full 100%-coverage, Dagster/dbt, browser, data-quality, Costguard,
  geography, and live NetCDF validation remain local release gates.

### Removed

- Removed GitHub-hosted live readiness so Earthdata downloads and production
  geography builds run only on operator-owned machines.
- Removed the nonessential copied `.cursor/rules/ponytail.mdc` material.

## [0.3.0] - 2026-07-17

### Added

- Task-oriented contributor and operator documentation, local browser
  rendering checks, GitHub issue/PR templates, CODEOWNERS, and constrained
  Dependabot updates
- Immutable geography generations behind one atomically replaced manifest,
  with deterministic build IDs, checksums, artifact modes, and rollback-safe
  retention
- Exact pooled region-hour statistics across all validated scans, monotonic raw
  hour revisions, sibling restoration, and checksum verification
- Incremental regional/anomaly dbt publication, including 28-day forward
  anomaly propagation and contract-version invalidation
- v0.2 to v0.3 clean-rebuild guide and production-geography live readiness

### Changed

- Constrained `dbt-core` below 1.12 and excluded incompatible 1.12+ updates from
  Dependabot until stable `dagster-dbt` releases support that boundary
- Prevented Dependabot from treating `netCDF4` 1.7.4 as universal while uv
  retains the compatible 1.7.3 split for Python 3.10 on Windows ARM64
- Warehouse and package schema version is now 0.3.0; populated v0.1/v0.2
  warehouses fail early with rebuild guidance
- CMR discovery now preserves nullable acquisition/revision metadata and uses
  one Arrow-backed merge without resetting processed state
- `tempo_no2_hourly_pipeline_schedule` now runs one discovery, pending
  processing, and incremental dbt publication through the full pipeline
- Environmental `zero_valid`, `low_coverage`, and `stale` findings remain
  visible but are advisory; integrity tests remain build blockers
- Live ingestion rejects synthetic geography and non-native production grids

### Fixed

- Public CMR discovery no longer requires Earthdata credentials; authentication
  remains mandatory when downloading granules.
- Dagster dbt runs fully release adapter connections so post-build live
  warehouse validation can reconnect safely.
- The credentialed live smoke uses a 24-hour discovery window while retaining
  its two-granule processing cap, so overnight UTC runs remain meaningful.
- dbt source assets now map to their ingestion asset keys, enforcing
  discovery, processing, then dbt order in the registered full-pipeline job.

### Removed

- Per-granule raw aggregates, legacy geography path/schedule variables, and
  unused `polars`, `tqdm`, `hypothesis`, `vcrpy`, and `pip-audit` dependencies

## [0.2.0] - 2026-07-13

### Added

- Pinned production geography for countries, first-level regions, US counties,
  Canadian census subdivisions, and Mexican municipalities, including dominant
  IANA timezones and deterministic equal-area overlap weights
- Public `tempo_no2_grid_latest` mart on TEMPO's native 0.02° grid, with
  latest-only storage for supported-country cells
- Credential-free `make demo` warehouse and documented native DuckDB CSV and
  Parquet export recipes
- v0.1 to v0.2 derived-warehouse rebuild guide and production-geography live
  readiness mode

### Changed

- Warehouse and package schema version is now 0.2.0; populated v0.1 warehouses
  fail early with rebuild guidance
- Regional coverage and statistics use actual overlap area, country metrics use
  direct country pixels, and zero-valid regions remain visible
- Robust anomalies use seven prior analysis-ready observations at the same IANA
  local hour, with median and MAD scaling
- Regional aggregates, native-grid latest rows, and ledger success now commit
  atomically per granule

### Fixed

- Hourly ingestion now records every failed granule, fails the Dagster run after
  processing the batch, and retries failed rows with a clean download on the next run
- Dagster discovery honors `TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS`, and hourly runs
  accept an optional `max_granules` limit
- `dbt/seeds/tempo_no2_contract.csv` is now the single runtime and dbt quality
  contract; inert threshold and backfill environment settings were removed
- `TEMPO_NO2_RAW_RETENTION_DAYS` now prunes only old, successfully processed
  NetCDF files while preserving their warehouse ledger history
- Manual discovery and one-granule live-readiness workflow with sanitized
  diagnostics and disposable local state
- Expanded operator runbooks for contracts, retention, backfills, and recovery
- Live Earthdata ingest: parse `earthaccess.DataGranule` metadata, persist HTTPS `download_url`, and download via authenticated `earthaccess.download`
- Real TEMPO L3 NRT NetCDF layout: read NO₂ and quality flags from the `product` group and 1D lat/lon grids
- Removed the approximate six-region bounding-box fallback; ingestion now
  requires validated v0.2 production overlap weights
- `.env` credential aliases: accept lowercase `earthdata_username` / `earthdata_password` in addition to `EARTHDATA_*`

### Release verification

- Full pinned geography build completed with TIGER/Line 2025 state
  (`59a220888a8d9be8117c4fcd38f542bd02d81abf0d198c78113595ad540dd957`)
  and county (`9c6e9d9076abce2670d1de255de3710c35ecca00a7005d88e012dec52d95f763`),
  Statistics Canada CSD 2025
  (`80157c64de60d6a52b4239e132243bb22d6d48bd78a7f88e9710c632f940ce7f`),
  INEGI MGI 2025
  (`f1335bab72d5582adab06e9e3b5d49b7c42da8f8d82e588f484ad6a7c7871d1b`),
  and timezone-boundary-builder 2026b
  (`f892b57ce8c7d9633a03ce9e6775d54544c05d9b8d62029bc6543091cac213c4`)
- Production geography result: 10,871 registry rows, 25,298,439 weights,
  404.555 seconds, 2.34 GiB observed peak RSS, 557,681-byte registry, and
  55,745,500-byte weight artifact; artifact SHA-256 values are
  `a64a6f0f308544fa3b3080aa8a3013572db1595214ba57cbee8a09e03b28ee86`
  and `6321ec8594b940a085b78410b67eb461475ed18ca670b6f2e15beee95ebdf7e7`
- One real V02 granule smoke committed 10,856 regional aggregates and
  3,833,865 latest-grid rows in 18.288 seconds
- Live Earthdata granule download and hourly ingest smoke verified locally with operator credentials
- Full local quality gate green (lint, 100% branch coverage, dbt build/unit/golden/freshness, GX, Costguard, docs-check)

## [0.1.0] - 2026-07-12

### Added

- Local Python pipeline foundation for NASA TEMPO NO₂ L3 NRT over Canada, the United States, and Mexico
- Dagster orchestration with granule discovery, hourly ingest, dbt build, and full pipeline jobs
- earthaccess CMR discovery pinned to concept `C3685668637-LARC_CLOUD`
- NetCDF validation and area-weighted regional aggregation into DuckDB raw and ops schemas
- Geography registry and TEMPO grid overlap-weight artifacts via operator GIS script
- dbt staging, intermediate, mart, and observability models for seven public tables
- DuckDB warehouse bootstrap, granule ledger, and profiling utilities
- MkDocs documentation site with CI docs-check validation
- GitHub Actions CI: lint, tests, docs build, dbt parse, dbt build, GX checks, and Costguard
- Schedules disabled by default; opt-in via `.env` for live ingestion

### Release verification

- Full local quality gate green (lint, 100% branch coverage, dbt build/unit/golden/freshness, GX, Costguard, docs-check)
- Live Earthdata granule download smoke requires operator Earthdata Login credentials (`.netrc` or interactive `earthaccess.login()`); not run in CI
