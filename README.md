# TitanSkies Pipeline

[![CI](https://github.com/hypertrial/titanskies-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/titanskies-pipeline/actions/workflows/ci.yml)
[![Coverage: 100%](https://img.shields.io/badge/coverage-100%25-brightgreen)](AGENTS.md#quality-gate)
[![Docs: MkDocs](https://img.shields.io/badge/docs-MkDocs-blue)](https://hypertrial.github.io/titanskies-pipeline/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

TitanSkies Pipeline is an open-source, local-first NASA science warehouse.
Version `0.7.0` adds pinned, unscheduled source-preflight profiles for
reproducing DOI `10.1029/2025JD044565` and DOI `10.1029/2024GL114185`. They
classify exact, provider-reprocessed, method-equivalent, and unavailable
inputs before acquisition while preserving every existing TEMPO,
`riverpulse:events`, and `plumegraph:events` lane.

Dagster coordinates discovery, NetCDF/CSV processing, DuckDB storage, and dbt
publication. Every operator controls the resulting local DuckDB file;
source and derived-data rights remain governed by their source terms. This
repository does not host a dataset or API.

TitanSkies is research and engineering software, not health, personal-exposure,
medical, safety, or regulatory advice. Near-real-time observations are
provisional and are not measurements of an individual's exposure. NASA and
the geography providers do not endorse TitanSkies or Hypertrial.

## Start here

| Reader | First step |
| --- | --- |
| Analysts | [Analysts hub](docs/audiences/analysts.md), then [Query the warehouse](docs/guides/query-the-warehouse.md), the [Data dictionary](docs/reference/data-dictionary.md), and the [Warehouse reference](docs/reference/warehouse.md). |
| Operators | [Operators hub](docs/audiences/operators.md), then build the demo, follow [Run the pipeline](docs/guides/run-the-pipeline.md) and [Troubleshooting](docs/guides/troubleshooting.md). |
| Contributors | [Contributors hub](docs/audiences/contributors.md), the [Development guide](docs/development/index.md), and [CONTRIBUTING.md](CONTRIBUTING.md). |
| Integrators | [Integrators hub](docs/audiences/integrators.md) and [Integration](docs/concepts/integration.md). |
| Maintainers | Review the [Architecture](docs/concepts/architecture.md), [Orchestration reference](docs/reference/orchestration.md), [Security policy](SECURITY.md), and [Changelog](CHANGELOG.md). |

Review the canonical [third-party and source notices](THIRD_PARTY_NOTICES.md)
and [privacy notice](PRIVACY.md) before use or redistribution.

## Quickstart

Build a credential-free demo with administrative history and native-grid latest
observations:

```bash
uv sync --locked --extra dev
uv run make demo
```

Build the separate credential-free RiverPulse network, revision, discharge,
provenance, and observability demo with:

```bash
uv run make riverpulse-demo
```

Build the separate credential-free PlumeGraph cohort, analysis, validation,
and immutable-release demo with:

```bash
uv run make plumegraph-demo
```

Validate the two tracked synthetic source inventories without downloading
production payloads:

```bash
uv run make sun2025-preflight
uv run make andreadis2025-preflight
```

The demo prints its `.cache/demo.duckdb` path, relation counts, sample queries,
and verified CSV/Parquet export paths. Synthetic geography is demo/test-only.
Serve the complete documentation locally with:

```bash
uv run make docs-serve
```

Open `http://127.0.0.1:8000` while that process is running. Published docs are
also available at
[hypertrial.github.io/titanskies-pipeline](https://hypertrial.github.io/titanskies-pipeline/).

For development:

```bash
uv sync --locked --extra dev --extra geo --extra plumegraph
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
python scripts/build_riverpulse_network.py --synthetic
uv run make dbt-parse
uv run make dagster-dev
```

Schedules are disabled by default. Before live ingestion, build and register
pinned production geography:

```bash
uv sync --locked --extra geo
python scripts/build_region_artifacts.py --output-dir artifacts/geo
uv run make dagster-dev
```

Credentialed source verification is local-only and opt-in: run
`uv run make live-smoke` with operator-owned Earthdata credentials. GitHub
Actions never downloads live NetCDF or production geography.

## Architecture and outputs

The pipeline is intentionally local and inspectable:

- NASA Earthdata CMR discovery and authenticated downloads feed raw NetCDF
  processing and a durable DuckDB granule ledger.
- Pinned overlap weights aggregate native TEMPO cells into Canadian, US, and
  Mexican administrative regions.
- Dagster runs discovery, ingestion, dbt publication, and the full pipeline
  independently per scope (`tempo_no2_*` and `tempo_no2_std_*` jobs).
- dbt publishes six analyst marts and two observability models per scope.
- RiverPulse pins SWORD v17b, collects `SWOT_L2_HR_RiverSP_reach_D` through
  bounded Hydrocron requests, and publishes five marts plus two observability
  models without filtering source-quality rows.
- PlumeGraph pins `TEMPO_NO2_L2` V04, HRRR forecast-hour-zero analysis, and
  EPA CAMD apportioned hourly emissions. It publishes eight evidence marts,
  seven observability surfaces, and checksum-addressed local releases.
- The two reproduction profiles pin paper-time collection, code, network,
  meteorology, emissions, and supplementary-artifact requirements. Exact-only
  readiness resolves provider metadata into deterministic bounded inventories;
  preflight creates an auditable acquisition plan only when every source
  passes. Neither workflow downloads or reproduces the papers.

Query `tempo_no2_marts` first and use `tempo_no2_observability` to investigate
freshness and quality. The main historical relation is
`tempo_no2_region_hourly`; `tempo_no2_grid_latest` intentionally retains only
the latest supported-country observation for each native 0.02° grid cell. The
standard scope publishes the identical shapes under `tempo_no2_std_marts`
and `tempo_no2_std_observability`.
RiverPulse relations live under `riverpulse_events_marts` and
`riverpulse_events_observability`.
PlumeGraph relations live under `plumegraph_events_marts` and
`plumegraph_events_observability`; attribution is evidence, not proof or a
regulatory conclusion.

```sql
select *
from tempo_no2_marts.tempo_no2_region_hourly
where is_analysis_ready;
```

See the [Architecture](docs/concepts/architecture.md), [Data contracts](docs/reference/data-contracts.md),
and [Data dictionary](docs/reference/data-dictionary.md) for the complete model.
TitanSkies v0.7 requires a clean derived-warehouse rebuild; older raw NetCDF,
verified geography, verified SWORD archives, and checksum-verified source
caches remain reusable.
`make demo` only builds
the `tempo:no2` (NRT) scope; see
[Upgrade to v0.7](docs/getting-started/upgrade-v07.md) for the rebuild and
reproduction-preflight rollout.

## Community

- Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing code or data-contract changes.
- Report vulnerabilities privately through [SECURITY.md](SECURITY.md).
- Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) in project spaces.
- Use GitHub issue forms for reproducible bugs, focused features, and documentation gaps.

TitanSkies has no telemetry and sends no user, warehouse, measurement, or
credential data to Hypertrial. NASA Earthdata, GitHub, and geography providers
may independently log requests under their own policies. MIT covers
Hypertrial's original code only; downloaded source data, reference geography,
and generated outputs retain their applicable source terms.
