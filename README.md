# TitanSkies Pipeline

[![CI](https://github.com/hypertrial/titanskies-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/titanskies-pipeline/actions/workflows/ci.yml)
[![Coverage: 100%](https://img.shields.io/badge/coverage-100%25-brightgreen)](AGENTS.md#quality-gate)
[![Docs: MkDocs](https://img.shields.io/badge/docs-MkDocs-blue)](https://hypertrial.github.io/titanskies-pipeline/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

TitanSkies Pipeline is an open-source, local-first NASA TEMPO NO₂ warehouse.
Version `0.4.0` publishes administrative history and native-grid latest
observations for Canada, the United States, and Mexico, across two parallel
scopes: `tempo:no2` (near-real-time) and `tempo:no2_std` (standard, V04).

Dagster coordinates Earthdata discovery, NetCDF processing, DuckDB storage,
and dbt publication. Every operator controls the resulting local DuckDB file;
source and derived-data rights remain governed by their source terms. This
repository does not host a dataset or API.

TitanSkies is research and engineering software, not health, personal-exposure,
medical, safety, or regulatory advice. Near-real-time observations are
provisional and are not measurements of an individual's exposure. NASA and
the geography providers do not endorse TitanSkies or Hypertrial.

## Start here

| Reader | First step |
| --- | --- |
| Analysts | Read [Query the warehouse](docs/guides/query-the-warehouse.md), the [Data dictionary](docs/reference/data-dictionary.md), and the [Warehouse reference](docs/reference/warehouse.md). |
| Operators | Build the demo, then follow [Run the pipeline](docs/guides/run-the-pipeline.md) and [Troubleshooting](docs/guides/troubleshooting.md). |
| Contributors | Use the [Development guide](docs/development/index.md) and [CONTRIBUTING.md](CONTRIBUTING.md). |
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
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
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

Query `tempo_no2_marts` first and use `tempo_no2_observability` to investigate
freshness and quality. The main historical relation is
`tempo_no2_region_hourly`; `tempo_no2_grid_latest` intentionally retains only
the latest supported-country observation for each native 0.02° grid cell. The
standard scope publishes the identical shapes under `tempo_no2_std_marts`
and `tempo_no2_std_observability`.

```sql
select *
from tempo_no2_marts.tempo_no2_region_hourly
where is_analysis_ready;
```

See the [Architecture](docs/concepts/architecture.md), [Data contracts](docs/reference/data-contracts.md),
and [Data dictionary](docs/reference/data-dictionary.md) for the complete model.
TitanSkies v0.4 requires a clean derived-warehouse rebuild; older raw NetCDF
files and geography source caches remain reusable. `make demo` only builds
the `tempo:no2` (NRT) scope; see
[Upgrade to v0.4](docs/getting-started/upgrade-v04.md) for enabling the
standard scope.

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
