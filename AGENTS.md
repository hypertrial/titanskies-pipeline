# AGENTS.md

TitanSkies Pipeline is an open-source, local-first NASA science warehouse.
Version `0.5.x` ships two TEMPO scopes (`tempo:no2` and `tempo:no2_std`) plus
the permanent `riverpulse:events` SWOT lane. TEMPO uses
`TEMPO_NO2_L3_NRT` and
`tempo:no2_std` (`TEMPO_NO2_L3` V04, standard). Both publish administrative
history and native-grid latest observations over Canada, the United States,
and Mexico. RiverPulse publishes SWORD v17b reaches, Hydrocron Version D
observation/discharge revisions, current revisions, and provenance.
`make demo` remains NRT-only; standard-scope raw/ops schemas bootstrap empty
and std marts appear only after an explicit standard discovery/ingest (and
dbt) run. `make riverpulse-demo` is a separate offline RiverPulse demo.
Stack: **Dagster**, **earthaccess**, **xarray**, **dbt**, **DuckDB**, **uv**, **Ruff** + **sqlfluff**, **pytest**.

## Setup

```bash
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
python scripts/build_riverpulse_network.py --synthetic
```

Default warehouse: `titanskies.duckdb` in the repo root. Keep schedules disabled unless intentionally running live ingestion:

```dotenv
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=false
TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=false
RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED=false
```

## Quality gate

```bash
uv run make lint
uv run make test-cov
uv run make dagster-jobs-smoke-cov
uv run make dagster-refresh-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make coverage-report
uv run make docs-check
uv run make check-secrets
uv run make dbt-parse
uv run make dbt-build-ci
uv run make gx-data-quality
uv run make costguard
```

This is the full local release gate. For tagging, GitHub Releases, and docs
publication, follow [docs/development/releasing.md](docs/development/releasing.md).
The offline `ci.yml` fast-gate job intentionally stays under five cumulative
minutes and runs lint, fast offline tests, saved HTTP contracts, dbt parse,
a strict documentation build, and docs structure/inventory tests
(`docs-build docs-structure`). A separate Docs workflow publishes GH Pages on
`main`, `workflow_dispatch`, and `v*` tags. Playwright render checks and demo
recipe SQL smoke stay in local `docs-check` only. Live CMR, geography, NetCDF,
Dagster/dbt integration, browser, 100%-coverage, data-quality, and Costguard
validation remain local release checks.

## Docs

Docs follow Audiences → Get started → Guides → Reference → Concepts → Development.
Docs-only PRs: `uv run make docs-check` (strict MkDocs build + structure +
Playwright render + demo recipe SQL smoke). CI runs `docs-build docs-structure`
only. Install Chromium once for render checks:
`uv run playwright install chromium`.

## Do not

- Commit `.env`, secrets, `*.duckdb` files, NetCDF exports, Hydrocron
  snapshots, SWORD archives/artifacts, or geo artifacts built from live
  sources unless they are explicit synthetic test fixtures.
- Add hosted API/dashboard work, pixel-level history, or legacy compatibility
  shims without explicit product direction.
