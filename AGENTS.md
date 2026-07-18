# AGENTS.md

TitanSkies Pipeline is an open-source, local-first NASA TEMPO NO₂ warehouse.
Version `0.3.x` ships `TEMPO_NO2_L3_NRT` administrative history and native-grid
latest observations over Canada, the United States, and Mexico.
Stack: **Dagster**, **earthaccess**, **xarray**, **dbt**, **DuckDB**, **uv**, **Ruff** + **sqlfluff**, **pytest**.

## Setup

```bash
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
```

Default warehouse: `titanskies.duckdb` in the repo root. Keep schedules disabled unless intentionally running live ingestion:

```dotenv
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=false
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
GitHub Actions intentionally uses one runner for less than five cumulative
minutes and runs lint, fast offline tests, saved HTTP contracts, dbt parse,
and a strict documentation build. Live CMR, geography, NetCDF, Dagster/dbt
integration, browser, 100%-coverage, data-quality, and Costguard validation
remain local release checks.

## Do not

- Commit `.env`, secrets, `*.duckdb` files, NetCDF exports, or geo artifacts built from live boundaries unless they are explicit test fixtures.
- Add hosted API/dashboard work, pixel-level history, or legacy compatibility
  shims without explicit product direction.
