# Development guide

## Setup

```bash
uv sync --locked --extra dev --extra geo
uv run playwright install chromium
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
```

Keep schedules disabled. Tests and demos may use synthetic geography; live
boundaries and NetCDF downloads must not enter tracked fixtures. A fixture
should contain only the fields and rows needed to exercise its contract.

## Targeted checks

| Change | Run while iterating |
| --- | --- |
| Configuration or storage | `uv run make unit-core` |
| NetCDF discovery or ingestion | `uv run make unit-ingest` and `uv run make contract-http` |
| Dagster definitions | `uv run make unit-orchestration` and `uv run make integration-dagster` |
| dbt SQL or contracts | `uv run make integration-dbt`, `uv run make dbt-unit`, and `uv run make golden-dbt` |
| Documentation | `uv run make docs-check` |

All test databases belong under pytest temporary directories or `.cache/`.
Tests must not depend on a developer's `.env`, default warehouse, Earthdata
credentials, network access, or previously generated geography.

## Complete quality gate

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

The commands above are the full local release gate, and the accumulated
coverage report must remain at 100% branch coverage. The local Costguard gate
uses the pinned `2.5.0` release and high-confidence policy:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

## Review policy

Update contracts, golden fixtures, documentation, and the Unreleased changelog
with the behavior they protect. Public mart grains, schedules, and production
geography changes need explicit callouts in the pull request. Test clean
rebuilds directly; do not add v0.1/v0.2 compatibility paths.
