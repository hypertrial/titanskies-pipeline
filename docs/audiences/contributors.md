# Contributors

Use this hub when changing ingestion, geography, dbt models, docs, or
orchestration. For operator setup, start with
[Quickstart](../getting-started/index.md).

## Setup

```bash
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
```

Docs contributors should install Chromium for Playwright checks once:

```bash
uv run playwright install chromium
```

## Which Quality Gate?

| Change | Gate |
| --- | --- |
| Docs, styles, or `mkdocs.yml` only | `uv run make docs-check` |
| Ordinary code or test PR | Offline gate in AGENTS.md / CONTRIBUTING |
| Pre-release / tagging | Full local release gate + optional live-smoke |

See [AGENTS.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/AGENTS.md)
and [Development](../development/index.md).

## Design Decisions

TitanSkies intentionally keeps dual scopes independent, rebuilds derived
warehouses instead of migrating them, and treats NetCDF/geography as
operator-local. Read [Design decisions](../concepts/decisions.md) before
proposing hosted APIs or in-place migrations.
