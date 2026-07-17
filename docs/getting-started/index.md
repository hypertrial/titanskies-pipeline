# Quickstart

No credentials or GIS dependencies are needed for the demo:

```bash
uv sync --locked --extra dev
uv run make demo
```

Open the printed `.cache/demo.duckdb` path in DuckDB to query administrative
history and the native-grid latest mart.

For pipeline development:

```bash
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
uv run make dbt-parse
uv run make dagster-dev
```

Jobs:

- `tempo_no2_granule_discovery`
- `tempo_no2_hourly_ingest`
- `tempo_no2_dbt_build`
- `tempo_no2_full_pipeline`

Version 0.3 requires a new derived warehouse. See the
[v0.2 to v0.3 upgrade guide](upgrade-v03.md) before enabling schedules against
an existing deployment.
