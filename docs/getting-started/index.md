# Quickstart

No credentials or GIS dependencies are needed for the demo:

```bash
uv sync --locked --extra dev
uv run make demo
uv run make riverpulse-demo
```

Open the printed `.cache/demo.duckdb` path in DuckDB to query administrative
history and the native-grid latest mart. `make demo` builds the NRT
(`tempo:no2`) scope only; standard-scope raw/ops schemas bootstrap empty and
std marts appear only after an explicit std dbt/ingest run. See
[Choose a scope](choose-a-scope.md).

For pipeline development:

```bash
uv sync --locked --extra dev --extra geo
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
python scripts/build_riverpulse_network.py --synthetic
uv run make dbt-parse
uv run make dagster-dev
```

NRT jobs:

- `tempo_no2_granule_discovery`
- `tempo_no2_hourly_ingest`
- `tempo_no2_dbt_build`
- `tempo_no2_full_pipeline`

Standard-scope mirrors use the `tempo_no2_std_*` job names
([Orchestration](../reference/orchestration.md)).
RiverPulse jobs use `riverpulse_events_source_discovery`,
`riverpulse_events_observation_ingest`, `riverpulse_events_dbt_build`, and
`riverpulse_events_full_pipeline`.

Version 0.5 requires a new warehouse for populated v0.4 DuckDB files. See the
[v0.5 upgrade guide](upgrade-v05.md) before enabling
schedules against an existing deployment. Older rebuild notes remain under
[Upgrade to v0.4](upgrade-v04.md),
[Upgrade to v0.3](upgrade-v03.md) and [Upgrade to v0.2](upgrade-v02.md).
