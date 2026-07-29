# Run the pipeline

Build geography artifacts and start Dagster:

```bash
uv sync --locked --extra geo
python scripts/build_region_artifacts.py --output-dir artifacts/geo
uv run make dagster-dev
```

Launch `tempo_no2_full_pipeline`. Schedules are stopped by default.

Hourly operator loop:

1. verify registered production geography;
2. discover once through `tempo_no2_granule_discovery`;
3. process pending granules and replace exact region-hour rows;
4. publish incremental dbt marts.

The hourly asset may set `max_granules` for a bounded smoke run. Production
runs normally leave it null. Synthetic geography is demo/test-only. If any selected granule fails, successful rows
remain committed but the job is red; the next run retries failed ledger rows.

After a successful run, query:

```sql
select *
from tempo_no2_marts.tempo_no2_region_latest;
```

## RiverPulse path

Build and register the network once:

```bash
python scripts/build_riverpulse_network.py --output-dir artifacts/riverpulse
uv run make dagster-dev
```

Materialize `riverpulse/events/ops/network_registry`, then launch
`riverpulse_events_full_pipeline`. Discovery plans rolling 90-day Hydrocron
requests; ingest processes requests serially and commits each successful
sibling; dbt runs only after ingest finishes without failures. For the
science-phase backfill, set `backfill: true` on the source-inventory asset.
The default start is `2023-08-01T00:00:00Z`.

If any request fails, successful siblings remain committed, every failure is
recorded, and publication is blocked. Correct the cause and rerun;
deterministic request/revision IDs make overlap and replay safe.
