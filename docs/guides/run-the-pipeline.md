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

## PlumeGraph path

Install the optional source dependencies and register a science-owner-approved
75-facility cohort:

```bash
uv sync --locked --extra dev --extra geo --extra plumegraph
uv run make plumegraph-live-smoke
```

The one-day smoke uses a disposable warehouse and does not commit live
artifacts. For the full 2024 benchmark, materialize the facility registry,
launch `plumegraph_events_source_discovery` with `backfill: true`, then run
`plumegraph_events_full_pipeline`. Successful region-date siblings remain
committed, but a failed source request or analysis partition blocks dbt and
validation until a clean retry.

Build an immutable release only after the held-out benchmark has passed:

```bash
PLUMEGRAPH_RELEASE_VERSION=v0.7.0 uv run make plumegraph-release
```

Benchmark versions are byte-frozen after their first successful load. The
release command also requires the selected validation run to match the exact
current region-date generation manifest. When the first validation fits a new
calibration temperature, rerun analysis, dbt publication, and validation once
so calibrated candidates and the final validation pin the same generation.

Release publication is excluded from the daily schedule.
