# Orchestration

TitanSkies ships two parallel TEMPO NO2 pipelines: `tempo:no2` (NRT) and
`tempo:no2_std` (standard/V04). Both scopes share the same asset/job/schedule
shape; only concept IDs, lookback windows, and schema names differ.

## `tempo:no2` (NRT)

Assets:

- `tempo/no2/ops/region_registry`
- `tempo/no2/raw/granule_inventory`
- `tempo/no2/raw/region_hour_aggregates`

Jobs:

- `tempo_no2_granule_discovery`
- `tempo_no2_hourly_ingest`
- `tempo_no2_dbt_build`
- `tempo_no2_full_pipeline`

`tempo_no2_hourly_pipeline_schedule` targets `tempo_no2_full_pipeline` and runs
registry precondition, one CMR discovery, pending processing, and incremental
dbt publication once per hour. It is **disabled by default**
(`TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=false`). The manual
`tempo_no2_hourly_ingest` job remains processing-only and accepts optional
`max_granules`. Geography bootstrap is an explicit operator action and is
excluded from recurring selections.

## `tempo:no2_std` (standard, V04)

Assets:

- `tempo/no2_std/ops/region_registry`
- `tempo/no2_std/raw/granule_inventory`
- `tempo/no2_std/raw/region_hour_aggregates`

Jobs:

- `tempo_no2_std_granule_discovery`
- `tempo_no2_std_hourly_ingest`
- `tempo_no2_std_dbt_build`
- `tempo_no2_std_full_pipeline`

`tempo_no2_std_pipeline_schedule` targets `tempo_no2_std_full_pipeline` on a
`:30` offset from the NRT schedule and is **disabled by default**
(`TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED=false`). Standard granules settle
more slowly than NRT, so its default discovery lookback is 24 hours (versus 8
for NRT). Both `tempo_no2_granule_discovery` and `tempo_no2_std_granule_discovery`
accept an optional explicit `window_start_utc`/`window_end_utc` pair on the
granule inventory op config, which overrides `lookback_hours` for chunked
backfills; see the [backfill guide](../guides/backfill-30-days.md).

## Shared `titanskies_dbt` asset

Both scopes publish through the single `titanskies_dbt` dbt asset selection,
scoped per job via `dbt_select` (`+tag:tempo,tag:no2` or
`+tag:tempo,tag:no2_std`).

## `riverpulse:events`

RiverPulse is explicit and separate from the TEMPO `ScopeSpec` registry.

Assets:

- `riverpulse/events/ops/network_registry`
- `riverpulse/events/raw/source_inventory`
- `riverpulse/events/raw/observations`

Jobs:

- `riverpulse_events_source_discovery`
- `riverpulse_events_observation_ingest`
- `riverpulse_events_dbt_build`
- `riverpulse_events_full_pipeline`

`riverpulse_events_pipeline_schedule` targets the full pipeline Sundays at
03:00 UTC and ships stopped
(`RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED=false`). It excludes network
bootstrap. Discovery defaults to a rolling 90-day window; explicit backfill
starts at `2023-08-01T00:00:00Z` and is split into calendar-year half-open
requests. Ingest processes all planned requests serially. A failed sibling is
recorded and fails the asset after successful siblings commit, so the dbt
asset cannot run until a clean retry.

The shared `titanskies_dbt` asset uses
`tag:riverpulse,tag:events` for RiverPulse. The product-neutral TitanSkies dbt
translator preserves all existing TEMPO asset keys and maps RiverPulse layers
under `riverpulse/events`.

## `plumegraph:events`

PlumeGraph is explicit and is not added to the TEMPO `ScopeSpec` factory.

Assets:

- `plumegraph/events/ops/facility_registry`
- `plumegraph/events/raw/source_inventory`
- `plumegraph/events/raw/tempo_snapshots`
- `plumegraph/events/raw/hrrr_snapshots`
- `plumegraph/events/raw/camd_emissions`
- `plumegraph/events/intermediate/analysis_results`
- `plumegraph/events/observability/validation`
- `plumegraph/events/releases/evidence_ledger`

Jobs:

- `plumegraph_events_source_discovery`
- `plumegraph_events_source_ingest`
- `plumegraph_events_analysis`
- `plumegraph_events_dbt_build`
- `plumegraph_events_validation`
- `plumegraph_events_release_build`
- `plumegraph_events_full_pipeline`

`plumegraph_events_daily_pipeline_schedule` targets the full pipeline daily at
06:00 UTC and ships stopped
(`PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED=false`). The recurring selection
uses a 14-day rediscovery window and excludes cohort bootstrap and immutable
release publication. Successful region-day partitions commit independently;
any failure blocks publication until a clean retry. PlumeGraph dbt uses
`tag:plumegraph,tag:events`.

## Paper-reproduction preflight

The two paper profiles are explicit assets and jobs; they are not added to the
TEMPO `ScopeSpec` factory and have no schedules.

Assets:

- `sun2025/repro/ops/source_inventory`
- `sun2025/repro/ops/source_preflight`
- `andreadis2025/repro/ops/source_inventory`
- `andreadis2025/repro/ops/source_preflight`

Jobs:

- `sun2025_repro_source_preflight`
- `andreadis2025_repro_source_preflight`
- `sun2025_repro_source_readiness`
- `andreadis2025_repro_source_readiness`

The standalone preflight jobs retain their existing behavior and accept a
prepared inventory. Each readiness job selects source inventory followed by
preflight, resolves metadata only, and permanently uses exact mode. Both
families persist contracts, requests, source objects, completeness, and the
preflight report, and create a `planned` production generation or `synthetic`
fixture generation only when ready. The jobs never download production
payloads and are intentionally unscheduled.
