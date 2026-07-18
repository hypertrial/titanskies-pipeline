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
dbt publication once per hour. The manual `tempo_no2_hourly_ingest` job remains
processing-only and accepts optional `max_granules`. Geography bootstrap is an
explicit operator action and is excluded from recurring selections.

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
