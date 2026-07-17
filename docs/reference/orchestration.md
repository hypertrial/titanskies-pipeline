# Orchestration

Assets:

- `tempo/no2/ops/region_registry`
- `tempo/no2/raw/granule_inventory`
- `tempo/no2/raw/region_hour_aggregates`
- `titanskies_dbt`

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
