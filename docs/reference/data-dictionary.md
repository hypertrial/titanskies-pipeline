# Data dictionary

## `tempo_no2_region_hourly`

Grain: region × observation hour

Includes area-weighted `no2_mean`, `no2_median`, `no2_p90`,
`valid_area_km2`, `total_area_km2`, area-derived `coverage_fraction`, and
`is_analysis_ready`, exact `source_granule_count`, and
`all_granules_validated`. Country rows are excluded. Median and p90 pool every
valid cell observation across every scan in the UTC hour.

## `tempo_no2_region_latest`

Grain: region

Latest trustworthy observation with valid/total area, coverage, and
`data_age_hours`.

## `tempo_no2_region_anomalies`

Grain: region × hour

`timezone`, `local_observation_hour`, `baseline_sample_count`,
`baseline_mad`, `no2_difference`, and `robust_z_score` against the prior 28
days at the same IANA local hour.

## `tempo_no2_country_hourly`

Grain: country × hour

National statistics computed directly from country-level pixel aggregates.
Finest-level rows contribute only `region_count` and
`analysis_ready_region_count`.

## `tempo_no2_grid_latest`

Grain: native TEMPO grid cell

Latest observation only for cells intersecting Canada, the United States, or
Mexico. Includes grid row/column, center coordinates, cell area, observation,
quality/readiness fields, granule ID, and ingestion time. Cell bounds are the
center coordinates ±0.01°.

## `tempo_region_registry`

Canonical cross-country geography contract.

## `tempo_no2_granule_observability`

Granule latency, checksum, and processing status.

## `tempo_no2_data_quality`

Explicit `zero_valid`, `low_coverage`, and `stale` issues by region and hour.
Severity remains visible to operators but environmental quality rows are
advisory and do not fail dbt builds.

## Standard (V04) marts

The `tempo:no2_std` scope publishes the identical mart shapes under a
`tempo_no2_std_*` prefix, backed by the standard TEMPO_NO2_L3 V04 product
(CMR `C3685896708-LARC_CLOUD`, DOI `10.5067/IS-40E/TEMPO/NO2_L3.004`). Column
definitions and grains are the same as their NRT counterparts above; only the
model/schema names and the source collection differ:

- `tempo_no2_std_region_hourly`
- `tempo_no2_std_region_latest`
- `tempo_no2_std_region_anomalies`
- `tempo_no2_std_country_hourly`
- `tempo_no2_std_grid_latest`
- `tempo_no2_std_region_registry`
- `tempo_no2_std_granule_observability`
- `tempo_no2_std_data_quality`

Quality thresholds for the standard scope are governed independently by
`dbt/seeds/tempo_no2_std_contract.csv`.
