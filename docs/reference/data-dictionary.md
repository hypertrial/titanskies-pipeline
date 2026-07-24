# Data dictionary

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. Start with
    [Query the warehouse](../guides/query-the-warehouse.md). Formal policy
    lives in [Data contracts](data-contracts.md).

The `tempo:no2_std` scope publishes identical shapes under the
`tempo_no2_std_*` prefix. Column guidance below applies to both families unless
noted.

## Core semantics

- Prefer `is_analysis_ready` for analysis-ready measurements.
- Administrative marts retain hourly history; `*_grid_latest` keeps latest cells
  only.
- Near-real-time observations are provisional research products, not personal
  exposure measurements.

## `tempo_no2_region_hourly`

| Guidance | Detail |
| --- | --- |
| Intended use | Area-weighted regional NO₂ history |
| Grain | region × observation hour |
| Key fields | `no2_mean`, `no2_median`, `no2_p90`, `coverage_fraction`, `is_analysis_ready`, `source_granule_count`, `all_granules_validated` |
| Common mistakes | Including country rows; ignoring coverage; mixing NRT and std schemas |

Country rows are excluded. Median and p90 pool every valid cell observation
across every scan in the UTC hour.

## `tempo_no2_region_latest`

| Guidance | Detail |
| --- | --- |
| Intended use | Latest trustworthy observation per region |
| Grain | region |
| Key fields | latest NO₂ stats, coverage, `data_age_hours` |

## `tempo_no2_region_anomalies`

| Guidance | Detail |
| --- | --- |
| Intended use | Same-local-hour robust z-scores vs prior 28 days |
| Grain | region × hour |
| Common mistakes | Interpreting null scores before baseline samples exist |

## `tempo_no2_country_hourly`

| Guidance | Detail |
| --- | --- |
| Intended use | National pixel-aggregate history |
| Grain | country × hour |

## `tempo_no2_grid_latest`

| Guidance | Detail |
| --- | --- |
| Intended use | Latest native 0.02° cell observation |
| Grain | native TEMPO grid cell intersecting CA/US/MX |
| Common mistakes | Expecting pixel-level history or WKT geometry columns |

Cell bounds are center coordinates ±0.01°.

## `tempo_region_registry`

Canonical cross-country geography contract used by all administrative marts.

## Observability

### `tempo_no2_granule_observability`

Granule latency, checksum, and processing status.

### `tempo_no2_data_quality`

Explicit `zero_valid`, `low_coverage`, and `stale` issues by region and hour.
Severity remains visible to operators but environmental quality rows are
advisory and do not fail dbt builds.

## Standard (V04) marts

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
