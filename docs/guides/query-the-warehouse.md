# Query the warehouse

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. Start here, then use
    [Query recipes](query-recipes.md), the
    [Data dictionary](../reference/data-dictionary.md), and
    [Data contracts](../reference/data-contracts.md).

Connect DuckDB to the path printed by `make demo`, or to `DUCKDB_PATH` for a
configured warehouse. Query public relations in `tempo_no2_marts`; use
`tempo_no2_observability` to decide whether data is ready to trust. The
standard scope mirrors the same shapes under `tempo_no2_std_marts` /
`tempo_no2_std_observability`.

## Table chooser

| Goal | Relation |
| --- | --- |
| Latest region snapshot | `tempo_no2_marts.tempo_no2_region_latest` |
| Regional hourly history | `tempo_no2_marts.tempo_no2_region_hourly` |
| Country hourly history | `tempo_no2_marts.tempo_no2_country_hourly` |
| Same-local-hour anomalies | `tempo_no2_marts.tempo_no2_region_anomalies` |
| Native-grid latest cells | `tempo_no2_marts.tempo_no2_grid_latest` |
| Geography contract | `tempo_no2_marts.tempo_region_registry` |
| Granule / quality diagnosis | `tempo_no2_observability.*` |

## Trust rules

- Filter measurements with `is_analysis_ready` for analysis-ready rows.
- Near-real-time observations are provisional; TitanSkies is not health or
  personal-exposure advice.
- Prefer observability models when marts look empty or stale.
- Do not treat `tempo_no2_ops` or staging as APIs.

Continue with [Query recipes](query-recipes.md).
