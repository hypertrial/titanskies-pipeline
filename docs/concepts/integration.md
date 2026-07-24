# Integration

## Allowed inputs

| Input | Notes |
| --- | --- |
| `tempo_no2_marts.*` / `tempo_no2_std_marts.*` | Prefer `is_analysis_ready` |
| DuckDB `COPY` exports | Operator-controlled CSV/Parquet snapshots |

## Do not treat as APIs

- `tempo_no2_ops` / `tempo_no2_std_ops` ledgers except for operator diagnosis
- Staging and intermediate dbt schemas
- Raw NetCDF paths as a public contract

## Versioning

Track
[CHANGELOG.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/CHANGELOG.md).
Derived warehouses may require clean rebuilds between releases.

See [Integrators](../audiences/integrators.md).
