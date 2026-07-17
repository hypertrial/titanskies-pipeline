# Data contracts

`dbt/seeds/tempo_no2_contract.csv` contains exactly one `default` row and is the
single quality-policy source for Python ingestion and dbt.

| Field | Meaning |
| --- | --- |
| `contract_version` | Incremental-model invalidation version |
| `min_region_coverage` | Minimum valid grid coverage for analysis-ready rows |
| `stale_hours_warn` | Age at which freshness becomes a warning |
| `stale_hours_error` | Age at which freshness becomes an error |
| `anomaly_baseline_days` | Prior same-local-hour baseline window |
| `anomaly_min_baseline_samples` | Required prior same-local-hour observations |
| `accepted_quality_flags` | Pipe-separated TEMPO flags accepted by aggregation |

Changes require dbt unit and golden tests plus an Unreleased changelog entry.
Do not add environment overrides: differing runtime and warehouse policy would
make a row appear accepted by one layer and rejected by another.

Public grains are:

- region-hour for `tempo_no2_region_hourly`;
- one row per region for `tempo_no2_region_latest`;
- country-hour for `tempo_no2_country_hourly`;
- region-hour for `tempo_no2_region_anomalies` and data-quality issues.
- native grid cell for `tempo_no2_grid_latest`, with latest observation only.

The v0.3 TEMPO grid contract has 2,950 latitude centers from 14.01° to 72.99° and
7,750 longitude centers from −167.99° to −13.01°, both at 0.02° spacing.
Ingestion rejects files whose coordinates do not match this contract.

Raw regional grain is exactly region × UTC hour. Every valid area-weighted
cell observation from all scans in that hour participates in mean, median, and
p90; overlap area is repeated once per scan. `source_granule_count`,
`all_granules_validated`, and monotonic `revision` describe each replacement.

Anomalies compare an analysis-ready row with prior analysis-ready rows from the
same IANA local hour during the preceding 28 days. The score is null until
seven prior observations exist, when baseline MAD is zero, or when the current
row is not analysis-ready.
