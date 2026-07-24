# Data contracts

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs.

`dbt/seeds/tempo_no2_contract.csv` contains exactly one `default` row and is the
single quality-policy source for Python ingestion and dbt for the `tempo:no2`
(NRT) scope. `dbt/seeds/tempo_no2_std_contract.csv` is the equivalent,
independently versioned contract for the `tempo:no2_std` (standard) scope.
Both contracts share the same shape:

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
make a row appear accepted by one layer and rejected by another. Each scope's
contract is versioned and invalidated independently.

## Public mart grains

Identical for the `tempo_no2_std_*` mart family:

| Relation (NRT / std) | Grain |
| --- | --- |
| `tempo_no2_region_hourly` / `tempo_no2_std_region_hourly` | region × UTC hour |
| `tempo_no2_region_latest` / `tempo_no2_std_region_latest` | region |
| `tempo_no2_country_hourly` / `tempo_no2_std_country_hourly` | country × UTC hour |
| `tempo_no2_region_anomalies` / `tempo_no2_std_region_anomalies` | region × hour |
| `tempo_no2_grid_latest` / `tempo_no2_std_grid_latest` | native grid cell, latest observation only |
| `tempo_region_registry` / `tempo_no2_std_region_registry` | canonical geography contract |

FQNs: NRT registry is `tempo_no2_marts.tempo_region_registry`; standard
registry is `tempo_no2_std_marts.tempo_no2_std_region_registry`.

## Grid geometry contract

The v0.3+ TEMPO grid contract has 2,950 latitude centers from 14.01° to 72.99°
and 7,750 longitude centers from −167.99° to −13.01°, both at 0.02° spacing.
Ingestion rejects files whose coordinates do not match this contract.

## Regional aggregation rules

Raw regional grain is exactly region × UTC hour. Every valid area-weighted
cell observation from all scans in that hour participates in mean, median, and
p90; overlap area is repeated once per scan. `source_granule_count`,
`all_granules_validated`, and monotonic `revision` describe each replacement.

## Anomaly rules

Anomalies compare an analysis-ready row with prior analysis-ready rows from the
same IANA local hour during the preceding baseline window. The score is null
until the minimum prior observations exist, when baseline MAD is zero, or when
the current row is not analysis-ready.

## Analysis-ready rule

For regional/country hourly rows, `is_analysis_ready` is true when
`quality_flag_accepted`, `all_granules_validated`, and
`coverage_fraction >= min_region_coverage` (from the active scope contract).
Native-grid latest rows use `quality_flag_accepted and no2 is not null`.

Freshness (`stale_hours_warn` / `stale_hours_error`) is reported only in
observability / `*_data_quality` (`issue_type = 'stale'`). It is **not**
folded into `is_analysis_ready`.

Prefer the flag in analyst queries on hourly, country, anomaly, and grid
marts. `*_region_latest` marts already filter to analysis-ready non-country
regions and **do not expose** `is_analysis_ready` — do not select or filter
that column on a latest mart.
