# Data dictionary

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. Start with
    [Query the warehouse](../guides/query-the-warehouse.md). Formal policy
    lives in [Data contracts](data-contracts.md).

The `tempo:no2` (NRT) and `tempo:no2_std` (standard V04) scopes publish
**parallel mart families** with the same grains and nearly identical column
shapes. NRT relations live under `tempo_no2_marts` /
`tempo_no2_observability`. Standard mirrors use the `tempo_no2_std_*` prefix
and schemas. Column guidance below applies to both families unless noted.
Quality thresholds differ by scope contract CSV; do not mix NRT and std rows
in one analysis without an explicit cross-scope design.

## RiverPulse public relations

RiverPulse is a separate science family under `riverpulse_events_marts`.

| Relation | Grain | Key analyst fields |
| --- | --- | --- |
| `riverpulse_events_reaches` | SWORD reach | `reach_id`, `basin_key`, EPSG:4326 `geometry_wkb`, centroid, topology, `network_version` |
| `riverpulse_events_observations` | stable SWOT observation, current revision | WSE/width/slope values and uncertainties, decoded reach-quality flags, discharge summary bits, provenance, per-measurement and overall readiness |
| `riverpulse_events_observation_revisions` | immutable observation revision | CRID, granule, source ingest/collection time, response checksum, snapshot IDs/URIs |
| `riverpulse_events_discharges` | current observation × algorithm × variant | normalized value/uncertainty/unit, quality, scale factor, variant, readiness |
| `riverpulse_events_discharge_revisions` | observation revision × algorithm × variant | complete discharge correction history |

`riverpulse_events_observability.riverpulse_events_request_health` is one row
per deterministic Hydrocron request.
`riverpulse_events_observability.riverpulse_events_scientific_quality_issues`
is one row per current observation × failed readiness reason. Issue rows do
not imply ingestion loss: source rows remain in revision history.

Observation marts preserve `reach_quality_bits`,
`unconstrained_discharge_quality_bits`, and
`constrained_discharge_quality_bits`. The twelve `has_*` columns decode the
official `reach_q_b` masks into named booleans without replacing the source
integer.

## Core semantics

- Prefer `is_analysis_ready` on hourly, country, anomaly, and grid marts when
  filtering measurements for analysis.
- `*_region_latest` **already filters** to analysis-ready, non-country regions
  and **does not expose** `is_analysis_ready`. Selecting that column from a
  latest mart is an error.
- Freshness (`stale_hours_warn` / `stale_hours_error`) is
  **observability-only** via `*_data_quality` (`issue_type = 'stale'`). It is
  not folded into `is_analysis_ready`.
- Administrative marts retain hourly history; `*_grid_latest` keeps latest
  cells only.
- Near-real-time observations are provisional research products, not personal
  exposure measurements.
- Registry FQNs differ by scope: NRT uses
  `tempo_no2_marts.tempo_region_registry`; standard uses
  `tempo_no2_std_marts.tempo_no2_std_region_registry`.

## `tempo_no2_region_hourly` / `tempo_no2_std_region_hourly`

| Guidance | Detail |
| --- | --- |
| Intended use | Area-weighted regional NO₂ history for counties, CSDs, municipalities, states/provinces, and similar administrative units |
| Grain | `canonical_region_id` × UTC `observation_hour` |
| Filters | Mart excludes `region_type = 'country'`. Prefer `is_analysis_ready` for analysis |
| Key fields | `no2_mean`, `no2_median`, `no2_p90`, `coverage_fraction`, `valid_area_km2`, `total_area_km2`, `source_granule_count`, `all_granules_validated`, `quality_flag_accepted`, `is_analysis_ready`, `revision`, `local_observation_hour`, `timezone` |
| Common mistakes | Including country rows from raw/intermediate instead of this mart; ignoring coverage; mixing NRT and std schemas; treating provisional NRT as settled standard |

Country totals belong in `*_country_hourly`. Median and p90 pool every valid
cell observation across every scan in the UTC hour. `revision` is monotonic
per region-hour replacement within a scope.

## `tempo_no2_region_latest` / `tempo_no2_std_region_latest`

| Guidance | Detail |
| --- | --- |
| Intended use | Latest trustworthy observation per non-country region |
| Grain | one row per `canonical_region_id` |
| Filters | Built from hourly rows where `is_analysis_ready` and `region_type != 'country'`. **No `is_analysis_ready` column** |
| Key fields | `latest_observation_hour`, `latest_no2_mean`, `latest_coverage_fraction`, `latest_valid_area_km2`, `latest_total_area_km2`, `data_age_hours`, `country_code`, `region_type` |
| Common mistakes | Filtering `is_analysis_ready` on this mart; expecting country rows; treating `data_age_hours` as an analysis-ready gate (use `*_data_quality` for stale) |

`data_age_hours` is informational age relative to query time. Stale severity
still comes from observability, not from this column alone.

## `tempo_no2_region_anomalies` / `tempo_no2_std_region_anomalies`

| Guidance | Detail |
| --- | --- |
| Intended use | Same-local-hour robust z-scores vs the prior baseline window |
| Grain | region × UTC hour (with IANA `local_observation_hour`) |
| Filters | Country IDs (`US`, `CA`, `MX`) excluded. Prefer `is_analysis_ready` |
| Key fields | `no2_mean`, `baseline_sample_count`, `baseline_median`, `baseline_mad`, `no2_difference`, `robust_z_score`, `is_analysis_ready`, `timezone` |
| Common mistakes | Interpreting null `robust_z_score` before baseline samples exist, when MAD is zero, or when the current row is not analysis-ready; comparing NRT anomalies to std baselines |

Baseline length and minimum samples come from the active scope contract
(`anomaly_baseline_days`, `anomaly_min_baseline_samples`).

## `tempo_no2_country_hourly` / `tempo_no2_std_country_hourly`

| Guidance | Detail |
| --- | --- |
| Intended use | National pixel-aggregate history plus subordinate analysis-ready region counts |
| Grain | `country_code` × UTC `observation_hour` |
| Filters | Prefer `is_analysis_ready` for national series used in analysis |
| Key fields | `no2_mean`, `no2_median`, `no2_p90`, `coverage_fraction`, `valid_pixel_count`, `source_granule_count`, `all_granules_validated`, `is_analysis_ready`, `analysis_ready_region_count`, `region_count` |
| Common mistakes | Joining country rows into `*_region_hourly` instead of using this mart; assuming `analysis_ready_region_count` equals every admin unit in the registry |

National aggregates come from `region_type = 'country'` source rows;
`analysis_ready_region_count` counts subordinate admin units that were
analysis-ready in the same hour.

## `tempo_no2_grid_latest` / `tempo_no2_std_grid_latest`

| Guidance | Detail |
| --- | --- |
| Intended use | Latest native 0.02° cell observation intersecting CA/US/MX |
| Grain | native TEMPO grid cell (`grid_row`, `grid_col`) — latest observation only |
| Filters | Prefer `is_analysis_ready` (`quality_flag_accepted and no2 is not null`) |
| Key fields | `latitude`, `longitude`, `cell_area_km2`, `observation_time`, `observation_hour`, `no2`, `quality_flag`, `quality_flag_accepted`, `granule_id`, `is_analysis_ready` |
| Common mistakes | Expecting pixel-level history; assuming WKT geometry columns; treating cell centers as polygon vertices without ±0.01° half-width |

Cell bounds are center coordinates ±0.01°. History is not retained in the
public grid mart.

## `tempo_region_registry` / `tempo_no2_std_region_registry`

| Guidance | Detail |
| --- | --- |
| Intended use | Canonical cross-country geography contract for all administrative marts in that scope |
| Grain | one row per `canonical_region_id` |
| FQN (NRT) | `tempo_no2_marts.tempo_region_registry` |
| FQN (std) | `tempo_no2_std_marts.tempo_no2_std_region_registry` |
| Key fields | `country_code`, `region_type`, `source_region_id`, `canonical_region_id`, `region_name`, `parent_region_id`, `timezone`, `geometry_version`, `geometry_checksum`, `loaded_at` |
| Common mistakes | Assuming both scopes share one registry relation name; editing Parquet registry files by hand; mixing geometry generations across checksums |

Both scopes load from the same pinned geography generation when registered,
but they publish **distinct** mart names. Always use the FQN for the scope
you are querying.

## Observability

### `tempo_no2_granule_observability` / `tempo_no2_std_granule_observability`

| Guidance | Detail |
| --- | --- |
| Intended use | Granule latency, checksum, and processing status for operators |
| Grain | one row per `granule_id` |
| Key fields | `acquisition_start`, `acquisition_end`, `discovered_at`, `downloaded_at`, `validated_at`, `processed_at`, `checksum_sha256`, `processing_status`, `error_message`, `processing_latency_minutes` |
| Common mistakes | Using discovery timestamps as acquisition time; treating this mart as an analysis API |

Prefer `acquisition_start` when ordering recent product acquisitions.

### `tempo_no2_data_quality` / `tempo_no2_std_data_quality`

| Guidance | Detail |
| --- | --- |
| Intended use | Explicit `zero_valid`, `low_coverage`, and `stale` issues by region and hour |
| Grain | issue row (`canonical_region_id` × `observation_hour` × `issue_type`) |
| Key fields | `issue_type`, `severity`, `message`, `coverage_fraction`, `valid_area_km2`, `total_area_km2` |
| Common mistakes | Treating advisory quality rows as dbt build failures; folding `stale` into `is_analysis_ready`; expecting latest marts to hide stale regions automatically |

Severity remains visible to operators. Environmental quality rows are
advisory and do not fail dbt builds. Freshness belongs here, not in the
analysis-ready flag.

## Standard (V04) notes

Standard-scope marts and observability appear after an **explicit** standard
discovery/ingest (and dbt) run. `make demo` remains NRT-only: it seeds both
contract CSVs but builds NRT marts; standard raw/ops schemas bootstrap empty
and std marts are not produced by the demo path.

Quality thresholds for the standard scope are governed independently by
`dbt/seeds/tempo_no2_std_contract.csv`. NRT uses
`dbt/seeds/tempo_no2_contract.csv`.

## Related pages

- [Data contracts](data-contracts.md) — formal analysis-ready and grain rules
- [Query recipes](../guides/query-recipes.md) — copy-paste SQL
- [Warehouse reference](warehouse.md) — schemas and local DuckDB layout
