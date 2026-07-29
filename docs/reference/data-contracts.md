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

## RiverPulse science contract

`dbt/seeds/riverpulse_events_contract.csv` is the sole policy source for
`riverpulse:events`.

| Field | Meaning |
| --- | --- |
| `contract_version` | Incremental-model invalidation version |
| `field_contract_version` | Deterministic Hydrocron request field-set version |
| `collection_name` / `collection_version` | Pinned `SWOT_L2_HR_RiverSP_reach_D` / `D` |
| `sword_version` | Pinned network version `17b` |
| `accepted_reach_quality` | Official good reach classification (`0`) |
| `accepted_discharge_quality` | Official good discharge classification (`0`) |

Every parseable source row is retained. `is_wse_ready`, `is_width_ready`, and
`is_slope_ready` each require matching collection/network versions, good reach
quality, and finite value plus uncertainty. `is_analysis_ready` requires all
three. `is_discharge_ready` applies the version, official discharge quality,
and finite value/uncertainty rule independently for each algorithm/variant.
The raw and mart contracts also preserve `dschg_q_b` and `dschg_gq_b` as
unconstrained/constrained summary bit fields and publish the named
`reach_q_b` masks as `has_*` booleans.

Stable observation identity is SHA-256 of collection family, reach ID,
observation time, cycle, and pass. Revision identity adds CRID, granule
identity, and canonical record content. Current selection orders by source
`ingest_time`, local snapshot collection time, then deterministic revision ID.

| Relation | Grain |
| --- | --- |
| `riverpulse_events_reaches` | registered SWORD reach |
| `riverpulse_events_observations` | stable observation (current revision) |
| `riverpulse_events_observation_revisions` | observation revision |
| `riverpulse_events_discharges` | current observation × algorithm × variant |
| `riverpulse_events_discharge_revisions` | observation revision × algorithm × variant |

## PlumeGraph science contract

`dbt/seeds/plumegraph_events_contract.csv` is the sole policy source for
`plumegraph:events`. It pins the TEMPO L2 V04 collection/concept, algorithm
version, 100 km AOI, pixel readiness, background/detection thresholds,
tracking gap, candidate classification, calibration, and complete
wind/lifetime/conversion ensemble. Environment variables cannot override
these scientific values.

A pixel is analysis-ready only when `main_data_quality_flag = 0`, effective
cloud fraction is below `0.1`, VCD and positive uncertainty are finite, WKB
geometry is valid, collection version matches, and provenance is complete.
Negative VCD values remain valid inputs to background estimation. Detection
uses a median/MAD background from at least 30 eligible upwind pixels,
dual 3×MAD and 2×combined-uncertainty seeding, native-grid connectivity, and
a three-pixel minimum.

Stable pixel, CEMS-hour, analysis-partition, and episode-revision identities
are SHA-256 hashes over the documented canonical inputs. Analysis partition
identity includes the contract and algorithm versions; episode revision
identity includes its ordered candidate, retained scan-edge, and pixel
identities. Source corrections append revisions. A region-date generation
becomes current only after its transaction succeeds; immutable releases pin
exact generations, snapshots, and normalized artifacts.
The three-hour boundary overlap consults the immediately preceding promoted
partition for conservative lineage matching. The current episode mart then
selects exactly one latest promoted revision per stable plume lineage; all
partition-specific revisions remain in the history and evidence marts.

Candidate score weights are trajectory `0.40`, concurrent CAMD `0.30`,
distance `0.20`, and annual-emissions prior `0.10`. Missing concurrent CAMD
prevents probability readiness. When held-out ECE exceeds `0.10`, ranks remain
published while probabilities are null and classification abstains.

| Relation | Grain |
| --- | --- |
| `plumegraph_events_facilities` | cohort version × facility |
| `plumegraph_events_episodes` | stable plume lineage, current complete revision |
| `plumegraph_events_episode_revisions` | episode revision |
| `plumegraph_events_episode_geometries` | episode revision × observation time |
| `plumegraph_events_candidate_sources` | episode revision × facility |
| `plumegraph_events_emission_estimates` | episode revision × sensitivity variant |
| `plumegraph_events_evidence_pixels` | episode revision × pixel revision × evidence role |
| `plumegraph_events_provenance` | episode revision × source snapshot × input identity |

Evidence format `plumegraph-evidence-v1` includes GeoParquet, Parquet,
per-revision JSON with geometries, candidates, estimates, evidence pixels, and
provenance, plus a checksum manifest. Attribution is evidence, not proof, a
health claim, or a regulatory conclusion.
