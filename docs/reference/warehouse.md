# Warehouse reference

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs.

TitanSkies stores pipeline state and analytics in a local DuckDB file. The
default is `titanskies.duckdb`; `DUCKDB_PATH` can select another local path.
The repository does not publish or synchronize that file.

The operator controls the local DuckDB file; source and derived-data rights
remain governed by their source terms. No ownership of NASA observations,
boundary data, or ODbL-derived geography is transferred by local file control.

## Schema ownership

| Schema | Audience | Purpose |
| --- | --- | --- |
| `tempo_no2_raw` | Internal | Regional hourly aggregates and latest native-grid observations (NRT). |
| `titanskies_ops` | Operators | Shared `warehouse_metadata` schema-version stamp (`0.6.0`). |
| `tempo_no2_ops` | Operators | Granule inventory, geography registry, and durable pipeline state (NRT). |
| `tempo_no2_staging` | dbt internal | Typed source projections (NRT). |
| `tempo_no2_intermediate` | dbt internal | Reusable hourly and anomaly calculations (NRT). |
| `tempo_no2_marts` | Analysts | Six stable public relations documented in the data dictionary (NRT). |
| `tempo_no2_observability` | Operators and analysts | Granule health and explicit data-quality findings (NRT). |
| `tempo_no2_std_raw` | Internal | Regional hourly aggregates and latest native-grid observations (standard V04). |
| `tempo_no2_std_ops` | Operators | Granule inventory and geography registry (standard V04). |
| `tempo_no2_std_staging` | dbt internal | Typed source projections (standard V04). |
| `tempo_no2_std_intermediate` | dbt internal | Reusable hourly and anomaly calculations (standard V04). |
| `tempo_no2_std_marts` | Analysts | Standard-scope counterparts to the six NRT marts. |
| `tempo_no2_std_observability` | Operators and analysts | Granule health and explicit data-quality findings (standard V04). |
| `riverpulse_events_ops` | Operators | Network manifest, deterministic Hydrocron requests, and immutable snapshot ledger. |
| `riverpulse_events_raw` | Internal | SWORD reaches/edges, observation/discharge revisions, and snapshot links. |
| `riverpulse_events_staging` | dbt internal | Source-conformed RiverPulse projections and science contract. |
| `riverpulse_events_intermediate` | dbt internal | Deterministic current observation/discharge revisions. |
| `riverpulse_events_marts` | Analysts | Five stable reach, observation, revision, and discharge relations. |
| `riverpulse_events_observability` | Operators and analysts | Request health and row-level scientific-quality issues. |
| `plumegraph_events_ops` | Operators | Cohort, source request/snapshot, analysis generation, validation, and immutable-release ledgers. |
| `plumegraph_events_raw` | Internal | Facility regions, source revisions, episode revisions, scan-graph/lineage edges, evidence links, and provenance. |
| `plumegraph_events_staging` | dbt internal | Typed PlumeGraph source projections and science contract. |
| `plumegraph_events_intermediate` | dbt internal | Current source and promoted episode revisions. |
| `plumegraph_events_marts` | Analysts | Eight stable facility, episode, source-attribution, estimate, evidence, and provenance relations. |
| `plumegraph_events_observability` | Operators and analysts | Request, partition, revision, quality, benchmark, calibration, and release-integrity status. |

Query marts for analysis. Raw, ops, staging, and intermediate relations are
debugging and implementation surfaces rather than public data contracts. The
NRT and standard schemas are fully independent: no rows, sequences, or
watermarks are shared between them. All product lanes share only the schema
stamp in `titanskies_ops`.

## Storage and retention

Administrative marts retain hourly history. `tempo_no2_grid_latest` retains
only the latest supported-country observation for each native cell. Raw
NetCDF files live outside DuckDB under `TEMPO_NO2_RAW_DATA_DIR` (NRT) or
`TEMPO_NO2_STD_RAW_DATA_DIR` (standard) and are pruned only after successful
processing and each scope's configured retention interval. The granule
ledger remains available after file pruning.

RiverPulse response bodies are retained indefinitely below
`RIVERPULSE_RAW_DATA_DIR`; database paths are relative to that root. Network
manifests point to immutable Parquet generations by artifact-root-relative
path. No API key or signed URL is persisted.

PlumeGraph source subsets and responses are retained below
`PLUMEGRAPH_RAW_DATA_DIR`. Each response also has a checksum-addressed,
region-and-date-partitioned normalized Parquet artifact registered in
`plumegraph_events_ops.normalized_artifacts`; its registration, normalized
DuckDB rows, source snapshot, and request success commit together. Verified
releases are atomically published below
`PLUMEGRAPH_RELEASE_DIR`; each release pins the current analysis generation,
source snapshot checksums, science contract, algorithm, cohort, code version,
validation run, normalized artifacts, and sanitized source request/result
lineage. EPA API keys, signed URLs, and Earthdata credentials are never
stored.

DuckDB, WAL files, raw downloads, generated geography, dbt targets, and the
built documentation site are local artifacts and must not be committed.

## Trust and observability

Use `is_analysis_ready` before analysis. Inspect
`tempo_no2_observability.tempo_no2_granule_observability` for ingestion status
and `tempo_no2_observability.tempo_no2_data_quality` for zero-valid,
low-coverage, and stale observations. Environmental findings remain visible
without blocking publication; integrity failures still fail the dbt build.

See the [Data dictionary](data-dictionary.md) for relation grains and
[Data contracts](data-contracts.md) for formal guarantees.

For RiverPulse, use
`riverpulse_events_observability.riverpulse_events_request_health` for HTTP,
retry, latency, and actionable failures, and
`riverpulse_events_scientific_quality_issues` for preserved rows that fail a
measurement readiness rule.

For PlumeGraph, begin with
`plumegraph_events_observability.plumegraph_events_request_health`,
`plumegraph_events_partition_completeness`, and
`plumegraph_events_data_quality_issues`. Treat attribution as an auditable
hypothesis rather than proof or a regulatory conclusion; probability columns
remain null when held-out calibration does not pass.

TitanSkies is research and engineering software, not health, exposure,
medical, safety, or regulatory advice. Measurements are area/time aggregates,
not an individual's exposure, and near-real-time products are provisional.
