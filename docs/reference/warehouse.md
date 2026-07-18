# Warehouse reference

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
| `tempo_no2_ops` | Operators | Granule inventory, geography registry, and durable pipeline state (NRT). Also holds the shared `warehouse_metadata` schema-version stamp. |
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

Query marts for analysis. Raw, ops, staging, and intermediate relations are
debugging and implementation surfaces rather than public data contracts. The
NRT and standard schemas are fully independent: no rows, sequences, or
watermarks are shared between them except the single `warehouse_metadata`
schema-version row in `tempo_no2_ops`.

## Storage and retention

Administrative marts retain hourly history. `tempo_no2_grid_latest` retains
only the latest supported-country observation for each native cell. Raw
NetCDF files live outside DuckDB under `TEMPO_NO2_RAW_DATA_DIR` (NRT) or
`TEMPO_NO2_STD_RAW_DATA_DIR` (standard) and are pruned only after successful
processing and each scope's configured retention interval. The granule
ledger remains available after file pruning.

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

TitanSkies is research and engineering software, not health, exposure,
medical, safety, or regulatory advice. Measurements are area/time aggregates,
not an individual's exposure, and near-real-time products are provisional.
