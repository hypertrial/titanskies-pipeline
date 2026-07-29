# System overview

TitanSkies converts NASA TEMPO L3 NO₂ granules into regional hourly DuckDB
marts and native-grid latest observations for Canada, the United States, and
Mexico. Version `0.5.x` also collects revision-safe NASA SWOT RiverSP reach
observations and discharge estimates for bounded Sacramento, Rhine, and Murray
mainstem corridors. It ships `tempo:no2`, `tempo:no2_std`, and the explicit
`riverpulse:events` lane in one local warehouse.

```mermaid
flowchart LR
  cmr["NASA CMR metadata"]
  inventory["Granule inventory"]
  netcdf["Authenticated NetCDF download"]
  agg["Native-grid validation and regional aggregation"]
  duckdb["DuckDB regional history and grid latest"]
  dbt["dbt staging → marts → observability"]

  cmr --> inventory
  inventory --> netcdf
  netcdf --> agg
  agg --> duckdb
  duckdb --> dbt
```

Dagster owns execution and lineage. DuckDB owns durable local state. NetCDF
files remain operator-owned local artifacts and are pruned only after their
granules were processed successfully. dbt publishes regional, national,
latest, anomaly, data-quality, and public native-grid latest relations per
scope.

RiverPulse follows a parallel explicit path: a verified SWORD v17b archive
produces immutable reach/topology generations; Hydrocron discovery plans
deterministic reach/time requests; serial ingestion retains response snapshots
and every observation/discharge revision; dbt publishes the current revision,
complete revision history, and request/scientific-quality observability. It
does not add RiverPulse to the TEMPO scope factory.

See [Architecture](architecture.md),
[Choose a scope](../getting-started/choose-a-scope.md), and
[Scope and non-goals](scope-and-non-goals.md).
