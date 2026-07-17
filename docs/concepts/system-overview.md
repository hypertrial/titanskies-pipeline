# System overview

TitanSkies converts NASA TEMPO L3 NRT NO₂ granules into regional hourly DuckDB
marts for Canada, the United States, and Mexico.

```text
NASA CMR metadata
  -> granule inventory
  -> authenticated NetCDF download
  -> native-grid validation and equal-area regional aggregation
  -> atomic DuckDB regional history and native-grid latest rows
  -> dbt staging, intermediate models, marts, and observability
```

Dagster owns execution and lineage. DuckDB owns durable local state. NetCDF
files remain operator-owned local artifacts and are pruned only after their
granules were processed successfully. dbt publishes regional, national,
latest, anomaly, data-quality, and public native-grid latest relations.
