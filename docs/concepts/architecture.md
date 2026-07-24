# Architecture

Stack: Dagster, earthaccess, xarray, DuckDB, dbt.

TitanSkies runs **two independent TEMPO NO2 scopes** in one local warehouse:
`tempo:no2` (NRT) and `tempo:no2_std` (standard V04). Each scope has its own
CMR concept ID, raw directory, ops ledger, hour-revision sequence, quality
contract CSV, Dagster jobs/schedule flag, and published mart/observability
schemas. They share pinned geography artifacts when both registries are
materialized, but they never share raw epochs or incremental contract
versions.

```mermaid
flowchart TB
  subgraph shared["Shared local host"]
    geo["Pinned geography manifest + overlap weights"]
    duck["One DuckDB warehouse path"]
  end

  subgraph nrt["tempo:no2 NRT lane"]
    nrtDisc["CMR discovery NRT"]
    nrtLedger["tempo_no2_ops.granule_inventory"]
    nrtIngest["Hourly ingest NRT"]
    nrtRaw["tempo_no2_raw aggregates + grid"]
    nrtDbt["dbt tag:tempo,tag:no2"]
    nrtMarts["tempo_no2_marts + tempo_no2_observability"]
    nrtDisc --> nrtLedger --> nrtIngest --> nrtRaw --> nrtDbt --> nrtMarts
  end

  subgraph std["tempo:no2_std standard lane"]
    stdDisc["CMR discovery std"]
    stdLedger["tempo_no2_std_ops.granule_inventory"]
    stdIngest["Hourly ingest std"]
    stdRaw["tempo_no2_std_raw aggregates + grid"]
    stdDbt["dbt tag:tempo,tag:no2_std"]
    stdMarts["tempo_no2_std_marts + tempo_no2_std_observability"]
    stdDisc --> stdLedger --> stdIngest --> stdRaw --> stdDbt --> stdMarts
  end

  geo --> nrtIngest
  geo --> stdIngest
  nrtRaw --> duck
  stdRaw --> duck
  nrtMarts --> duck
  stdMarts --> duck
```

## Per-granule ingest path

Within a scope, CMR discovery upserts immutable granule identities into that
scope's ops ledger. Hourly ingestion downloads each pending or failed granule,
validates the NetCDF layout, computes weighted regional statistics, and writes
idempotent regional aggregates. The latest supported native-grid cells,
regional aggregates, and processed-ledger success commit in one transaction
per granule; failure rolls all three back before its error is recorded
separately. A failed batch finishes the remaining work, records every error,
then fails the Dagster asset.

Failed files are deleted so the next run downloads a clean copy. Successful
retries clear the prior error. This favors correctness over bandwidth; the
ledger remains the source for attempt status and recovery details.

NetCDF files remain under the configured raw data directory for that scope.
Before ingestion, files for successfully processed granules older than the
scope retention window are deleted, and only their `local_path` is cleared.
Checksums, sizes, timestamps, raw aggregates, and processed ledger history
remain in DuckDB.

## Contracts and publication

Each scope has a sole quality contract CSV. Python reads accepted quality
flags before aggregation, while dbt reads the same row for coverage,
freshness, and anomaly policy. Environment variables configure operations, not
competing quality thresholds. Freshness is observability-only;
`is_analysis_ready` does not encode stale age. `*_region_latest` pre-filters
analysis-ready non-country regions and does not expose `is_analysis_ready`.

Production overlap weights stay as sorted, compressed Parquet and are loaded
once as columnar arrays for an ingestion batch. DuckDB stores a small artifact
manifest rather than duplicating the weights. Administrative marts retain
hourly history; the public native-grid mart intentionally retains only the
latest cell observation.

`make demo` exercises the NRT lane only. Standard marts appear after an
explicit standard discovery/ingest and dbt run.

See [Orchestration](../reference/orchestration.md) and
[TEMPO product notes](tempo-product-notes.md).
