# Design decisions

## Local-first warehouse, no hosted API

**Why:** Operators control DuckDB files, NetCDF caches, and redistribution
under source terms.

## Dual independent scopes

**Why:** NRT and standard collections differ in product identity and readiness.
Separate jobs, schemas, and contract seeds prevent silent cross-contamination.

## Rebuild derived warehouses instead of migrating

**Why:** Geometry and contract versions invalidate region identities. Clean
rebuilds are safer than dual-layout compatibility layers.

## Latest-only native-grid mart

**Why:** Pixel-level history is out of scope; administrative history carries
the durable time series.

## Contract CSV is the sole quality policy

**Why:** Python and dbt must share one threshold source. Environment variables
configure operations, not competing quality rules.
