# Upgrade to v0.7

TitanSkies v0.7 adds two paper-reproduction preflight ledgers and a clean
warehouse boundary. A populated v0.6 DuckDB file is deliberately rejected;
there is no migration or compatibility shim.

1. Stop every Dagster writer and retain the v0.6 database as rollback.
2. Point `DUCKDB_PATH` at a new file and run the normal bootstrap/dbt build.
3. Reuse only checksum-verified source caches; do not copy derived v0.6 tables.
4. Run `make demo`, `make riverpulse-demo`, and `make plumegraph-demo`.
5. Run `make sun2025-preflight` and `make andreadis2025-preflight` to verify
   the offline contract path.
6. Before production acquisition, replace the synthetic inventories with
   provider-catalog exports, review every exactness blocker, and set explicit
   object/storage budgets.

The new ops schemas are `sun2025_repro_ops` and
`andreadis2025_repro_ops`. They contain source contracts, planned requests,
preflight runs, per-source completeness, source-object revisions, and planned
or synthetic acquisition generations. No schedule or production download is
enabled by this release.
