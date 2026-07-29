# Upgrade to v0.5

Version 0.5 is a deliberate clean warehouse boundary. It adds
`riverpulse:events`, moves the shared schema stamp to
`titanskies_ops.warehouse_metadata`, and does not include a compatibility
migration.

1. Stop Dagster, dbt, and every writable DuckDB client.
2. Back up the v0.4 DuckDB file and its WAL files. Keep it unchanged for
   rollback.
3. Preserve operator-owned raw NetCDF, verified geography source caches, and
   verified SWORD archives. Do not copy derived tables into v0.5.
4. Point `DUCKDB_PATH` at a new file and initialize TitanSkies.
5. Register TEMPO geography and, when operating RiverPulse, build/register the
   pinned SWORD v17b network.
6. Run `make demo` and `make riverpulse-demo`.
7. Run one bounded live smoke, the RiverPulse science-phase backfill from
   `2023-08-01T00:00:00Z`, and an immediate idempotent rerun.
8. Review request/science observability before enabling any schedule.

A populated v0.4 warehouse fails at startup with clean-rebuild guidance. To
roll back, stop writers and restore the untouched v0.4 configuration and
database; never point v0.4 code at the v0.5 file.
