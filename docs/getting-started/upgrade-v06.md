# Upgrade to v0.6

TitanSkies v0.6 introduces a clean warehouse boundary for
`plumegraph:events`. A populated v0.5 DuckDB file is deliberately rejected;
there is no migration or compatibility shim.

1. Stop every Dagster writer and retain the v0.5 database as rollback.
2. Create a new warehouse path and run the v0.6 bootstrap and dbt build.
3. Reuse only verified immutable source caches. Do not copy derived v0.5
   tables into the new file.
4. Run `make demo`, `make riverpulse-demo`, and `make plumegraph-demo`.
5. Before live PlumeGraph work, install `--extra plumegraph`, approve the
   frozen 75-facility cohort, load the 200-window benchmark, and keep
   `PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED=false`.
6. Complete one facility/day smoke, the 2024 backfill, an immediate
   idempotent rerun, observability review, and release verification before
   enabling recurring collection.

The shared version stamp is `titanskies_ops.warehouse_metadata`. Raw TEMPO,
Hydrocron, HRRR, CAMD, and verified geography/network archives remain
operator-controlled and may be reused when their checksums still match.
