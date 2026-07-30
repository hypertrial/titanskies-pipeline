# Validate and recover

## Validation checklist

1. Query `tempo_no2_observability.tempo_no2_granule_observability` for recent
   processing status.
2. Confirm analysis-ready rows exist in the mart you care about.
3. For live operators, run
   `uv run python scripts/run_live_smoke.py --mode discovery` before enabling
   schedules.
4. Use `make live-smoke` only with operator-owned Earthdata credentials.
5. For RiverPulse, inspect
   `riverpulse_events_observability.riverpulse_events_request_health` and
   `riverpulse_events_scientific_quality_issues`, then verify every public
   observation has a snapshot checksum and artifact URI.
6. For PlumeGraph, inspect `plumegraph_events_request_health`,
   `plumegraph_events_partition_completeness`,
   `plumegraph_events_calibration_state`, and
   `plumegraph_events_release_integrity`; verify the release manifest before
   distributing any evidence bundle.
7. For paper readiness, inspect the resolver exit status and every source
   `resolution_outcome`. A `definitively_unavailable` result documents a
   completed investigation but must not create a `planned` generation.

## Recovery

1. Stop Dagster and any DuckDB clients before warehouse recovery.
2. Preserve the raw download directory and reviewed geography source cache.
3. Move a corrupt database and its WAL files out of the working path.
4. Initialize a clean warehouse and rerun ingestion/dbt. Never copy partially
   derived tables into the replacement.
5. Populated older major derived warehouses intentionally fail at startup;
   follow the matching upgrade guide under Get started.

Failed granules are recorded in `tempo_no2_ops.granule_inventory`. Correct
authentication, network, NetCDF, or geography issues and rerun the hourly job;
failed rows are automatically reselected and redownloaded.

Failed Hydrocron work is recorded in
`riverpulse_events_ops.source_requests`. Successful siblings and immutable
snapshots remain committed. Correct timeouts, throttling, field drift, or
network/version issues and rerun the observation-ingest/full job. Never delete
revision rows to force a retry. Requests left `running` by an interrupted
worker become retryable after one hour. A SWORD version change with
observations, or any v0.4→v0.5 upgrade, requires a new warehouse; raw response
snapshots and verified source caches may be retained.

Failed PlumeGraph work is recorded in `plumegraph_events_ops.source_requests`
or `analysis_runs`. Retry failed requests/partitions; do not delete successful
siblings, source revisions, or an older release. A contract, algorithm, or
source-manifest correction creates new deterministic partitions and revisions.
HRRR current-revision selection uses authoritative source revision time before
local collection time. Release generation refuses stale validation; rerun
validation after any generation promotion instead of reusing an older passing
run. A loaded benchmark version is immutable, so corrected annotations require
a new benchmark version.
Populated v0.6 warehouses require the v0.7 clean rebuild; verified immutable
source caches may be retained.

Readiness inventories are atomically replaced, so an interrupted writer leaves
the previous complete inventory intact. On `transient_error`, retain successful
sibling evidence, correct provider access, and rerun the readiness job. On
`operator_input_required`, place the checksum-matching export under the
configured import directory; never copy credentials, `.cdsapirc`, signed URLs,
or payloads into that directory. Do not edit a definitive-unavailable outcome
to `resolved`; replace it only with new immutable provider evidence. Repeated
identical resolution/preflight input reuses the same object revisions, run,
and acquisition-generation identities.

See [Troubleshooting](troubleshooting.md) for common failure modes.
