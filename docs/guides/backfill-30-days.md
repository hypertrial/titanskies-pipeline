# Chunked backfill (30 days)

Backfill is operator-driven; the repo ships the capability, not a hosted
historical warehouse. Administrative marts retain hourly history; native-grid
output remains latest-only. Failed granules make a run fail after the batch
finishes. They remain in the granule ledger and are downloaded again on the
next run.

Both scopes support two backfill styles on the `*_granule_discovery` job's
granule-inventory op config:

- **Lookback-based**: `lookback_hours` counts back from "now". Simple, but a
  single very large lookback (for example 720 hours) issues one large CMR
  query and one large batch of downloads.
- **Explicit window**: `window_start_utc` / `window_end_utc` (ISO 8601, UTC)
  bound an exact discovery range and override `lookback_hours` when set. Use
  this to backfill in smaller, resumable chunks, which keeps individual CMR
  queries and download batches small and makes a failed chunk easy to retry
  without re-running the whole 30 days.

## NRT (`tempo:no2`)

Single-shot lookback backfill of the trailing 30 days:

```yaml
ops:
  tempo__no2__raw__granule_inventory:
    config:
      lookback_hours: 720
  tempo__no2__raw__region_hour_aggregates:
    config:
      max_granules: null
```

Chunked backfill (recommended for 30 days): run `tempo_no2_full_pipeline`
once per multi-day window, advancing `window_start_utc`/`window_end_utc`
each time, for example in 5-day chunks:

```yaml
ops:
  tempo__no2__raw__granule_inventory:
    config:
      window_start_utc: "2026-06-01T00:00:00"
      window_end_utc: "2026-06-06T00:00:00"
  tempo__no2__raw__region_hour_aggregates:
    config:
      max_granules: null
```

Repeat with the next non-overlapping (or slightly overlapping, since
discovery upserts are idempotent) window until the full 30-day range is
covered.

## Standard (`tempo:no2_std`, V04)

The standard scope uses the same run-config shape under
`tempo__no2_std__raw__granule_inventory` and
`tempo__no2_std__raw__region_hour_aggregates`. Because standard granules
settle more slowly than NRT, prefer smaller chunks (for example 3-5 days) and
expect discovery to surface revised granules for windows you already
backfilled:

```yaml
ops:
  tempo__no2_std__raw__granule_inventory:
    config:
      window_start_utc: "2026-06-01T00:00:00"
      window_end_utc: "2026-06-04T00:00:00"
  tempo__no2_std__raw__region_hour_aggregates:
    config:
      max_granules: null
```

Launch these run configs against `tempo_no2_std_full_pipeline`.

## Notes

- `window_start_utc` and `window_end_utc` must both be set together; setting
  only one is a config validation error.
- When both a window and `lookback_hours` are present, the explicit window
  wins.
- Each scope's backfill is fully independent: chunking or retrying NRT does
  not affect standard-scope state, and vice versa.
