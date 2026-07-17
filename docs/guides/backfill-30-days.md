# Backfill 30 days

Launch `tempo_no2_full_pipeline` with this Dagster run config:

```yaml
ops:
  tempo__no2__raw__granule_inventory:
    config:
      lookback_hours: 720
  tempo__no2__raw__region_hour_aggregates:
    config:
      max_granules: null
```

Backfill is operator-driven in v0.3.0; the repo ships the capability, not a
hosted historical warehouse. Administrative marts retain hourly history;
native-grid output remains latest-only.

Failed granules make the run fail after the batch finishes. They remain in the
ledger and are downloaded again on the next run.
