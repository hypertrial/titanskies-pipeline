# Naming

Sources: `tempo`, `riverpulse`, `plumegraph`, `sun2025`, `andreadis2025`

Scopes: `no2` (NRT) and `no2_std` (standard V04)

RiverPulse scope: `events`

NRT schemas (`tempo_no2_*`):

- `tempo_no2_raw`
- `tempo_no2_ops`
- `tempo_no2_staging`
- `tempo_no2_intermediate`
- `tempo_no2_marts`
- `tempo_no2_observability`

Standard schemas (`tempo_no2_std_*`):

- `tempo_no2_std_raw`
- `tempo_no2_std_ops`
- `tempo_no2_std_staging`
- `tempo_no2_std_intermediate`
- `tempo_no2_std_marts`
- `tempo_no2_std_observability`

Asset keys follow `tempo/no2/<layer>/<entity>` and
`tempo/no2_std/<layer>/<entity>`. The NRT geography registry relation remains
`tempo_region_registry`; the standard mirror is `tempo_no2_std_region_registry`.

RiverPulse schemas use `riverpulse_events_{ops,raw,staging,intermediate,marts,observability}`.
Its explicit ingestion keys are
`riverpulse/events/ops/network_registry`,
`riverpulse/events/raw/source_inventory`, and
`riverpulse/events/raw/observations`. Shared warehouse metadata lives in
`titanskies_ops.warehouse_metadata`.

Paper profiles use scope `repro` and retain metadata only in
`sun2025_repro_ops` or `andreadis2025_repro_ops`. Their explicit asset keys
are `<profile>/repro/ops/source_inventory` and
`<profile>/repro/ops/source_preflight`.
