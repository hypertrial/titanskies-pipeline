# Choose a scope

TitanSkies ships four first-class collection scopes plus two unscheduled
paper-source preflight profiles in one warehouse.

| Scope | Product | Schemas | When to use |
| --- | --- | --- | --- |
| `tempo:no2` | TEMPO NO₂ L3 NRT | `tempo_no2_*` | Near-real-time monitoring; `make demo` builds this scope |
| `tempo:no2_std` | TEMPO NO₂ L3 V04 standard | `tempo_no2_std_*` | Standard collection; schemas bootstrap empty until an explicit std run |
| `riverpulse:events` | SWOT RiverSP reach Version D + SWORD v17b | `riverpulse_events_*` | River observations, discharges, topology, revisions, and provenance; `make riverpulse-demo` |
| `plumegraph:events` | TEMPO L2 V04 + HRRR + EPA CAMD | `plumegraph_events_*` | Auditable 2024 power-plant plume episodes, source hypotheses, estimates, validation, and immutable local releases; `make plumegraph-demo` |

Jobs and schedules are independent per scope (`tempo_no2_*` and
`tempo_no2_std_*`). Quality contracts are versioned separately
(`tempo_no2_contract.csv` vs `tempo_no2_std_contract.csv`).
RiverPulse has explicit assets/jobs rather than using the TEMPO scope factory,
and its science policy is `riverpulse_events_contract.csv`.
PlumeGraph is also explicit, review-gates its cohort and benchmark, and uses
`plumegraph_events_contract.csv` as its only scientific-policy source.

`sun2025:repro` and `andreadis2025:repro` are metadata-only acquisition
readiness/preflight profiles. They use `sun2025_repro_ops` and
`andreadis2025_repro_ops`, respectively, publish no marts, and run only when an
operator explicitly supplies evidence/import configuration or an existing
inventory. See
[Paper reproduction preflight](../concepts/paper-reproduction-preflight.md).

See [TEMPO product notes](../concepts/tempo-product-notes.md),
[Orchestration](../reference/orchestration.md), and
[RiverPulse product notes](../concepts/riverpulse-product-notes.md), and
[PlumeGraph product notes](../concepts/plumegraph-product-notes.md).
