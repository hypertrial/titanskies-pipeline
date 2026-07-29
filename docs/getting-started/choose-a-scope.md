# Choose a scope

TitanSkies ships three first-class scopes in one warehouse.

| Scope | Product | Schemas | When to use |
| --- | --- | --- | --- |
| `tempo:no2` | TEMPO NO₂ L3 NRT | `tempo_no2_*` | Near-real-time monitoring; `make demo` builds this scope |
| `tempo:no2_std` | TEMPO NO₂ L3 V04 standard | `tempo_no2_std_*` | Standard collection; schemas bootstrap empty until an explicit std run |
| `riverpulse:events` | SWOT RiverSP reach Version D + SWORD v17b | `riverpulse_events_*` | River observations, discharges, topology, revisions, and provenance; `make riverpulse-demo` |

Jobs and schedules are independent per scope (`tempo_no2_*` and
`tempo_no2_std_*`). Quality contracts are versioned separately
(`tempo_no2_contract.csv` vs `tempo_no2_std_contract.csv`).
RiverPulse has explicit assets/jobs rather than using the TEMPO scope factory,
and its science policy is `riverpulse_events_contract.csv`.

See [TEMPO product notes](../concepts/tempo-product-notes.md),
[Orchestration](../reference/orchestration.md), and
[RiverPulse product notes](../concepts/riverpulse-product-notes.md).
