# Paper reproduction preflight

TitanSkies v0.7 introduces two independent, unscheduled profiles:

| Profile | Paper | Pinned period |
| --- | --- | --- |
| `sun2025:repro` | DOI `10.1029/2025JD044565` | 2023-08-02 through 2024-12-31 |
| `andreadis2025:repro` | DOI `10.1029/2024GL114185` | 2023-03-30 through 2024-07-21, with a 2024-10-24 paper-access cutoff |

The tracked JSON manifests under `config/reproductions/` pin collection
versions, concept IDs, DOIs, provider access methods, request fields,
attribution, source terms, and allowed fallbacks. The adjacent CSV files are
the sole scientific-policy manifests. Environment variables cannot change
those scientific values.

## What preflight proves

Preflight accepts a provider-discovery inventory, validates every source and
object, estimates known storage, counts unknown-size objects, and records:

- the source, request, and scientific-contract hashes;
- every discovered object's stable content identity and provenance;
- per-source completeness and exactness;
- hard object/storage-budget failures; and
- an acquisition generation only when all required sources pass.

It rejects duplicate identities, malformed sizes, undeclared sources,
secret-bearing fields, credential-bearing URLs, and signed URLs in any
persisted metadata field. Identical input creates the same run and generation
identities.

The four exactness states are:

| Status | Meaning |
| --- | --- |
| `exact` | The paper-time collection, revision, code, network, and contract are established. |
| `provider_reprocessed` | The provider has republished historical content under a later revision. |
| `method_equivalent` | The input supports the method but is not the paper-time artifact. |
| `unavailable` | The required artifact or revision has not been established. |

Exact mode accepts only each source's declared paper requirement. Fallback
mode accepts only fallbacks listed in the tracked manifest; it never silently
relabels them as exact.

Every inventory must declare `inventory_mode` as `production` or `synthetic`.
A ready production inventory creates a `planned` acquisition generation. A
ready fixture creates a `synthetic` generation, which downstream production
acquisition must reject.

## Run the offline contract demonstration

```bash
uv run make sun2025-preflight
uv run make andreadis2025-preflight
```

Those targets use small hand-authored inventories under
`tests/fixtures/reproductions/` and write only
`.cache/reproduction_preflight.duckdb`. Every fixture URL uses
`example.test`; fixtures declare `inventory_mode: synthetic`, and the result
demonstrates software behavior, not scientific availability.

To validate a real provider-catalog export, point the corresponding Make
variable at an operator-owned JSON inventory:

```bash
SUN2025_PREFLIGHT_INVENTORY=/absolute/path/sun-inventory.json \
  uv run make sun2025-preflight

ANDREADIS2025_PREFLIGHT_INVENTORY=/absolute/path/swot-inventory.json \
  uv run make andreadis2025-preflight
```

Use `scripts/run_reproduction_preflight.py --help` for storage/object budgets
and explicit fallback mode. The command reads metadata only; it does not
download source payloads.

## Known exact-mode blockers

The source manifests intentionally retain unresolved inputs as blockers:

- the 14-facility reviewed cohort and any supplementary selection detail for
  the TEMPO paper must be extracted and reviewed;
- the paper-time GEOS-CF archive revision must be established;
- the exact object generation from the pinned SWOT L4 SoS Version 1 collection
  available at the paper's 2024-10-24 access cutoff must be resolved; current
  Version 3 is a fallback, not exact; and
- the paper-time Confluence repository revisions must be pinned.

GRDC gauge data is optional when the pinned L4 product contains the needed
priors. If it is needed, operators must honor the research-use and
nonredistribution terms.

## Deliberate v0.7 boundary

This slice does not yet acquire production data, normalize new ERA5/GEOS-CF
or RiverSP node/L4 payloads, execute paper algorithms, or publish result
comparisons. Those stages must consume only a ready preflight generation and
must preserve the exactness labels through every downstream artifact.
