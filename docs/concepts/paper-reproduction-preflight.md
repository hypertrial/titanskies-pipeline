# Paper reproduction preflight

TitanSkies v0.7 introduces two independent, unscheduled profiles:

| Profile | Paper | Pinned period |
| --- | --- | --- |
| `sun2025:repro` | DOI `10.1029/2025JD044565` | 2023-08-02 through 2024-12-31 |
| `andreadis2025:repro` | DOI `10.1029/2024GL114185` | 2023-03-30 through 2024-07-21, with a 2024-10-24 paper-access cutoff |

The tracked JSON manifests under `config/reproductions/` pin collection
versions, concept IDs, DOIs, provider access methods, request fields,
attribution, source terms, and allowed fallbacks. Adjacent
`reproduction-resolution-v1` bundles record public technical evidence and
precise blockers. The Sun profile also tracks the normalized 14-row CAMD
cohort extracted from Supporting Information Table S1. The contract CSV files
remain the sole scientific-policy manifests; environment variables cannot
change those values.

## What preflight proves

Readiness resolves provider metadata into a deterministic
`reproduction-source-inventory-v2` file. Preflight validates that inventory
and records:

- the source, request, and scientific-contract hashes;
- every discovered object's stable content identity and provenance;
- per-source completeness and exactness;
- exact reported bytes, conservative size upper bounds, hard object/storage
  budgets, and unbounded-object failures; and
- an acquisition generation only when all required sources pass.

It rejects duplicate identities, malformed or BIGINT-overflowing sizes,
undeclared sources,
secret-bearing fields, credential-bearing URLs, and signed URLs in any
persisted metadata field. Identical input creates the same run and generation
identities. Production inventory ordering is canonical, and local run time is
not part of its identity.

CMR resolution verifies the provider-reported hit count on every page and
classifies timed-out or prematurely ended pagination as `transient_error`.
EPA CAMD resolution consumes the official
[bulk-file `items` contract](https://api.epa.gov/easey/camd-services/swagger/)
and requires one hourly national-quarter file for each quarter of 2023 and
2024; the catalog `s3Path`, byte count, and `lastUpdated` value become the
canonical object identity, size, and provider revision.

Each source resolution has one technical outcome:

| Outcome | Meaning |
| --- | --- |
| `resolved` | Immutable provider identities and complete bounded objects were established. |
| `operator_input_required` | A documented provider export, credential, or licensed input is still needed. |
| `transient_error` | Provider metadata could not be completely resolved during this run. |
| `definitively_unavailable` | Public evidence establishes that the exact artifact cannot currently be recovered. |
| `not_required` | A conditional source is proven unnecessary by another exact artifact. |

A definitive-unavailable result means the investigation completed
successfully; it does **not** create an acquisition-ready generation.

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

Every inventory declares `inventory_mode` as `production` or `synthetic`.
Production additionally requires the resolution format and bundle hash, exact
source outcomes, complete coverage, and `unbounded_size_count = 0`.
`max_bytes` is checked against `planned_max_bytes`: exact sizes when known and
documented upper bounds otherwise. A ready production inventory creates a
`planned` acquisition generation. A ready fixture creates a `synthetic`
generation, which downstream production acquisition must reject.

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

## Run exact-source readiness

Readiness is exact-only. Supply an evidence bundle and an import directory;
generated inventory and DuckDB state remain under `.cache/`:

```bash
SUN2025_READINESS_EVIDENCE=config/reproductions/sun2025_resolution.json \
SUN2025_READINESS_IMPORT_DIR=config/reproductions \
  uv run make sun2025-readiness

ANDREADIS2025_READINESS_EVIDENCE=config/reproductions/andreadis2025_resolution.json \
ANDREADIS2025_READINESS_IMPORT_DIR=/absolute/operator/imports \
  uv run make andreadis2025-readiness
```

The resolver uses exit `0` for exact-ready, `2` for complete
evidence-backed blockers, and `3` when provider resolution or operator input
is incomplete. It never reads CDS credentials or `.cdsapirc`; operators export
CDS request/result metadata matching the canonical facility-month requests.
`PLUMEGRAPH_EPA_API_KEY`, when needed for the shared EPA service, is sent only
as a header and is never persisted.

## Current exact-mode findings

The evidence bundles currently establish:

- the Sun Table S1 cohort is normalized to exactly 14 unique CAMD facility IDs,
  with paper labels, coordinates, source locators, crosswalk evidence, and a
  30 km source-centered analysis extent;
- CDS result metadata remains operator input for canonical monthly ERA5
  requests;
- the paper-time GEOS-CF v1 analysis/replay object revision is definitively
  unavailable from the paper, supplement, and current v2 service;
- the exact SWOT L4 SoS Version 1 generation available by the
  2024-10-24 cutoff is definitively unavailable from current public CMR
  metadata; Version 3 remains a declared non-exact fallback; and
- the paper does not name the complete Confluence repository/commit set, while
  the public organization contains 59 repositories. Inferring
  latest-before-cutoff commits would not prove exactness.

GRDC gauge data becomes `not_required` only when an exact L4 priors artifact
proves that the needed priors are embedded. Otherwise exact GRDC evidence is a
conditional blocker, and operators must honor its research-use and
nonredistribution terms.

## Deliberate v0.7 boundary

This slice does not yet acquire production data, normalize new ERA5/GEOS-CF
or RiverSP node/L4 payloads, execute paper algorithms, or publish result
comparisons. Those stages must consume only a ready preflight generation and
must preserve the exactness labels through every downstream artifact.
