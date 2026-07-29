# Scripts

Operator and maintainer scripts under `scripts/`. Prefer Make targets from
[AGENTS.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/AGENTS.md)
over ad-hoc flags unless diagnosing a failure. Private helpers whose names
start with `_` (for example `_bootstrap.py`) are not part of the public
inventory.

| Script | Purpose | When to use | When not to use |
| --- | --- | --- | --- |
| `build_region_artifacts.py` | Build pinned production geography (verified cache/offline) or deterministic `--synthetic` fixtures; writes the atomic manifest | First live setup, geometry pin changes, or demo/test synthetic geography | Hand-editing generated Parquet; mixing files from different generations |
| `build_riverpulse_network.py` | Build pinned SWORD v17b Sacramento/Rhine/Murray corridors or a synthetic network; writes immutable Parquet generations | RiverPulse bootstrap, verified offline rebuild, or synthetic demo | Recurring schedule runs; changing network version after observations exist |
| `build_demo.py` | Build the credential-free NRT demo warehouse at `.cache/demo.duckdb` | Local analyst demos, docs recipe smoke prerequisites (`make demo`) | Expecting standard-scope marts; production Earthdata runs |
| `build_riverpulse_demo.py` | Build the offline synthetic RiverPulse warehouse and show current-versus-revision queries | `make riverpulse-demo`, local review, and regression validation | Live Hydrocron/SWORD validation |
| `build_plumegraph_demo.py` | Build a synthetic 75-plant PlumeGraph warehouse, validate it, and verify an immutable release | `make plumegraph-demo`, offline review, regression validation | Scientific approval or live source validation |
| `build_plumegraph_release.py` | Build and verify an immutable release from current promoted generations | `make plumegraph-release` after held-out validation | Mutable or unvalidated publication |
| `check_docs_recipe_sql.py` | Smoke-check SQL fences in `docs/guides/query-recipes.md` against `.cache/demo.duckdb` (skips missing std relations) | After editing query recipes; `make docs-recipe-smoke` / `docs-check` | As a substitute for full dbt golden tests or live smoke |
| `generate_geo_fixtures.py` | Regenerate synthetic geography fixtures under `tests/fixtures/geo` | Maintainer fixture updates after registry/weight contract changes | Operator day-two geography; prefer `build_region_artifacts.py` |
| `generate_netcdf_fixtures.py` | Regenerate synthetic TEMPO-shaped NetCDF fixtures for tests | Maintainer NetCDF layout/fixture updates | Live ingestion or demo warehouse builds |
| `run_gx_data_quality.py` | Run Great Expectations-style checks against a disposable dbt build database | Local release gate after `dbt-build-ci` (`make gx-data-quality` / `data-quality`) | Against a locked production writer session without a disposable DB |
| `seed_dbt_source_freshness.py` | Seed disposable DuckDB source rows for dbt source-freshness CI | `make dbt-source-freshness-ci` | Seeding an operator production warehouse |
| `compact_warehouse.py` | Rewrite the DuckDB file to reclaim dead space (`make compact-warehouse`) | Occasional maintenance when the warehouse file grew after deletes | While another writer holds the DB; during an active Dagster run |
| `run_live_smoke.py` | Opt-in CMR discovery, pinned geography build helpers, or credentialed disposable two-granule validation | Local live readiness and weekly operator smoke (`make live-smoke`, `--mode discovery`) | CI/GitHub Actions; committing downloaded payloads |
| `run_riverpulse_live_smoke.py` | Opt-in one-reach, at-most-90-day Hydrocron run against a production network and disposable warehouse | RiverPulse release/operator readiness (`make riverpulse-live-smoke`) | CI/GitHub Actions; broad backfills |
| `run_plumegraph_live_smoke.py` | Run one reviewed facility/day through Harmony, HRRR, CAMD, analysis, and dbt | PlumeGraph readiness (`make plumegraph-live-smoke`) | CI, broad backfills, or release publication |
| `run_reproduction_preflight.py` | Validate and ledger a paper-profile provider-discovery inventory without payload downloads | Before source acquisition; offline contract checks | Claiming that either paper has been reproduced |

## Related Make targets

| Target | Script / role |
| --- | --- |
| `make demo` | `build_demo.py` |
| `make riverpulse-demo` | `build_riverpulse_demo.py` |
| `make riverpulse-live-smoke` | `run_riverpulse_live_smoke.py` with `RIVERPULSE_NETWORK_MANIFEST` |
| `make plumegraph-demo` | `build_plumegraph_demo.py` |
| `make plumegraph-live-smoke` | `run_plumegraph_live_smoke.py` with `PLUMEGRAPH_COHORT_MANIFEST` |
| `make plumegraph-release` | `build_plumegraph_release.py` with `PLUMEGRAPH_RELEASE_VERSION` |
| `make sun2025-preflight` | `run_reproduction_preflight.py sun2025` with `SUN2025_PREFLIGHT_INVENTORY` |
| `make andreadis2025-preflight` | `run_reproduction_preflight.py andreadis2025` with `ANDREADIS2025_PREFLIGHT_INVENTORY` |
| `make docs-recipe-smoke` | `check_docs_recipe_sql.py` |
| `make live-smoke` | `run_live_smoke.py --mode live-smoke` |
| `make gx-data-quality` | `run_gx_data_quality.py` |
| `make compact-warehouse` | `compact_warehouse.py` |
