# Scripts

Operator and maintainer scripts under `scripts/`. Prefer Make targets from
[AGENTS.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/AGENTS.md)
over ad-hoc flags unless diagnosing a failure. Private helpers whose names
start with `_` (for example `_bootstrap.py`) are not part of the public
inventory.

| Script | Purpose | When to use | When not to use |
| --- | --- | --- | --- |
| `build_region_artifacts.py` | Build pinned production geography (verified cache/offline) or deterministic `--synthetic` fixtures; writes the atomic manifest | First live setup, geometry pin changes, or demo/test synthetic geography | Hand-editing generated Parquet; mixing files from different generations |
| `build_demo.py` | Build the credential-free NRT demo warehouse at `.cache/demo.duckdb` | Local analyst demos, docs recipe smoke prerequisites (`make demo`) | Expecting standard-scope marts; production Earthdata runs |
| `check_docs_recipe_sql.py` | Smoke-check SQL fences in `docs/guides/query-recipes.md` against `.cache/demo.duckdb` (skips missing std relations) | After editing query recipes; `make docs-recipe-smoke` / `docs-check` | As a substitute for full dbt golden tests or live smoke |
| `generate_geo_fixtures.py` | Regenerate synthetic geography fixtures under `tests/fixtures/geo` | Maintainer fixture updates after registry/weight contract changes | Operator day-two geography; prefer `build_region_artifacts.py` |
| `generate_netcdf_fixtures.py` | Regenerate synthetic TEMPO-shaped NetCDF fixtures for tests | Maintainer NetCDF layout/fixture updates | Live ingestion or demo warehouse builds |
| `run_gx_data_quality.py` | Run Great Expectations-style checks against a disposable dbt build database | Local release gate after `dbt-build-ci` (`make gx-data-quality` / `data-quality`) | Against a locked production writer session without a disposable DB |
| `seed_dbt_source_freshness.py` | Seed disposable DuckDB source rows for dbt source-freshness CI | `make dbt-source-freshness-ci` | Seeding an operator production warehouse |
| `compact_warehouse.py` | Rewrite the DuckDB file to reclaim dead space (`make compact-warehouse`) | Occasional maintenance when the warehouse file grew after deletes | While another writer holds the DB; during an active Dagster run |
| `run_live_smoke.py` | Opt-in CMR discovery, pinned geography build helpers, or credentialed disposable two-granule validation | Local live readiness and weekly operator smoke (`make live-smoke`, `--mode discovery`) | CI/GitHub Actions; committing downloaded payloads |

## Related Make targets

| Target | Script / role |
| --- | --- |
| `make demo` | `build_demo.py` |
| `make docs-recipe-smoke` | `check_docs_recipe_sql.py` |
| `make live-smoke` | `run_live_smoke.py --mode live-smoke` |
| `make gx-data-quality` | `run_gx_data_quality.py` |
| `make compact-warehouse` | `compact_warehouse.py` |
