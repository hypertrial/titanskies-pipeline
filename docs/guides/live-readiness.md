# Local live readiness

Live readiness is an operator-owned local workflow. GitHub Actions is limited
to a single offline gate of less than five cumulative minutes and never
downloads production geography or TEMPO NetCDF data.

Public CMR discovery needs no Earthdata credentials:

```bash
uv run python scripts/run_live_smoke.py --mode discovery
```

Build and verify the pinned production geography locally:

```bash
uv sync --locked --extra geo
uv run python scripts/run_live_smoke.py --mode geography
```

Credentialed end-to-end verification uses operator-owned
`EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` values:

```bash
uv run make live-smoke
```

The smoke discovers the preceding 24 hours, processes at most two real
granules through Dagster and dbt, validates nonempty administrative and grid
marts, and reports DQ severity counts without treating expected observation
quality as a pipeline failure. All disposable state stays below
`.cache/live-readiness/`; NetCDF, DuckDB, and geography artifacts remain
excluded from source control.
