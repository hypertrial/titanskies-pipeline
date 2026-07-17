# Build geography artifacts

Run the operator GIS script before the first pipeline materialization:

```bash
uv sync --locked --extra geo
python scripts/build_region_artifacts.py --output-dir artifacts/geo
```

Production mode downloads the exact sources listed in
`config/geography_sources.json` to `.cache/geo_sources`. Every archive is
SHA-256 verified before reading. Both Parquet files are validated in a
temporary generation. The immutable `generations/<build_id>/` directory is
published first, then one JSON manifest pointer is atomically replaced.
Identical generations are reused and older generations remain for rollback.

For a managed or air-gapped cache:

```bash
python scripts/build_region_artifacts.py \
  --source-cache /srv/titanskies/geo_sources \
  --output-dir artifacts/geo

python scripts/build_region_artifacts.py \
  --source-cache /srv/titanskies/geo_sources \
  --output-dir artifacts/geo \
  --offline
```

`--offline` never performs a network request and fails if any pinned archive
is absent or has the wrong checksum.

For local smoke and CI fixtures:

```bash
python scripts/build_region_artifacts.py --synthetic
```

Outputs:

- `artifacts/geo/tempo_geography_artifacts.json`
- `artifacts/geo/generations/<build_id>/tempo_region_registry.parquet`
- `artifacts/geo/generations/<build_id>/tempo_grid_region_weights.parquet`

The registry covers countries, states/provinces/territories, US counties,
Canadian census subdivisions, and Mexican municipalities. Weight rows contain
actual overlap square kilometres on the native TEMPO 0.02° grid and are sorted
by region, row, and column. Large weights remain in Parquet; DuckDB stores only
the registry and a one-row artifact manifest.

The JSON manifest records `artifact_mode`. Live ingestion rejects `synthetic`
or unknown modes; `--synthetic` is exclusively for demos and tests.

No downloaded boundary archive or generated production artifact is
distributed by this repository. The operator controls local files, while
source and derived-data rights remain governed by the source terms in the
[legal matrix](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
Redistributing the registry, weights, or a geography-enriched DuckDB may
trigger attribution, transformation-disclosure, Statistics Canada, INEGI, and
ODbL share-alike/access obligations. Review those terms for the intended use.
