# Build the RiverPulse network

RiverPulse geometry and topology come only from the pinned SWORD v17b
GeoPackage archive. Production collection never asks Hydrocron for geometry.

Install the geo extra and build the three bounded pilot corridors:

```bash
uv sync --locked --extra geo
python scripts/build_riverpulse_network.py \
  --output-dir artifacts/riverpulse \
  --source-cache .cache/riverpulse/sword
```

The build verifies the official archive checksum, resolves one outlet anchor
for the Sacramento, Rhine, and Murray systems, then follows the upstream branch
with greatest flow accumulation for at most 100 reaches. Non-selected
neighbours remain explicit boundary references. The published manifest points
to immutable, checksummed `reaches.parquet` and `reach_edges.parquet`
generations by root-relative path.

Use `--offline` to require the verified cached archive. Use `--synthetic` only
for demos and tests:

```bash
python scripts/build_riverpulse_network.py --synthetic
uv run make riverpulse-demo
```

In Dagster, materialize `riverpulse/events/ops/network_registry` once. The
weekly pipeline intentionally excludes network bootstrap. A network-generation
change after observations exist requires a clean warehouse.
