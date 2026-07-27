"""Façade for geography artifact build: acquire → normalize → weights → publish."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from titanskies_pipeline.geography.acquire import (
    acquire_all_sources,
    acquire_source,
    find_file,
    load_source_manifest,
    safe_extract,
    sha256_file,
)
from titanskies_pipeline.geography.normalize import (
    CANADA_PROVINCES,
    MEXICO_STATES,
    assign_dominant_timezones,
    canonical_frame,
    derive_countries,
    derive_parents,
    geo_modules,
    geometry_checksum,
    provider_regions,
    region_registry_table,
    repair_dissolve,
    require_columns,
)
from titanskies_pipeline.geography.publish import (
    atomic_json,
    publish_artifact_generation,
)
from titanskies_pipeline.geography.tempo_grid import GRID_VERSION
from titanskies_pipeline.geography.weights import (
    atomic_parquet,
    atomic_weights,
    iter_region_weight_tables,
)

# Private aliases preserved for unit tests that import underscore names.
_geo_modules = geo_modules
_safe_extract = safe_extract
_find_file = find_file
_require_columns = require_columns
_repair_dissolve = repair_dissolve
_canonical_frame = canonical_frame
_provider_regions = provider_regions
_derive_parents = derive_parents
_derive_countries = derive_countries
_geometry_checksum = geometry_checksum
_atomic_parquet = atomic_parquet
_atomic_weights = atomic_weights
_atomic_json = atomic_json


def build_production_artifacts(
    *,
    output_dir: Path,
    source_cache: Path,
    manifest_path: Path,
    offline: bool,
) -> dict[str, Any]:
    manifest = load_source_manifest(manifest_path)
    sources = acquire_all_sources(manifest, source_cache=source_cache, offline=offline)
    regions, timezones = provider_regions(sources, source_cache)
    regions["timezone"] = assign_dominant_timezones(regions, timezones)
    geometry_version = str(manifest["geometry_version"])
    manifest_checksum = sha256_file(manifest_path)
    metadata = {
        b"grid_version": GRID_VERSION.encode(),
        b"geometry_version": geometry_version.encode(),
        b"source_manifest_sha256": manifest_checksum.encode(),
    }
    registry = region_registry_table(regions, geometry_version=geometry_version)
    registry = registry.replace_schema_metadata(metadata)
    output_dir.mkdir(parents=True, exist_ok=True)
    build_dir = Path(tempfile.mkdtemp(prefix=".build.", dir=output_dir))
    registry_path = build_dir / "tempo_region_registry.parquet"
    weights_path = build_dir / "tempo_grid_region_weights.parquet"
    atomic_parquet(registry, registry_path)
    weight_count = atomic_weights(
        iter_region_weight_tables(regions, geometry_version=geometry_version),
        weights_path,
        metadata=metadata,
    )
    try:
        return publish_artifact_generation(
            output_dir=output_dir,
            registry_source=registry_path,
            weights_source=weights_path,
            artifact_mode="production",
            geometry_version=geometry_version,
            grid_version=GRID_VERSION,
            source_manifest_sha256=manifest_checksum,
            region_count=registry.num_rows,
            weight_count=weight_count,
        )
    finally:
        import shutil

        shutil.rmtree(build_dir, ignore_errors=True)


__all__ = [
    "CANADA_PROVINCES",
    "MEXICO_STATES",
    "acquire_all_sources",
    "acquire_source",
    "assign_dominant_timezones",
    "build_production_artifacts",
    "iter_region_weight_tables",
    "load_source_manifest",
    "publish_artifact_generation",
    "region_registry_table",
    "sha256_file",
]
