"""Immutable geography artifact generation publication and manifest writing."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from titanskies_pipeline.geography.acquire import (
    ARTIFACT_MANIFEST_NAME,
    ARTIFACT_MANIFEST_VERSION,
    sha256_file,
)
from titanskies_pipeline.geography.registry import REGISTRY_COLUMNS, WEIGHT_COLUMNS


def atomic_json(payload: Mapping[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, separators=(",", ": ")) + "\n"
        )
        json.loads(temporary.read_text())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def publish_artifact_generation(
    *,
    output_dir: Path,
    registry_source: Path,
    weights_source: Path,
    artifact_mode: str,
    geometry_version: str,
    grid_version: str,
    source_manifest_sha256: str,
    region_count: int,
    weight_count: int,
) -> dict[str, Any]:
    """Publish an immutable artifact pair, then atomically point at it."""
    if artifact_mode not in {"production", "synthetic"}:
        raise ValueError(f"Unknown geography artifact mode: {artifact_mode}")
    registry_parquet = pq.ParquetFile(registry_source)
    weights_parquet = pq.ParquetFile(weights_source)
    missing_registry = set(REGISTRY_COLUMNS) - set(registry_parquet.schema_arrow.names)
    missing_weights = set(WEIGHT_COLUMNS) - set(weights_parquet.schema_arrow.names)
    if missing_registry or missing_weights:
        raise ValueError("Geography artifact schema validation failed")
    expected_metadata = {
        b"grid_version": grid_version.encode(),
        b"geometry_version": geometry_version.encode(),
        b"source_manifest_sha256": source_manifest_sha256.encode(),
    }
    if any(
        (parquet.schema_arrow.metadata or {}).get(key) != value
        for parquet in (registry_parquet, weights_parquet)
        for key, value in expected_metadata.items()
    ):
        raise ValueError("Geography artifact metadata validation failed")
    if (
        registry_parquet.metadata.num_rows != region_count
        or weights_parquet.metadata.num_rows != weight_count
        or region_count < 1
        or weight_count < 1
    ):
        raise ValueError("Geography artifact row-count validation failed")
    import duckdb

    validator = duckdb.connect()
    try:
        registry_unsorted = validator.execute(
            """
            SELECT 1 FROM (
                SELECT canonical_region_id,
                       lag(canonical_region_id) OVER () AS previous_id
                FROM read_parquet(?)
            ) WHERE canonical_region_id <= previous_id LIMIT 1
            """,
            [str(registry_source)],
        ).fetchone()
        weights_unsorted = validator.execute(
            """
            SELECT 1 FROM (
                SELECT canonical_region_id, grid_row, grid_col,
                       lag(canonical_region_id) OVER () AS previous_id,
                       lag(grid_row) OVER () AS previous_row,
                       lag(grid_col) OVER () AS previous_col
                FROM read_parquet(?)
            ) WHERE canonical_region_id < previous_id
               OR (canonical_region_id = previous_id AND grid_row < previous_row)
               OR (canonical_region_id = previous_id AND grid_row = previous_row
                   AND grid_col < previous_col)
            LIMIT 1
            """,
            [str(weights_source)],
        ).fetchone()
    finally:
        validator.close()
    if registry_unsorted or weights_unsorted:
        raise ValueError("Geography artifacts are not canonically sorted")

    registry_checksum = sha256_file(registry_source)
    weights_checksum = sha256_file(weights_source)
    identity = {
        "artifact_mode": artifact_mode,
        "geometry_version": geometry_version,
        "grid_version": grid_version,
        "source_manifest_sha256": source_manifest_sha256,
        "registry_sha256": registry_checksum,
        "weights_sha256": weights_checksum,
        "region_count": region_count,
        "weight_count": weight_count,
    }
    build_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:20]
    generation = output_dir / "generations" / build_id
    if not generation.exists():
        temporary = output_dir / "generations" / f".{build_id}.{os.getpid()}.tmp"
        temporary.mkdir(parents=True, exist_ok=False)
        try:
            registry_path = temporary / "tempo_region_registry.parquet"
            weights_path = temporary / "tempo_grid_region_weights.parquet"
            os.replace(registry_source, registry_path)
            os.replace(weights_source, weights_path)
            if sha256_file(registry_path) != registry_checksum:
                raise ValueError("Registry checksum changed during publication")
            if sha256_file(weights_path) != weights_checksum:
                raise ValueError("Weight checksum changed during publication")
            os.replace(temporary, generation)
        finally:
            if temporary.exists():
                import shutil

                shutil.rmtree(temporary)
    else:
        existing_registry = generation / "tempo_region_registry.parquet"
        existing_weights = generation / "tempo_grid_region_weights.parquet"
        if (
            not existing_registry.is_file()
            or not existing_weights.is_file()
            or sha256_file(existing_registry) != registry_checksum
            or sha256_file(existing_weights) != weights_checksum
        ):
            raise ValueError(f"Existing geography generation {build_id} is corrupt")
        registry_source.unlink(missing_ok=True)
        weights_source.unlink(missing_ok=True)

    manifest = {
        "manifest_version": ARTIFACT_MANIFEST_VERSION,
        "build_id": build_id,
        "artifact_mode": artifact_mode,
        "geometry_version": geometry_version,
        "grid_version": grid_version,
        "source_manifest_sha256": source_manifest_sha256,
        "registry": {
            "path": f"generations/{build_id}/tempo_region_registry.parquet",
            "sha256": registry_checksum,
            "row_count": region_count,
        },
        "weights": {
            "path": f"generations/{build_id}/tempo_grid_region_weights.parquet",
            "sha256": weights_checksum,
            "row_count": weight_count,
        },
    }
    manifest_path = output_dir / ARTIFACT_MANIFEST_NAME
    atomic_json(manifest, manifest_path)
    return {
        "manifest_path": manifest_path,
        "build_id": build_id,
        "artifact_mode": artifact_mode,
        "registry_path": generation / "tempo_region_registry.parquet",
        "weights_path": generation / "tempo_grid_region_weights.parquet",
        "region_count": region_count,
        "weight_count": weight_count,
        "registry_checksum": registry_checksum,
        "weights_checksum": weights_checksum,
        "geometry_version": geometry_version,
        "grid_version": grid_version,
    }


__all__ = ["atomic_json", "publish_artifact_generation"]
