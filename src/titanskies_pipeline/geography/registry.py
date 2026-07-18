"""Load, validate, and register an atomic geography artifact generation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from titanskies_pipeline.config.settings import TEMPO_GEOGRAPHY_MANIFEST_PATH
from titanskies_pipeline.naming import SCOPE_NO2
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    tempo_ops_tbl,
    tempo_raw_tbl,
)

REGISTRY_COLUMNS = (
    "country_code",
    "region_type",
    "source_region_id",
    "canonical_region_id",
    "region_name",
    "parent_region_id",
    "timezone",
    "geometry_version",
    "geometry_checksum",
)

WEIGHT_COLUMNS = (
    "grid_row",
    "grid_col",
    "canonical_region_id",
    "overlap_weight",
    "geometry_version",
)


@dataclass(frozen=True)
class GeoArtifacts:
    manifest_path: Path
    build_id: str
    artifact_mode: str
    registry_path: Path
    weights_path: Path
    geometry_version: str
    source_manifest_sha256: str
    registry_checksum: str
    weights_checksum: str
    grid_version: str
    region_count: int
    weight_count: int


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_columns(
    columns: list[str], required: tuple[str, ...], label: str
) -> None:
    missing = [col for col in required if col not in columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _contained_artifact_path(root: Path, relative: object, label: str) -> Path:
    candidate = Path(str(relative))
    if candidate.is_absolute():
        raise ValueError(f"Geography {label} path must be manifest-relative")
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"Geography {label} path escapes the artifact root")
    return resolved


def load_geo_artifacts(
    manifest_path: Path | None = None,
    *,
    allow_synthetic: bool = False,
) -> GeoArtifacts:
    manifest_path = (manifest_path or TEMPO_GEOGRAPHY_MANIFEST_PATH).resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Geography manifest not found at {manifest_path}. "
            "Run scripts/build_region_artifacts.py first."
        )
    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"Invalid geography manifest: {manifest_path}") from exc
    required = {
        "manifest_version",
        "build_id",
        "artifact_mode",
        "geometry_version",
        "grid_version",
        "source_manifest_sha256",
        "registry",
        "weights",
    }
    missing = required - set(manifest)
    if missing:
        raise ValueError(
            f"Geography manifest missing fields: {', '.join(sorted(missing))}"
        )
    if str(manifest["manifest_version"]) != "1":
        raise ValueError(
            f"Unsupported geography manifest version: {manifest['manifest_version']}"
        )
    mode = str(manifest["artifact_mode"])
    if mode not in {"production", "synthetic"}:
        raise ValueError(f"Unknown geography artifact mode: {mode}")
    if mode != "production" and not allow_synthetic:
        raise ValueError(
            "Production ingestion requires a production geography manifest; "
            f"found {mode!r}"
        )
    root = manifest_path.parent.resolve()
    registry_info = manifest["registry"]
    weights_info = manifest["weights"]
    if not isinstance(registry_info, dict) or not isinstance(weights_info, dict):
        raise ValueError("Geography artifact entries must be objects")
    for label, info in (("registry", registry_info), ("weights", weights_info)):
        if {"path", "sha256", "row_count"} - set(info):
            raise ValueError(f"Geography {label} manifest entry is incomplete")
    registry_path = _contained_artifact_path(
        root, registry_info.get("path"), "registry"
    )
    weights_path = _contained_artifact_path(root, weights_info.get("path"), "weights")
    for label, path in (("registry", registry_path), ("weights", weights_path)):
        if not path.is_file():
            raise FileNotFoundError(f"Geography {label} artifact not found at {path}")
    registry_checksum = _file_checksum(registry_path)
    weights_checksum = _file_checksum(weights_path)
    if registry_checksum != registry_info.get("sha256"):
        raise ValueError("Geography registry checksum mismatch")
    if weights_checksum != weights_info.get("sha256"):
        raise ValueError("Geography weights checksum mismatch")

    registry_file = pq.ParquetFile(registry_path)
    weights_file = pq.ParquetFile(weights_path)
    _validate_columns(registry_file.schema_arrow.names, REGISTRY_COLUMNS, "registry")
    _validate_columns(weights_file.schema_arrow.names, WEIGHT_COLUMNS, "weights")
    expected_metadata = {
        b"grid_version": str(manifest["grid_version"]).encode(),
        b"geometry_version": str(manifest["geometry_version"]).encode(),
        b"source_manifest_sha256": str(manifest["source_manifest_sha256"]).encode(),
    }
    if any(
        (parquet.schema_arrow.metadata or {}).get(key) != value
        for parquet in (registry_file, weights_file)
        for key, value in expected_metadata.items()
    ):
        raise ValueError("Geography artifact metadata does not match manifest")
    region_count = registry_file.metadata.num_rows
    weight_count = weights_file.metadata.num_rows
    if region_count != registry_info.get(
        "row_count"
    ) or weight_count != weights_info.get("row_count"):
        raise ValueError("Geography artifact row count does not match manifest")
    if region_count == 0 or weight_count == 0:
        raise ValueError("Geography artifacts must not be empty")

    registry = registry_file.read(columns=["geometry_version"])
    registry_versions = set(registry["geometry_version"].to_pylist())
    weights = weights_file.read(columns=["geometry_version"])
    weight_versions = set(weights["geometry_version"].to_pylist())
    geometry_version = str(manifest["geometry_version"])
    if registry_versions != {geometry_version} or weight_versions != {geometry_version}:
        raise ValueError("Geography artifact geometry_version mismatch")

    return GeoArtifacts(
        manifest_path=manifest_path,
        build_id=str(manifest["build_id"]),
        artifact_mode=mode,
        registry_path=registry_path,
        weights_path=weights_path,
        geometry_version=geometry_version,
        source_manifest_sha256=str(manifest["source_manifest_sha256"]),
        registry_checksum=registry_checksum,
        weights_checksum=weights_checksum,
        grid_version=str(manifest["grid_version"]),
        region_count=region_count,
        weight_count=weight_count,
    )


def persist_geo_artifacts(
    artifacts: GeoArtifacts, *, scope: str = SCOPE_NO2, conn=None
) -> dict[str, int]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    registry = pq.read_table(artifacts.registry_path)
    loaded = registry.append_column("loaded_at", pa.array([now] * registry.num_rows))

    with _use_conn(conn) as connection:
        existing = connection.execute(
            f"SELECT geometry_version FROM "
            f"{tempo_ops_tbl('geography_artifact_manifest', scope=scope)} LIMIT 1"
        ).fetchone()
        if (
            existing
            and str(existing[0]) != artifacts.geometry_version
            and connection.execute(
                f"SELECT 1 FROM "
                f"{tempo_raw_tbl('region_hour_aggregates', scope=scope)} LIMIT 1"
            ).fetchone()
        ):
            raise RuntimeError(
                "Geography version changes require a v0.4 warehouse rebuild"
            )
        connection.register("_tempo_region_registry", loaded)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"DELETE FROM {tempo_ops_tbl('region_registry', scope=scope)}"
            )
            connection.execute(
                f"""
                INSERT INTO {tempo_ops_tbl("region_registry", scope=scope)}
                SELECT country_code, region_type, source_region_id,
                       canonical_region_id, region_name, parent_region_id,
                       timezone, geometry_version, geometry_checksum, loaded_at
                FROM _tempo_region_registry
                """
            )
            connection.execute(
                f"DELETE FROM "
                f"{tempo_ops_tbl('geography_artifact_manifest', scope=scope)}"
            )
            connection.execute(
                f"""
                INSERT INTO
                {tempo_ops_tbl("geography_artifact_manifest", scope=scope)}
                (build_id, artifact_mode, geometry_version, source_manifest_sha256,
                 registry_path, weights_path, registry_checksum, weights_checksum,
                 grid_version, region_count, weight_count, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    artifacts.build_id,
                    artifacts.artifact_mode,
                    artifacts.geometry_version,
                    artifacts.source_manifest_sha256,
                    str(artifacts.registry_path),
                    str(artifacts.weights_path),
                    artifacts.registry_checksum,
                    artifacts.weights_checksum,
                    artifacts.grid_version,
                    artifacts.region_count,
                    artifacts.weight_count,
                    now,
                ],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.unregister("_tempo_region_registry")

    return {
        "regions_loaded": artifacts.region_count,
        "weights_loaded": artifacts.weight_count,
    }


__all__ = [
    "REGISTRY_COLUMNS",
    "WEIGHT_COLUMNS",
    "GeoArtifacts",
    "load_geo_artifacts",
    "persist_geo_artifacts",
]
