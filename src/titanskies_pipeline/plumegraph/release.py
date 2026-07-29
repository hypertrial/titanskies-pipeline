"""Atomic, checksum-addressed PlumeGraph evidence release builder."""

from __future__ import annotations

import importlib.metadata
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from titanskies_pipeline.config.settings_plumegraph import (
    PLUMEGRAPH_EVIDENCE_FORMAT,
    get_plumegraph_settings,
)
from titanskies_pipeline.geography.acquire import sha256_file
from titanskies_pipeline.plumegraph.identity import (
    analysis_generation_manifest_identity,
    canonical_json,
    sha256_identity,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import plumegraph_ops_tbl

_PARQUET_RELATIONS = {
    "facilities": "plumegraph_events_marts.plumegraph_events_facilities",
    "episodes": "plumegraph_events_marts.plumegraph_events_episodes",
    "episode_revisions": (
        "plumegraph_events_marts.plumegraph_events_episode_revisions"
    ),
    "episode_geometries": (
        "plumegraph_events_marts.plumegraph_events_episode_geometries"
    ),
    "trajectories": "plumegraph_events_marts.plumegraph_events_episode_geometries",
    "candidate_sources": (
        "plumegraph_events_marts.plumegraph_events_candidate_sources"
    ),
    "emission_estimates": (
        "plumegraph_events_marts.plumegraph_events_emission_estimates"
    ),
    "evidence_pixels": ("plumegraph_events_marts.plumegraph_events_evidence_pixels"),
    "validation_metrics": (
        "plumegraph_events_observability.plumegraph_events_benchmark_metrics"
    ),
    "benchmark_labels": "plumegraph_events_raw.benchmark_labels",
    "provenance": "plumegraph_events_marts.plumegraph_events_provenance",
}
_GEOMETRY_COLUMNS = {
    "facilities": "geometry_wkb",
    "episode_geometries": "geometry_wkb",
    "trajectories": "geometry_wkb",
    "evidence_pixels": "geometry_wkb",
}


@dataclass(frozen=True)
class ReleaseMetrics:
    release_id: str
    release_path: Path
    manifest_sha256: str
    episode_count: int
    file_count: int


def _db_time(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _relation_exists(connection, relation: str) -> bool:
    schema, name = relation.split(".", 1)
    return (
        connection.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, name],
        ).fetchone()
        is not None
    )


def _arrow_table(connection, relation: str) -> pa.Table:
    if not _relation_exists(connection, relation):
        raise RuntimeError(
            f"Missing PlumeGraph publication relation {relation}; run the "
            "PlumeGraph dbt build before creating a release"
        )
    return connection.execute(f"SELECT * FROM {relation}").to_arrow_table()


def _write_parquet(
    table: pa.Table,
    destination: Path,
    *,
    geometry_column: str | None = None,
) -> None:
    if geometry_column:
        if geometry_column not in table.column_names:
            raise ValueError(f"Missing release geometry column {geometry_column}")
        geo = {
            "version": "1.1.0",
            "primary_column": geometry_column,
            "columns": {
                geometry_column: {
                    "encoding": "WKB",
                    "geometry_types": [],
                    "crs": {
                        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                        "type": "GeographicCRS",
                        "name": "WGS 84",
                        "id": {"authority": "EPSG", "code": 4326},
                    },
                }
            },
        }
        metadata = dict(table.schema.metadata or {})
        metadata[b"geo"] = canonical_json(geo).encode()
        table = table.replace_schema_metadata(metadata)
    pq.write_table(table, destination, compression="zstd")
    verified = pq.read_table(destination)
    if (
        verified.num_rows != table.num_rows
        or verified.column_names != table.column_names
    ):
        raise ValueError(f"Failed to verify release artifact {destination.name}")
    if geometry_column and b"geo" not in (verified.schema.metadata or {}):
        raise ValueError(f"GeoParquet metadata missing from {destination.name}")


def _assert_unique(table: pa.Table, columns: Sequence[str], name: str) -> None:
    if not columns or table.num_rows == 0:
        return
    missing = set(columns) - set(table.column_names)
    if missing:
        raise ValueError(f"{name} release key missing columns: {sorted(missing)}")
    rows = table.select(columns).to_pylist()
    identities = [canonical_json(row) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{name} release contains duplicate primary keys")


def _package_version() -> str:
    try:
        return importlib.metadata.version("titanskies-pipeline")
    except importlib.metadata.PackageNotFoundError:
        return "0.7.0"


def build_release(
    *,
    release_version: str,
    output_dir: Path | None = None,
    validation_run_id: str | None = None,
    require_passed_validation: bool = True,
    conn=None,
) -> ReleaseMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    if not release_version or "/" in release_version or ".." in release_version:
        raise ValueError("Release version must be a safe, non-empty path component")
    settings = get_plumegraph_settings()
    release_root = (output_dir or settings.release_dir).resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    destination = (release_root / release_version).resolve()
    with _use_conn(conn) as connection:
        validation = (
            connection.execute(
                f"""
                SELECT validation_run_id, passed, probability_enabled,
                       analysis_run_manifest_sha256
                FROM {plumegraph_ops_tbl("validation_runs")}
                WHERE validation_run_id = ?
                """,
                [validation_run_id],
            ).fetchone()
            if validation_run_id
            else connection.execute(
                f"""
                SELECT validation_run_id, passed, probability_enabled,
                       analysis_run_manifest_sha256
                FROM {plumegraph_ops_tbl("validation_runs")}
                ORDER BY completed_at DESC, validation_run_id DESC
                LIMIT 1
                """
            ).fetchone()
        )
        if require_passed_validation and (not validation or not bool(validation[1])):
            raise RuntimeError(
                "Production PlumeGraph releases require a passing held-out "
                "validation run"
            )
        selected_validation_id = str(validation[0]) if validation else None
        generation_rows = connection.execute(
            f"""
            SELECT analysis_region_id, partition_date, analysis_run_id
            FROM {plumegraph_ops_tbl("current_generations")}
            ORDER BY analysis_region_id, partition_date
            """
        ).fetchall()
        if not generation_rows:
            raise RuntimeError("No current PlumeGraph analysis generation to release")
        analysis_manifest_sha = analysis_generation_manifest_identity(generation_rows)
        if validation and str(validation[3]) != analysis_manifest_sha:
            raise RuntimeError(
                "PlumeGraph validation is stale for the current analysis "
                "generation; rerun validation before creating a release"
            )
        release_id = sha256_identity(
            PLUMEGRAPH_EVIDENCE_FORMAT,
            release_version,
            analysis_manifest_sha,
            selected_validation_id or "",
            settings.contract["contract_version"],
            settings.contract["algorithm_version"],
            _package_version(),
        )
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{release_version}.", dir=release_root)
        )
        try:
            tables: dict[str, pa.Table] = {}
            for name, relation in _PARQUET_RELATIONS.items():
                table = _arrow_table(connection, relation)
                tables[name] = table
                _write_parquet(
                    table,
                    temporary / f"{name}.parquet",
                    geometry_column=_GEOMETRY_COLUMNS.get(name),
                )
            _assert_unique(tables["episodes"], ["episode_revision_id"], "episodes")
            _assert_unique(
                tables["episode_revisions"],
                ["episode_revision_id"],
                "episode revisions",
            )
            _assert_unique(
                tables["candidate_sources"],
                ["episode_revision_id", "facility_id"],
                "candidate sources",
            )
            _assert_unique(
                tables["emission_estimates"],
                ["episode_revision_id", "variant_id"],
                "emission estimates",
            )
            _assert_unique(
                tables["benchmark_labels"],
                ["benchmark_version", "window_id"],
                "benchmark labels",
            )
            evidence_dir = temporary / "evidence"
            evidence_dir.mkdir()
            revision_ids = sorted(
                str(value)
                for value in tables["episode_revisions"][
                    "episode_revision_id"
                ].to_pylist()
            )
            for revision_id in revision_ids:
                bundle = {
                    "evidence_format": PLUMEGRAPH_EVIDENCE_FORMAT,
                    "episode_revision": next(
                        row
                        for row in tables["episode_revisions"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ),
                    "geometries": [
                        row
                        for row in tables["episode_geometries"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ],
                    "candidate_sources": [
                        row
                        for row in tables["candidate_sources"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ],
                    "emission_estimates": [
                        row
                        for row in tables["emission_estimates"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ],
                    "evidence_pixels": [
                        row
                        for row in tables["evidence_pixels"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ],
                    "provenance": [
                        row
                        for row in tables["provenance"].to_pylist()
                        if str(row["episode_revision_id"]) == revision_id
                    ],
                }
                (evidence_dir / f"{revision_id}.json").write_text(
                    json.dumps(bundle, indent=2, sort_keys=True, default=str) + "\n"
                )
            source_snapshots = connection.execute(
                f"""
                SELECT snapshot_id, content_sha256, source_etag,
                       schema_fingerprint, source_lineage_json
                FROM {plumegraph_ops_tbl("source_snapshots")}
                ORDER BY snapshot_id
                """
            ).fetchall()
            normalized_artifacts = connection.execute(
                f"""
                SELECT normalized_artifact_id, source_snapshot_id, artifact_uri,
                       content_sha256, schema_fingerprint, row_count
                FROM {plumegraph_ops_tbl("normalized_artifacts")}
                ORDER BY normalized_artifact_id
                """
            ).fetchall()
            cohort_manifests = connection.execute(
                f"""
                SELECT cohort_version, manifest_sha256, source_manifest_sha256
                FROM {plumegraph_ops_tbl("cohort_manifests")}
                ORDER BY cohort_version
                """
            ).fetchall()
            files = sorted(path for path in temporary.rglob("*") if path.is_file())
            manifest = {
                "evidence_format": PLUMEGRAPH_EVIDENCE_FORMAT,
                "release_id": release_id,
                "release_version": release_version,
                "package_version": _package_version(),
                "contract_version": settings.contract["contract_version"],
                "algorithm_version": settings.contract["algorithm_version"],
                "cohort_versions": [str(row[0]) for row in cohort_manifests],
                "cohorts": [
                    {
                        "cohort_version": str(row[0]),
                        "manifest_sha256": str(row[1]),
                        "source_manifest_sha256": str(row[2]),
                    }
                    for row in cohort_manifests
                ],
                "analysis_generations": [
                    {
                        "analysis_region_id": str(row[0]),
                        "partition_date": str(row[1]),
                        "analysis_run_id": str(row[2]),
                    }
                    for row in generation_rows
                ],
                "analysis_manifest_sha256": analysis_manifest_sha,
                "validation_run_id": selected_validation_id,
                "probability_enabled": bool(validation[2]) if validation else False,
                "source_snapshots": [
                    {
                        "snapshot_id": str(row[0]),
                        "sha256": str(row[1]),
                        "etag": row[2],
                        "schema_fingerprint": str(row[3]),
                        "source_lineage": json.loads(str(row[4])),
                    }
                    for row in source_snapshots
                ],
                "normalized_artifacts": [
                    {
                        "normalized_artifact_id": str(row[0]),
                        "source_snapshot_id": str(row[1]),
                        "artifact_uri": str(row[2]),
                        "sha256": str(row[3]),
                        "schema_fingerprint": str(row[4]),
                        "rows": int(row[5]),
                    }
                    for row in normalized_artifacts
                ],
                "relations": {
                    name: {
                        "rows": table.num_rows,
                        "columns": table.column_names,
                    }
                    for name, table in tables.items()
                },
                "files": [
                    {
                        "path": path.relative_to(temporary).as_posix(),
                        "sha256": sha256_file(path),
                        "bytes": path.stat().st_size,
                    }
                    for path in files
                ],
            }
            manifest_path = temporary / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n"
            )
            manifest_sha = sha256_file(manifest_path)
            for item in manifest["files"]:
                path = temporary / item["path"]
                if sha256_file(path) != item["sha256"]:
                    raise ValueError(f"Release checksum mismatch for {item['path']}")
            if destination.exists():
                existing = destination / "manifest.json"
                if not existing.exists() or sha256_file(existing) != manifest_sha:
                    raise FileExistsError(
                        "An immutable PlumeGraph release version already exists"
                    )
                shutil.rmtree(temporary)
            else:
                os.replace(temporary, destination)
            published_at = _db_time(datetime.now(timezone.utc))
            episode_count = tables["episodes"].num_rows
            connection.execute(
                f"""
                INSERT OR IGNORE INTO {plumegraph_ops_tbl("release_manifests")}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    release_id,
                    PLUMEGRAPH_EVIDENCE_FORMAT,
                    release_version,
                    analysis_manifest_sha,
                    selected_validation_id,
                    destination.relative_to(release_root).as_posix(),
                    manifest_sha,
                    episode_count,
                    published_at,
                ],
            )
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
    return ReleaseMetrics(
        release_id=release_id,
        release_path=destination,
        manifest_sha256=manifest_sha,
        episode_count=episode_count,
        file_count=len(manifest["files"]) + 1,
    )


def verify_release(path: Path) -> dict[str, object]:
    manifest_path = path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("evidence_format") != PLUMEGRAPH_EVIDENCE_FORMAT:
        raise ValueError("Unsupported PlumeGraph evidence format")
    for item in manifest.get("files", []):
        artifact = (path / str(item["path"])).resolve()
        if not artifact.is_relative_to(path.resolve()):
            raise ValueError("PlumeGraph release manifest contains a path escape")
        if (
            not artifact.is_file()
            or sha256_file(artifact) != str(item["sha256"])
            or artifact.stat().st_size != int(item["bytes"])
        ):
            raise ValueError(f"PlumeGraph release artifact failed: {item['path']}")
    return manifest


__all__ = ["ReleaseMetrics", "build_release", "verify_release"]
