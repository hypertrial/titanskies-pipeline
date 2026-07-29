"""Pinned SWORD acquisition, mainstem selection, and network artifact publication."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import struct
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from titanskies_pipeline.geography.acquire import safe_extract, sha256_file
from titanskies_pipeline.geography.publish import atomic_json
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    riverpulse_ops_tbl,
    riverpulse_raw_tbl,
)

NETWORK_MANIFEST_VERSION = "1"
NETWORK_VERSION = "17b"
REACH_COLUMNS = (
    "network_version",
    "reach_id",
    "basin_key",
    "river_name",
    "reach_length_m",
    "flow_accumulation",
    "distance_to_outlet_m",
    "geometry_wkb",
    "centroid_longitude",
    "centroid_latitude",
    "is_outlet_anchor",
)
EDGE_COLUMNS = (
    "network_version",
    "from_reach_id",
    "to_reach_id",
    "is_selection_boundary",
)


@dataclass(frozen=True)
class NetworkArtifacts:
    manifest_path: Path
    build_id: str
    artifact_mode: str
    network_version: str
    source_manifest_sha256: str
    reaches_path: Path
    edges_path: Path
    reaches_sha256: str
    edges_sha256: str
    reach_count: int
    edge_count: int
    resolved_anchors: dict[str, str]


def _checksum(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_sword_source_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    if {
        "manifest_version",
        "network_version",
        "sources",
    } - set(manifest):
        raise ValueError("SWORD source manifest is incomplete")
    if str(manifest["network_version"]) != NETWORK_VERSION:
        raise ValueError("Unsupported SWORD source version")
    if not isinstance(manifest["sources"], list) or len(manifest["sources"]) != 1:
        raise ValueError("SWORD source manifest must contain exactly one source")
    for source in manifest["sources"]:
        required = {
            "id",
            "version",
            "url",
            "filename",
            "checksum_algorithm",
            "checksum",
            "attribution",
            "license",
        }
        if required - set(source):
            raise ValueError("SWORD source entry is incomplete")
        try:
            hashlib.new(str(source["checksum_algorithm"]))
        except ValueError as exc:
            raise ValueError("Unsupported SWORD checksum algorithm") from exc
    return manifest


def acquire_sword_archive(
    source: Mapping[str, str], *, source_cache: Path, offline: bool
) -> Path:
    source_cache.mkdir(parents=True, exist_ok=True)
    filename = Path(source["filename"])
    if filename.is_absolute() or filename.name != str(filename):
        raise ValueError("SWORD archive filename must be a cache-relative basename")
    destination = source_cache / filename
    expected = source["checksum"].lower()
    algorithm = source["checksum_algorithm"].lower()
    if destination.exists() and _checksum(destination, algorithm) == expected:
        return destination
    if destination.exists() and offline:
        raise ValueError("Cached SWORD archive failed checksum")
    if offline:
        raise FileNotFoundError(f"Verified SWORD archive is not cached: {destination}")
    fd, name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=source_cache
    )
    os.close(fd)
    temporary = Path(name)
    try:
        with requests.get(source["url"], stream=True, timeout=(30, 600)) as response:
            response.raise_for_status()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        actual = _checksum(temporary, algorithm)
        if actual != expected:
            raise ValueError(
                f"Downloaded SWORD archive failed checksum: expected {expected}, "
                f"got {actual}"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _neighbor_ids(value: object) -> list[str]:
    if value is None:
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        text = str(value).strip().strip("[]")
        values = text.replace(",", " ").split()
    return [
        str(item).strip().strip("'\"")
        for item in values
        if str(item).strip().strip("'\"") not in {"", "0", "-9999", "None", "nan"}
    ]


def _float(value: object) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result else None


def _record_value(record: Mapping[str, object], *names: str) -> object:
    casefolded = {str(key).casefold(): value for key, value in record.items()}
    for name in names:
        if name.casefold() in casefolded:
            return casefolded[name.casefold()]
    return None


def select_mainstem(
    records: Sequence[Mapping[str, object]],
    *,
    basin_key: str,
    aliases: Sequence[str],
    max_reaches: int = 100,
    anchor_reach_id: str | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    if max_reaches < 1 or max_reaches > 100:
        raise ValueError("RiverPulse corridor size must be between 1 and 100")
    by_id = {
        str(_record_value(record, "reach_id")): record
        for record in records
        if _record_value(record, "reach_id") is not None
    }
    if len(by_id) != len(records):
        raise ValueError("SWORD records contain duplicate or missing reach IDs")
    candidates = [
        record
        for record in records
        if any(
            alias.casefold()
            in str(_record_value(record, "river_name", "river")).casefold()
            for alias in aliases
        )
    ]
    if anchor_reach_id:
        try:
            anchor = by_id[str(anchor_reach_id)]
        except KeyError as exc:
            raise ValueError(
                f"Unknown reviewed outlet anchor: {anchor_reach_id}"
            ) from exc
        if anchor not in candidates:
            raise ValueError(
                f"Reviewed outlet anchor does not match pilot {basin_key}: "
                f"{anchor_reach_id}"
            )
    else:
        outlet_candidates = [
            record
            for record in candidates
            if _float(_record_value(record, "end_reach")) == 2
        ]
        anchor_pool = outlet_candidates or candidates
        if not anchor_pool:
            raise ValueError(f"No SWORD reaches found for pilot {basin_key}")
        anchor = min(
            anchor_pool,
            key=lambda record: (
                _float(_record_value(record, "dist_out", "distance_to_outlet_m"))
                if _float(_record_value(record, "dist_out", "distance_to_outlet_m"))
                is not None
                else float("inf"),
                str(_record_value(record, "reach_id")),
            ),
        )
    anchor_id = str(_record_value(anchor, "reach_id"))
    selected_ids: list[str] = []
    current_id = anchor_id
    while current_id not in selected_ids and len(selected_ids) < max_reaches:
        selected_ids.append(current_id)
        current = by_id[current_id]
        upstream = [
            by_id[neighbor]
            for neighbor in _neighbor_ids(
                _record_value(current, "rch_id_up", "reach_id_up")
            )
            if neighbor in by_id
        ]
        if not upstream:
            break
        current_id = str(
            _record_value(
                max(
                    upstream,
                    key=lambda record: (
                        _float(_record_value(record, "facc", "flow_accumulation"))
                        if _float(_record_value(record, "facc", "flow_accumulation"))
                        is not None
                        else float("-inf"),
                        str(_record_value(record, "reach_id")),
                    ),
                ),
                "reach_id",
            )
        )
    selected = set(selected_ids)
    reaches: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    for reach_id in selected_ids:
        record = by_id[reach_id]
        geometry = _record_value(record, "geometry_wkb")
        if not isinstance(geometry, bytes):
            geometry_obj = _record_value(record, "geometry")
            geometry = getattr(geometry_obj, "wkb", None)
        if not isinstance(geometry, bytes):
            raise ValueError(f"SWORD reach {reach_id} has no parseable geometry")
        centroid_lon = _float(_record_value(record, "x", "centroid_longitude"))
        centroid_lat = _float(_record_value(record, "y", "centroid_latitude"))
        geometry_obj = _record_value(record, "geometry")
        if (centroid_lon is None or centroid_lat is None) and geometry_obj is not None:
            centroid_lon = float(geometry_obj.centroid.x)
            centroid_lat = float(geometry_obj.centroid.y)
        if centroid_lon is None or centroid_lat is None:
            raise ValueError(f"SWORD reach {reach_id} has no centroid")
        reaches.append(
            {
                "network_version": NETWORK_VERSION,
                "reach_id": reach_id,
                "basin_key": basin_key,
                "river_name": str(
                    _record_value(record, "river_name", "river") or aliases[0]
                ),
                "reach_length_m": _float(
                    _record_value(record, "reach_len", "reach_length", "length")
                ),
                "flow_accumulation": _float(
                    _record_value(record, "facc", "flow_accumulation")
                ),
                "distance_to_outlet_m": _float(
                    _record_value(record, "dist_out", "distance_to_outlet_m")
                ),
                "geometry_wkb": geometry,
                "centroid_longitude": centroid_lon,
                "centroid_latitude": centroid_lat,
                "is_outlet_anchor": reach_id == anchor_id,
            }
        )
        neighbors = set(
            _neighbor_ids(_record_value(record, "rch_id_up", "reach_id_up"))
            + _neighbor_ids(_record_value(record, "rch_id_dn", "rch_id_down"))
        )
        for neighbor in sorted(neighbors):
            if neighbor == reach_id:
                raise ValueError(f"SWORD reach {reach_id} has a self-edge")
            edges.append(
                {
                    "network_version": NETWORK_VERSION,
                    "from_reach_id": reach_id,
                    "to_reach_id": neighbor,
                    "is_selection_boundary": neighbor not in selected,
                }
            )
            if neighbor in selected:
                reciprocal = set(
                    _neighbor_ids(
                        _record_value(by_id[neighbor], "rch_id_up", "reach_id_up")
                    )
                    + _neighbor_ids(
                        _record_value(by_id[neighbor], "rch_id_dn", "rch_id_down")
                    )
                )
                if reach_id not in reciprocal:
                    raise ValueError(
                        f"Inconsistent reciprocal topology: {reach_id}, {neighbor}"
                    )
    deduped_edges = {
        (edge["from_reach_id"], edge["to_reach_id"]): edge for edge in edges
    }
    return reaches, list(deduped_edges.values()), anchor_id


def _line_wkb(points: Sequence[tuple[float, float]]) -> bytes:
    payload = bytearray(struct.pack("<BI", 1, 2))
    payload.extend(struct.pack("<I", len(points)))
    for longitude, latitude in points:
        payload.extend(struct.pack("<dd", longitude, latitude))
    return bytes(payload)


def synthetic_network_rows() -> tuple[
    list[dict[str, object]], list[dict[str, object]], dict[str, str]
]:
    systems = (
        ("sacramento", "Sacramento River", -121.8, 38.1),
        ("rhine", "Rhine", 7.6, 50.3),
        ("murray", "Murray River", 144.7, -35.2),
    )
    reaches: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    anchors: dict[str, str] = {}
    for basin_index, (basin, river, x, y) in enumerate(systems, start=1):
        ids = [f"RP{basin_index}00{index}" for index in range(1, 4)]
        anchors[basin] = ids[0]
        for index, reach_id in enumerate(ids):
            reaches.append(
                {
                    "network_version": NETWORK_VERSION,
                    "reach_id": reach_id,
                    "basin_key": basin,
                    "river_name": river,
                    "reach_length_m": 10_000.0 + index * 500,
                    "flow_accumulation": 300.0 - index * 50,
                    "distance_to_outlet_m": index * 10_000.0,
                    "geometry_wkb": _line_wkb(
                        [(x + index * 0.1, y), (x + (index + 1) * 0.1, y + 0.1)]
                    ),
                    "centroid_longitude": x + (index + 0.5) * 0.1,
                    "centroid_latitude": y + 0.05,
                    "is_outlet_anchor": index == 0,
                }
            )
        for downstream, upstream in zip(ids, ids[1:]):
            edges.extend(
                [
                    {
                        "network_version": NETWORK_VERSION,
                        "from_reach_id": downstream,
                        "to_reach_id": upstream,
                        "is_selection_boundary": False,
                    },
                    {
                        "network_version": NETWORK_VERSION,
                        "from_reach_id": upstream,
                        "to_reach_id": downstream,
                        "is_selection_boundary": False,
                    },
                ]
            )
        edges.append(
            {
                "network_version": NETWORK_VERSION,
                "from_reach_id": ids[-1],
                "to_reach_id": f"BOUNDARY-{basin.upper()}",
                "is_selection_boundary": True,
            }
        )
    return reaches, edges, anchors


def _validate_network_tables(reaches: pa.Table, edges: pa.Table) -> None:
    if set(REACH_COLUMNS) - set(reaches.column_names):
        raise ValueError("RiverPulse reaches artifact schema is incomplete")
    if set(EDGE_COLUMNS) - set(edges.column_names):
        raise ValueError("RiverPulse edges artifact schema is incomplete")
    reach_ids = [str(value) for value in reaches["reach_id"].to_pylist()]
    if not reach_ids or len(reach_ids) != len(set(reach_ids)):
        raise ValueError("RiverPulse reaches contain duplicate or missing IDs")
    reach_set = set(reach_ids)
    edge_pairs: set[tuple[str, str]] = set()
    for edge in edges.to_pylist():
        pair = (str(edge["from_reach_id"]), str(edge["to_reach_id"]))
        if pair[0] == pair[1]:
            raise ValueError("RiverPulse network contains a self-edge")
        if pair in edge_pairs:
            raise ValueError("RiverPulse network contains duplicate edges")
        edge_pairs.add(pair)
        if pair[0] not in reach_set:
            raise ValueError("RiverPulse edge starts outside the selected network")
        if not edge["is_selection_boundary"] and pair[1] not in reach_set:
            raise ValueError("RiverPulse internal edge points outside the network")
    for start, end in edge_pairs:
        row = next(
            edge
            for edge in edges.to_pylist()
            if str(edge["from_reach_id"]) == start and str(edge["to_reach_id"]) == end
        )
        if not row["is_selection_boundary"] and (end, start) not in edge_pairs:
            raise ValueError("RiverPulse internal topology is not reciprocal")
    counts: dict[str, int] = {}
    for basin in reaches["basin_key"].to_pylist():
        counts[str(basin)] = counts.get(str(basin), 0) + 1
    if any(count > 100 for count in counts.values()):
        raise ValueError("RiverPulse pilot corridor exceeds 100 reaches")


def _validate_resolved_anchors(
    reaches: pa.Table, resolved_anchors: Mapping[str, str]
) -> None:
    expected = {str(key): str(value) for key, value in resolved_anchors.items()}
    actual: dict[str, list[str]] = {}
    for row in reaches.select(
        ["basin_key", "reach_id", "is_outlet_anchor"]
    ).to_pylist():
        if row["is_outlet_anchor"]:
            actual.setdefault(str(row["basin_key"]), []).append(str(row["reach_id"]))
    basins = {str(value) for value in reaches["basin_key"].to_pylist()}
    if set(expected) != basins or any(
        actual.get(basin) != [anchor] for basin, anchor in expected.items()
    ):
        raise ValueError("RiverPulse resolved anchors do not match reach artifacts")


def publish_network_generation(
    *,
    output_dir: Path,
    reaches: Iterable[Mapping[str, object]],
    edges: Iterable[Mapping[str, object]],
    artifact_mode: str,
    source_manifest_sha256: str,
    resolved_anchors: Mapping[str, str],
) -> NetworkArtifacts:
    if artifact_mode not in {"production", "synthetic"}:
        raise ValueError("Unknown RiverPulse artifact mode")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        b"network_version": NETWORK_VERSION.encode(),
        b"source_manifest_sha256": source_manifest_sha256.encode(),
    }
    reaches_table = (
        pa.Table.from_pylist(list(reaches))
        .select(REACH_COLUMNS)
        .sort_by([("basin_key", "ascending"), ("reach_id", "ascending")])
        .replace_schema_metadata(metadata)
    )
    edges_table = (
        pa.Table.from_pylist(list(edges))
        .select(EDGE_COLUMNS)
        .sort_by([("from_reach_id", "ascending"), ("to_reach_id", "ascending")])
        .replace_schema_metadata(metadata)
    )
    _validate_network_tables(reaches_table, edges_table)
    _validate_resolved_anchors(reaches_table, resolved_anchors)
    build_dir = Path(tempfile.mkdtemp(prefix=".build.", dir=output_dir))
    try:
        reaches_source = build_dir / "reaches.parquet"
        edges_source = build_dir / "reach_edges.parquet"
        pq.write_table(reaches_table, reaches_source, compression="zstd")
        pq.write_table(edges_table, edges_source, compression="zstd")
        reaches_sha = sha256_file(reaches_source)
        edges_sha = sha256_file(edges_source)
        identity = {
            "artifact_mode": artifact_mode,
            "network_version": NETWORK_VERSION,
            "source_manifest_sha256": source_manifest_sha256,
            "reaches_sha256": reaches_sha,
            "edges_sha256": edges_sha,
            "resolved_anchors": dict(sorted(resolved_anchors.items())),
        }
        build_id = hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:20]
        generation = output_dir / "generations" / build_id
        if not generation.exists():
            generation.parent.mkdir(parents=True, exist_ok=True)
            temporary = generation.with_name(f".{build_id}.{os.getpid()}.tmp")
            temporary.mkdir()
            try:
                os.replace(reaches_source, temporary / "reaches.parquet")
                os.replace(edges_source, temporary / "reach_edges.parquet")
                os.replace(temporary, generation)
            finally:
                shutil.rmtree(temporary, ignore_errors=True)
        else:
            if (
                sha256_file(generation / "reaches.parquet") != reaches_sha
                or sha256_file(generation / "reach_edges.parquet") != edges_sha
            ):
                raise ValueError("Existing RiverPulse network generation is corrupt")
        manifest = {
            "manifest_version": NETWORK_MANIFEST_VERSION,
            "build_id": build_id,
            "artifact_mode": artifact_mode,
            "network_version": NETWORK_VERSION,
            "source_manifest_sha256": source_manifest_sha256,
            "resolved_anchors": dict(sorted(resolved_anchors.items())),
            "reaches": {
                "path": f"generations/{build_id}/reaches.parquet",
                "sha256": reaches_sha,
                "row_count": reaches_table.num_rows,
            },
            "edges": {
                "path": f"generations/{build_id}/reach_edges.parquet",
                "sha256": edges_sha,
                "row_count": edges_table.num_rows,
            },
        }
        manifest_path = output_dir / "riverpulse_network_artifacts.json"
        atomic_json(manifest, manifest_path)
        return load_network_artifacts(manifest_path, allow_synthetic=True)
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def _contained(root: Path, relative: object) -> Path:
    candidate = Path(str(relative))
    if candidate.is_absolute():
        raise ValueError("RiverPulse artifact paths must be root-relative")
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("RiverPulse artifact path escapes its root")
    return resolved


def load_network_artifacts(
    manifest_path: Path, *, allow_synthetic: bool = False
) -> NetworkArtifacts:
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text())
    required = {
        "manifest_version",
        "build_id",
        "artifact_mode",
        "network_version",
        "source_manifest_sha256",
        "resolved_anchors",
        "reaches",
        "edges",
    }
    if required - set(manifest):
        raise ValueError("RiverPulse network manifest is incomplete")
    if str(manifest["manifest_version"]) != NETWORK_MANIFEST_VERSION:
        raise ValueError("Unsupported RiverPulse network manifest version")
    if str(manifest["network_version"]) != NETWORK_VERSION:
        raise ValueError("Unsupported SWORD network version")
    mode = str(manifest["artifact_mode"])
    if mode not in {"production", "synthetic"}:
        raise ValueError("Unknown RiverPulse network artifact mode")
    if mode == "synthetic" and not allow_synthetic:
        raise ValueError("Production collection requires a production SWORD network")
    root = manifest_path.parent
    reaches_path = _contained(root, manifest["reaches"]["path"])
    edges_path = _contained(root, manifest["edges"]["path"])
    for label, path, info in (
        ("reaches", reaches_path, manifest["reaches"]),
        ("edges", edges_path, manifest["edges"]),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"RiverPulse {label} artifact not found: {path}")
        if sha256_file(path) != info["sha256"]:
            raise ValueError(f"RiverPulse {label} checksum mismatch")
    reaches_file = pq.ParquetFile(reaches_path)
    edges_file = pq.ParquetFile(edges_path)
    if (
        reaches_file.metadata.num_rows != manifest["reaches"]["row_count"]
        or edges_file.metadata.num_rows != manifest["edges"]["row_count"]
    ):
        raise ValueError("RiverPulse artifact row count mismatch")
    expected_metadata = {
        b"network_version": NETWORK_VERSION.encode(),
        b"source_manifest_sha256": str(manifest["source_manifest_sha256"]).encode(),
    }
    if any(
        (file.schema_arrow.metadata or {}).get(key) != value
        for file in (reaches_file, edges_file)
        for key, value in expected_metadata.items()
    ):
        raise ValueError("RiverPulse artifact metadata mismatch")
    reaches_table = reaches_file.read()
    edges_table = edges_file.read()
    resolved_anchors = {
        str(key): str(value) for key, value in manifest["resolved_anchors"].items()
    }
    _validate_network_tables(reaches_table, edges_table)
    _validate_resolved_anchors(reaches_table, resolved_anchors)
    identity = {
        "artifact_mode": mode,
        "network_version": NETWORK_VERSION,
        "source_manifest_sha256": str(manifest["source_manifest_sha256"]),
        "reaches_sha256": str(manifest["reaches"]["sha256"]),
        "edges_sha256": str(manifest["edges"]["sha256"]),
        "resolved_anchors": dict(sorted(resolved_anchors.items())),
    }
    expected_build_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:20]
    if str(manifest["build_id"]) != expected_build_id:
        raise ValueError("RiverPulse network build identity mismatch")
    return NetworkArtifacts(
        manifest_path=manifest_path,
        build_id=str(manifest["build_id"]),
        artifact_mode=mode,
        network_version=NETWORK_VERSION,
        source_manifest_sha256=str(manifest["source_manifest_sha256"]),
        reaches_path=reaches_path,
        edges_path=edges_path,
        reaches_sha256=str(manifest["reaches"]["sha256"]),
        edges_sha256=str(manifest["edges"]["sha256"]),
        reach_count=int(manifest["reaches"]["row_count"]),
        edge_count=int(manifest["edges"]["row_count"]),
        resolved_anchors=resolved_anchors,
    )


def persist_network_artifacts(
    artifacts: NetworkArtifacts, *, conn=None
) -> dict[str, int]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    reaches = pq.read_table(artifacts.reaches_path).append_column(
        "loaded_at", pa.array([now] * artifacts.reach_count)
    )
    edges = pq.read_table(artifacts.edges_path).append_column(
        "loaded_at", pa.array([now] * artifacts.edge_count)
    )
    with _use_conn(conn) as connection:
        existing = connection.execute(
            f"SELECT build_id, network_version FROM "
            f"{riverpulse_ops_tbl('network_artifact_manifest')} LIMIT 1"
        ).fetchone()
        has_observations = connection.execute(
            f"SELECT 1 FROM {riverpulse_raw_tbl('observation_revisions')} LIMIT 1"
        ).fetchone()
        if (
            existing
            and (
                str(existing[0]) != artifacts.build_id
                or str(existing[1]) != artifacts.network_version
            )
            and has_observations
        ):
            raise RuntimeError(
                "SWORD network generation changes require a clean warehouse rebuild"
            )
        connection.register("_riverpulse_reaches", reaches)
        connection.register("_riverpulse_edges", edges)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(f"DELETE FROM {riverpulse_raw_tbl('reach_edges')}")
            connection.execute(f"DELETE FROM {riverpulse_raw_tbl('reaches')}")
            connection.execute(
                f"""
                INSERT INTO {riverpulse_raw_tbl("reaches")}
                SELECT network_version, reach_id, basin_key, river_name,
                       reach_length_m, flow_accumulation, distance_to_outlet_m,
                       geometry_wkb, centroid_longitude, centroid_latitude,
                       is_outlet_anchor, loaded_at
                FROM _riverpulse_reaches
                """
            )
            connection.execute(
                f"""
                INSERT INTO {riverpulse_raw_tbl("reach_edges")}
                SELECT network_version, from_reach_id, to_reach_id,
                       is_selection_boundary, loaded_at
                FROM _riverpulse_edges
                """
            )
            connection.execute(
                f"DELETE FROM {riverpulse_ops_tbl('network_artifact_manifest')}"
            )
            connection.execute(
                f"""
                INSERT INTO {riverpulse_ops_tbl("network_artifact_manifest")}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    artifacts.build_id,
                    artifacts.artifact_mode,
                    artifacts.network_version,
                    artifacts.source_manifest_sha256,
                    str(
                        artifacts.reaches_path.relative_to(
                            artifacts.manifest_path.parent
                        )
                    ),
                    str(
                        artifacts.edges_path.relative_to(artifacts.manifest_path.parent)
                    ),
                    artifacts.reaches_sha256,
                    artifacts.edges_sha256,
                    artifacts.reach_count,
                    artifacts.edge_count,
                    json.dumps(artifacts.resolved_anchors, sort_keys=True),
                    now,
                ],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.unregister("_riverpulse_reaches")
            connection.unregister("_riverpulse_edges")
    return {
        "reaches_loaded": artifacts.reach_count,
        "edges_loaded": artifacts.edge_count,
    }


def build_production_network(
    *,
    output_dir: Path,
    source_cache: Path,
    source_manifest_path: Path,
    pilots_path: Path,
    offline: bool,
) -> NetworkArtifacts:
    manifest = load_sword_source_manifest(source_manifest_path)
    source = manifest["sources"][0]
    archive = acquire_sword_archive(source, source_cache=source_cache, offline=offline)
    extracted = safe_extract(archive, source_cache / "extracted")
    pilots = json.loads(pilots_path.read_text())
    all_reaches: list[dict[str, object]] = []
    all_edges: list[dict[str, object]] = []
    anchors: dict[str, str] = {}
    try:
        import geopandas as gpd
    except ImportError as exc:  # pragma: no cover - optional production dependency
        raise RuntimeError("Production SWORD builds require the geo extra") from exc
    for system in pilots["systems"]:
        continent = str(system["continent"]).casefold()
        continent_files = sorted(
            path
            for path in extracted.rglob("*.gpkg")
            if continent in path.name.casefold()
        )
        if not continent_files:
            raise FileNotFoundError(
                f"SWORD reach GeoPackage not found for continent {continent}"
            )
        reach_files = [
            path for path in continent_files if "reach" in path.name.casefold()
        ]
        source_candidates = reach_files or continent_files
        if len(source_candidates) != 1:
            raise ValueError(f"Ambiguous SWORD GeoPackages for continent {continent}")
        source_path = source_candidates[0]
        read_options: dict[str, str] = {}
        if hasattr(gpd, "list_layers"):
            layers = [
                str(value)
                for value in gpd.list_layers(source_path)["name"].tolist()
                if "reach" in str(value).casefold()
            ]
            if len(layers) > 1:
                raise ValueError(
                    f"Ambiguous SWORD reach layers for continent {continent}"
                )
            if layers:
                read_options["layer"] = layers[0]
        frame = gpd.read_file(source_path, **read_options)
        rows = frame.to_dict("records")
        reaches, edges, anchor = select_mainstem(
            rows,
            basin_key=str(system["basin_key"]),
            aliases=[str(alias) for alias in system["river_name_aliases"]],
            max_reaches=int(pilots["max_reaches_per_system"]),
            anchor_reach_id=system.get("anchor_reach_id"),
        )
        all_reaches.extend(reaches)
        all_edges.extend(edges)
        anchors[str(system["basin_key"])] = anchor
    source_manifest_sha = sha256_file(source_manifest_path)
    return publish_network_generation(
        output_dir=output_dir,
        reaches=all_reaches,
        edges=all_edges,
        artifact_mode="production",
        source_manifest_sha256=source_manifest_sha,
        resolved_anchors=anchors,
    )


__all__ = [
    "EDGE_COLUMNS",
    "NETWORK_VERSION",
    "NetworkArtifacts",
    "REACH_COLUMNS",
    "acquire_sword_archive",
    "build_production_network",
    "load_network_artifacts",
    "load_sword_source_manifest",
    "persist_network_artifacts",
    "publish_network_generation",
    "select_mainstem",
    "synthetic_network_rows",
]
