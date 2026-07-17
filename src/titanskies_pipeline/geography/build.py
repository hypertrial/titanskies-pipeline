"""Pinned production-geography download, normalization, and grid weighting."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from titanskies_pipeline.geography.registry import REGISTRY_COLUMNS, WEIGHT_COLUMNS
from titanskies_pipeline.geography.tempo_grid import GRID_VERSION, TEMPO_GRID

CANADA_PROVINCES = {
    "10": "NL",
    "11": "PE",
    "12": "NS",
    "13": "NB",
    "24": "QC",
    "35": "ON",
    "46": "MB",
    "47": "SK",
    "48": "AB",
    "59": "BC",
    "60": "YT",
    "61": "NT",
    "62": "NU",
}

MEXICO_STATES = {
    "01": "AGU",
    "02": "BCN",
    "03": "BCS",
    "04": "CAM",
    "05": "COA",
    "06": "COL",
    "07": "CHP",
    "08": "CHH",
    "09": "CMX",
    "10": "DUR",
    "11": "GUA",
    "12": "GRO",
    "13": "HID",
    "14": "JAL",
    "15": "MEX",
    "16": "MIC",
    "17": "MOR",
    "18": "NAY",
    "19": "NLE",
    "20": "OAX",
    "21": "PUE",
    "22": "QUE",
    "23": "ROO",
    "24": "SLP",
    "25": "SIN",
    "26": "SON",
    "27": "TAB",
    "28": "TAM",
    "29": "TLA",
    "30": "VER",
    "31": "YUC",
    "32": "ZAC",
}

COUNTRY_NAMES = {
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
}

ARTIFACT_MANIFEST_VERSION = "1"
ARTIFACT_MANIFEST_NAME = "tempo_geography_artifacts.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_source_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    required = {"manifest_version", "geometry_version", "sources"}
    if required - set(manifest):
        raise ValueError("Geography source manifest is incomplete")
    for source in manifest["sources"]:
        missing = {
            "id",
            "version",
            "url",
            "filename",
            "sha256",
            "attribution",
            "license",
        } - set(source)
        if missing:
            raise ValueError(
                f"Geography source {source.get('id', '<unknown>')} is incomplete"
            )
        if len(source["sha256"]) != 64:
            raise ValueError(f"Geography source {source['id']} has an invalid SHA-256")
    return manifest


def acquire_source(
    source: Mapping[str, str],
    *,
    source_cache: Path,
    offline: bool,
) -> Path:
    source_cache.mkdir(parents=True, exist_ok=True)
    destination = source_cache / source["filename"]
    if destination.exists() and sha256_file(destination) == source["sha256"]:
        return destination
    if destination.exists() and offline:
        raise ValueError(f"Cached geography source failed checksum: {source['id']}")
    if offline:
        raise FileNotFoundError(
            f"Offline geography source is not cached: {source['id']} ({destination})"
        )

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=source_cache
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with requests.get(source["url"], stream=True, timeout=(30, 300)) as response:
            response.raise_for_status()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        actual = sha256_file(temporary)
        if actual != source["sha256"]:
            raise ValueError(
                f"Downloaded geography source failed checksum: {source['id']} "
                f"(expected {source['sha256']}, got {actual})"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def acquire_all_sources(
    manifest: Mapping[str, Any],
    *,
    source_cache: Path,
    offline: bool,
) -> dict[str, Path]:
    return {
        source["id"]: acquire_source(source, source_cache=source_cache, offline=offline)
        for source in manifest["sources"]
    }


def _geo_modules():
    try:
        import geopandas as gpd
        import shapely
        from pyproj import Transformer
    except ImportError as exc:  # pragma: no cover - exercised without geo extra
        raise RuntimeError(
            "Install geography dependencies: uv sync --locked --extra geo"
        ) from exc
    return gpd, shapely, Transformer


def _safe_extract(archive: Path, destination: Path) -> Path:
    archive_checksum = sha256_file(archive)
    destination = destination / archive_checksum
    marker = destination / ".complete"
    if marker.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{archive_checksum}.", dir=destination.parent)
    )
    try:
        with zipfile.ZipFile(archive) as zipped:
            for member in zipped.infolist():
                resolved = (temporary / member.filename).resolve()
                if not resolved.is_relative_to(temporary.resolve()):
                    raise ValueError(
                        f"Unsafe path in geography archive: {member.filename}"
                    )
            zipped.extractall(temporary)
        (temporary / ".complete").touch()
        try:
            os.replace(temporary, destination)
        except FileExistsError:
            pass
    finally:
        if temporary.exists():
            import shutil

            shutil.rmtree(temporary)
    return destination


def _find_file(root: Path, *suffixes: str) -> Path:
    matches = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and any(path.name.casefold().endswith(suffix.casefold()) for suffix in suffixes)
    )
    if not matches:
        raise FileNotFoundError(f"None of {suffixes!r} found below {root}")
    return matches[0]


def _require_columns(frame, columns: set[str], provider: str) -> None:
    missing = columns - set(frame.columns)
    if missing:
        raise ValueError(
            f"{provider} boundary is missing required fields: "
            f"{', '.join(sorted(missing))}"
        )


def _repair_dissolve(frame, *, by: str):
    _gpd, shapely, _transformer = _geo_modules()
    normalized = frame.to_crs(4326).copy()
    normalized.geometry = shapely.make_valid(normalized.geometry.array)
    normalized = normalized[~normalized.geometry.is_empty & normalized.geometry.notna()]
    normalized = normalized.dissolve(by=by, as_index=False, sort=True)
    normalized.geometry = shapely.make_valid(normalized.geometry.array)
    if normalized.empty or not normalized.geometry.is_valid.all():
        raise ValueError(f"Invalid geometry remained after dissolving by {by}")
    return normalized


def _canonical_frame(frame, rows: Iterable[dict[str, Any]]):
    gpd, _shapely, _transformer = _geo_modules()
    metadata = list(rows)
    result = gpd.GeoDataFrame(metadata, geometry=frame.geometry.array, crs=frame.crs)
    return _repair_dissolve(result, by="canonical_region_id")


def _provider_regions(paths: Mapping[str, Path], source_cache: Path):
    gpd, _shapely, _transformer = _geo_modules()
    import pandas as pd

    extracted = {
        key: _safe_extract(path, source_cache / "extracted" / key)
        for key, path in paths.items()
    }

    us_states = gpd.read_file(_find_file(extracted["us_states_2025"], ".shp"))
    us_counties = gpd.read_file(_find_file(extracted["us_counties_2025"], ".shp"))
    _require_columns(us_states, {"STATEFP", "STUSPS", "NAME"}, "US states")
    _require_columns(
        us_counties, {"STATEFP", "COUNTYFP", "GEOID", "NAME"}, "US counties"
    )
    state_by_fips = dict(zip(us_states["STATEFP"], us_states["STUSPS"], strict=True))
    us_first = _canonical_frame(
        us_states,
        (
            {
                "country_code": "US",
                "region_type": "state",
                "source_region_id": str(row.STATEFP),
                "canonical_region_id": f"US-{row.STUSPS}",
                "region_name": str(row.NAME),
                "parent_region_id": "US",
            }
            for row in us_states.itertuples()
        ),
    )
    us_finest = _canonical_frame(
        us_counties,
        (
            {
                "country_code": "US",
                "region_type": "county",
                "source_region_id": str(row.GEOID),
                "canonical_region_id": (
                    f"US-{state_by_fips[str(row.STATEFP)]}-{str(row.COUNTYFP).zfill(3)}"
                ),
                "region_name": str(row.NAME),
                "parent_region_id": f"US-{state_by_fips[str(row.STATEFP)]}",
            }
            for row in us_counties.itertuples()
        ),
    )

    canada = gpd.read_file(_find_file(extracted["canada_csd_2025"], ".shp"))
    _require_columns(canada, {"PRUID", "CSDUID", "CSDNAME", "PRNAME"}, "Canadian CSD")
    ca_finest = _canonical_frame(
        canada,
        (
            {
                "country_code": "CA",
                "region_type": "census_subdivision",
                "source_region_id": str(row.CSDUID),
                "canonical_region_id": (
                    f"CA-{CANADA_PROVINCES[str(row.PRUID)]}-{row.CSDUID}"
                ),
                "region_name": str(row.CSDNAME),
                "parent_region_id": f"CA-{CANADA_PROVINCES[str(row.PRUID)]}",
                "first_name": str(row.PRNAME),
            }
            for row in canada.itertuples()
        ),
    )
    ca_first = _derive_parents(ca_finest, country="CA", region_type="province")

    mexico_root = extracted["mexico_geostatistical_2025"]
    mexico_states = gpd.read_file(_find_file(mexico_root, "00ent.shp"))
    mexico_municipalities = gpd.read_file(_find_file(mexico_root, "00mun.shp"))
    _require_columns(mexico_states, {"CVE_ENT", "NOMGEO"}, "Mexican AGEE")
    _require_columns(
        mexico_municipalities,
        {"CVE_ENT", "CVE_MUN", "NOMGEO"},
        "Mexican AGEM",
    )
    mx_first = _canonical_frame(
        mexico_states,
        (
            {
                "country_code": "MX",
                "region_type": "state",
                "source_region_id": str(row.CVE_ENT).zfill(2),
                "canonical_region_id": (
                    f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}"
                ),
                "region_name": str(row.NOMGEO),
                "parent_region_id": "MX",
            }
            for row in mexico_states.itertuples()
        ),
    )
    mx_finest = _canonical_frame(
        mexico_municipalities,
        (
            {
                "country_code": "MX",
                "region_type": "municipality",
                "source_region_id": (
                    f"{str(row.CVE_ENT).zfill(2)}{str(row.CVE_MUN).zfill(3)}"
                ),
                "canonical_region_id": (
                    f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}-"
                    f"{str(row.CVE_MUN).zfill(3)}"
                ),
                "region_name": str(row.NOMGEO),
                "parent_region_id": (f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}"),
            }
            for row in mexico_municipalities.itertuples()
        ),
    )

    frames = [us_first, us_finest, ca_first, ca_finest, mx_first, mx_finest]
    countries = _derive_countries([us_first, ca_first, mx_first])
    regions = gpd.GeoDataFrame(
        pd.concat([countries, *frames], ignore_index=True), crs=4326
    )
    timezone_path = _find_file(extracted["land_timezones_2026b"], ".geojson", ".json")
    timezones = gpd.read_file(timezone_path).to_crs(4326)
    _require_columns(timezones, {"tzid"}, "timezone")
    return regions.sort_values("canonical_region_id").reset_index(drop=True), timezones


def _derive_parents(finest, *, country: str, region_type: str):
    gpd, _shapely, _transformer = _geo_modules()
    rows = []
    for parent_id, group in finest.groupby("parent_region_id", sort=True):
        rows.append(
            {
                "country_code": country,
                "region_type": region_type,
                "source_region_id": parent_id.split("-")[-1],
                "canonical_region_id": parent_id,
                "region_name": str(group.iloc[0].get("first_name", parent_id)),
                "parent_region_id": country,
                "geometry": group.geometry.union_all(),
            }
        )
    return _repair_dissolve(gpd.GeoDataFrame(rows, crs=4326), by="canonical_region_id")


def _derive_countries(first_frames):
    gpd, _shapely, _transformer = _geo_modules()
    rows = []
    for frame in first_frames:
        country = str(frame.iloc[0].country_code)
        rows.append(
            {
                "country_code": country,
                "region_type": "country",
                "source_region_id": country,
                "canonical_region_id": country,
                "region_name": COUNTRY_NAMES[country],
                "parent_region_id": None,
                "geometry": frame.geometry.union_all(),
            }
        )
    return gpd.GeoDataFrame(rows, crs=4326)


def assign_dominant_timezones(regions, timezones):
    _gpd, shapely, Transformer = _geo_modules()
    project = Transformer.from_crs(4326, 6933, always_xy=True)
    timezone_geometries = timezones.geometry.array
    timezone_ids = timezones["tzid"].astype(str).to_numpy()
    index = timezones.sindex
    assignments: list[str] = []
    for region in regions.geometry.array:
        candidates = list(index.query(region, predicate="intersects"))
        if not candidates:
            raise ValueError("A region has no intersecting IANA land timezone")
        intersections = shapely.intersection(timezone_geometries[candidates], region)
        projected = shapely.transform(
            intersections, project.transform, interleaved=False
        )
        areas = shapely.area(projected)
        assignments.append(str(timezone_ids[candidates[int(np.argmax(areas))]]))
    return assignments


def _geometry_checksum(geometry) -> str:
    _gpd, shapely, _transformer = _geo_modules()
    return hashlib.sha256(shapely.to_wkb(geometry, hex=False)).hexdigest()


def region_registry_table(regions, *, geometry_version: str) -> pa.Table:
    records = []
    for row in regions.itertuples():
        records.append(
            {
                "country_code": row.country_code,
                "region_type": row.region_type,
                "source_region_id": row.source_region_id,
                "canonical_region_id": row.canonical_region_id,
                "region_name": row.region_name,
                "parent_region_id": row.parent_region_id,
                "timezone": row.timezone,
                "geometry_version": geometry_version,
                "geometry_checksum": _geometry_checksum(row.geometry),
            }
        )
    return pa.Table.from_pylist(records).select(REGISTRY_COLUMNS)


def iter_region_weight_tables(
    regions,
    *,
    geometry_version: str,
    row_chunk_size: int = 64,
):
    if row_chunk_size < 1:
        raise ValueError("row_chunk_size must be positive")
    _gpd, shapely, Transformer = _geo_modules()
    project = Transformer.from_crs(4326, 6933, always_xy=True)
    half = TEMPO_GRID.step_degrees / 2
    for region in regions.sort_values("canonical_region_id").itertuples():
        region_geometry = region.geometry
        shapely.prepare(region_geometry)
        minx, miny, maxx, maxy = region_geometry.bounds
        row_start = max(
            0,
            int(
                np.ceil(
                    (miny - half - TEMPO_GRID.latitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        row_end = min(
            TEMPO_GRID.rows - 1,
            int(
                np.floor(
                    (maxy + half - TEMPO_GRID.latitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        col_start = max(
            0,
            int(
                np.ceil(
                    (minx - half - TEMPO_GRID.longitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        col_end = min(
            TEMPO_GRID.cols - 1,
            int(
                np.floor(
                    (maxx + half - TEMPO_GRID.longitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        if row_start > row_end or col_start > col_end:
            continue
        for chunk_start in range(row_start, row_end + 1, row_chunk_size):
            chunk_end = min(row_end + 1, chunk_start + row_chunk_size)
            rows = np.repeat(np.arange(chunk_start, chunk_end), col_end - col_start + 1)
            cols = np.tile(np.arange(col_start, col_end + 1), chunk_end - chunk_start)
            lat = TEMPO_GRID.latitude_start + rows * TEMPO_GRID.step_degrees
            lon = TEMPO_GRID.longitude_start + cols * TEMPO_GRID.step_degrees
            cells = shapely.box(lon - half, lat - half, lon + half, lat + half)
            intersects = shapely.intersects(region_geometry, cells)
            if not np.any(intersects):
                continue
            selected_cells = cells[intersects]
            contained = shapely.contains(region_geometry, selected_cells)
            area = np.empty(selected_cells.size, dtype=float)
            if np.any(contained):
                projected_cells = shapely.transform(
                    selected_cells[contained], project.transform, interleaved=False
                )
                area[contained] = shapely.area(projected_cells) / 1_000_000.0
            boundary = ~contained
            clipped = shapely.intersection(selected_cells[boundary], region_geometry)
            projected_clips = shapely.transform(
                clipped, project.transform, interleaved=False
            )
            area[boundary] = shapely.area(projected_clips) / 1_000_000.0
            keep = area > 0
            if not np.any(keep):
                continue
            selected_rows = rows[intersects][keep]
            selected_cols = cols[intersects][keep]
            yield pa.table(
                {
                    "grid_row": pa.array(selected_rows, type=pa.int32()),
                    "grid_col": pa.array(selected_cols, type=pa.int32()),
                    "canonical_region_id": [region.canonical_region_id]
                    * int(keep.sum()),
                    "overlap_weight": area[keep],
                    "geometry_version": [geometry_version] * int(keep.sum()),
                }
            ).select(WEIGHT_COLUMNS)


def _atomic_parquet(table: pa.Table, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, temporary, compression="zstd", version="2.6")
        if pq.ParquetFile(temporary).metadata.num_rows != table.num_rows:
            raise ValueError(f"Parquet validation failed for {destination.name}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_weights(
    tables: Iterable[pa.Table],
    destination: Path,
    *,
    metadata: Mapping[bytes, bytes],
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    writer = None
    count = 0
    try:
        for table in tables:
            table = table.replace_schema_metadata(metadata)
            writer = writer or pq.ParquetWriter(
                temporary, table.schema, compression="zstd", version="2.6"
            )
            writer.write_table(table)
            count += table.num_rows
        if writer is None:
            raise ValueError("Production geography produced no grid weights")
        writer.close()
        writer = None
        parquet = pq.ParquetFile(temporary)
        if parquet.metadata.num_rows != count:
            raise ValueError("Grid-weight Parquet row-count validation failed")
        os.replace(temporary, destination)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
    return count


def _atomic_json(payload: Mapping[str, Any], destination: Path) -> None:
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
    _atomic_json(manifest, manifest_path)
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


def build_production_artifacts(
    *,
    output_dir: Path,
    source_cache: Path,
    manifest_path: Path,
    offline: bool,
) -> dict[str, Any]:
    manifest = load_source_manifest(manifest_path)
    sources = acquire_all_sources(manifest, source_cache=source_cache, offline=offline)
    regions, timezones = _provider_regions(sources, source_cache)
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
    _atomic_parquet(registry, registry_path)
    weight_count = _atomic_weights(
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
