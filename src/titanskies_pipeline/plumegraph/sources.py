"""PlumeGraph cohort, request planning, and source-normalization primitives."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from titanskies_pipeline.config.settings_plumegraph import (
    PLUMEGRAPH_BENCHMARK_END,
    PLUMEGRAPH_BENCHMARK_START,
    PLUMEGRAPH_CMR_CONCEPT_ID,
    PLUMEGRAPH_SOURCE_MANIFEST_PATH,
    get_plumegraph_settings,
)
from titanskies_pipeline.geography.acquire import sha256_file
from titanskies_pipeline.plumegraph.identity import (
    camd_local_standard_hour_to_utc,
    canonical_json,
    emission_identity,
    gps_seconds_to_utc,
    pixel_identity,
    pixel_revision_identity,
    sha256_identity,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    plumegraph_ops_tbl,
    plumegraph_raw_tbl,
)


@dataclass(frozen=True)
class Facility:
    facility_id: str
    facility_name: str
    latitude: float
    longitude: float
    timezone: str
    utc_standard_offset_minutes: int
    annual_nox_tons: float | None
    is_cohort: bool
    review_status: str
    inclusion_reason: str


@dataclass(frozen=True)
class SourceRequest:
    request_id: str
    connector: str
    source_version: str
    analysis_region_id: str | None
    window_start: datetime
    window_end: datetime
    request_contract_version: str
    request: dict[str, object]


@dataclass(frozen=True)
class SourceSnapshot:
    snapshot_id: str
    request_id: str
    connector: str
    source_identity: str
    source_revision_at: datetime | None
    artifact_uri: str
    content_sha256: str
    source_etag: str | None
    schema_fingerprint: str
    row_count: int
    collected_at: datetime
    source_lineage_json: str = "{}"


@dataclass(frozen=True)
class NormalizedArtifact:
    normalized_artifact_id: str
    source_snapshot_id: str
    connector: str
    analysis_region_id: str | None
    partition_date: date
    artifact_uri: str
    content_sha256: str
    schema_fingerprint: str
    row_count: int
    created_at: datetime


@dataclass(frozen=True)
class DiscoveryMetrics:
    regions: int
    requests_planned: int
    requests_existing: int


@dataclass(frozen=True)
class SourceIngestMetrics:
    snapshots: int
    pixels_inserted: int
    meteorology_rows_inserted: int
    emission_revisions_inserted: int


def _db_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _month_windows(
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime]]:
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("Monthly windows require an ordered timezone-aware range")
    cursor = start.astimezone(timezone.utc)
    final = end.astimezone(timezone.utc)
    windows: list[tuple[datetime, datetime]] = []
    while cursor < final:
        if cursor.month == 12:
            boundary = cursor.replace(
                year=cursor.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            boundary = cursor.replace(
                month=cursor.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        next_cursor = min(boundary, final)
        windows.append((cursor, next_cursor))
        cursor = next_cursor
    return windows


def load_cohort_manifest(
    path: Path,
    *,
    require_approved: bool = True,
    expected_cohort_count: int = 75,
) -> tuple[str, list[Facility], dict[str, object]]:
    document = json.loads(path.read_text())
    if not isinstance(document, dict):
        raise ValueError("PlumeGraph cohort manifest must be a JSON object")
    if document.get("schema_version") != "plumegraph-cohort-v1":
        raise ValueError("Unsupported PlumeGraph cohort schema")
    cohort_version = str(document.get("cohort_version") or "")
    facilities_raw = document.get("facilities")
    if not cohort_version or not isinstance(facilities_raw, list):
        raise ValueError("PlumeGraph cohort version and facilities are required")
    review_status = str(document.get("review_status") or "")
    if require_approved and (
        review_status != "approved" or not document.get("approved_by")
    ):
        raise ValueError(
            "Production PlumeGraph cohort must be explicitly approved by a "
            "scientific owner"
        )
    facilities: list[Facility] = []
    seen: set[str] = set()
    for item in facilities_raw:
        if not isinstance(item, dict):
            raise ValueError("PlumeGraph facility entries must be objects")
        facility = Facility(
            facility_id=str(item["facility_id"]),
            facility_name=str(item["facility_name"]),
            latitude=float(item["latitude"]),
            longitude=float(item["longitude"]),
            timezone=str(item["timezone"]),
            utc_standard_offset_minutes=int(item["utc_standard_offset_minutes"]),
            annual_nox_tons=(
                None
                if item.get("annual_nox_tons") in (None, "")
                else float(item["annual_nox_tons"])
            ),
            is_cohort=bool(item.get("is_cohort", True)),
            review_status=str(item.get("review_status") or review_status),
            inclusion_reason=str(item.get("inclusion_reason") or ""),
        )
        if (
            not facility.facility_id
            or facility.facility_id in seen
            or not facility.facility_name
            or not -90 <= facility.latitude <= 90
            or not -180 <= facility.longitude <= 180
            or not facility.inclusion_reason
        ):
            raise ValueError("PlumeGraph cohort contains an invalid facility")
        camd_local_standard_hour_to_utc(
            date(2024, 1, 1),
            0,
            timezone_name=facility.timezone,
            utc_standard_offset_minutes=facility.utc_standard_offset_minutes,
        )
        seen.add(facility.facility_id)
        facilities.append(facility)
    cohort = [facility for facility in facilities if facility.is_cohort]
    if expected_cohort_count < 1:
        raise ValueError("Expected PlumeGraph cohort count must be positive")
    if len(cohort) != expected_cohort_count:
        raise ValueError(
            f"PlumeGraph cohort must contain exactly {expected_cohort_count} plants"
        )
    return cohort_version, facilities, document


def _geometry_modules():
    from pyproj import Transformer
    from shapely.geometry import Point, mapping
    from shapely.ops import transform, unary_union
    from shapely.wkb import dumps

    return Transformer, Point, mapping, transform, unary_union, dumps


def build_analysis_regions(
    facilities: Sequence[Facility],
    *,
    cohort_version: str,
    aoi_radius_km: float,
) -> list[dict[str, object]]:
    if aoi_radius_km <= 0:
        raise ValueError("PlumeGraph AOI radius must be positive")
    cohort = sorted(
        (facility for facility in facilities if facility.is_cohort),
        key=lambda facility: facility.facility_id,
    )
    if not cohort:
        raise ValueError("PlumeGraph analysis regions require cohort facilities")
    Transformer, Point, mapping, transform, unary_union, dumps = _geometry_modules()
    to_equal_area = Transformer.from_crs(
        "EPSG:4326", "EPSG:5070", always_xy=True
    ).transform
    to_wgs84 = Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True).transform
    projected = [
        (
            facility,
            transform(
                to_equal_area,
                Point(facility.longitude, facility.latitude),
            ).buffer(aoi_radius_km * 1000),
        )
        for facility in cohort
    ]
    remaining = set(range(len(projected)))
    groups: list[list[int]] = []
    while remaining:
        seed = min(remaining)
        group = {seed}
        frontier = [seed]
        remaining.remove(seed)
        while frontier:
            current = frontier.pop()
            neighbors = [
                index
                for index in sorted(remaining)
                if projected[current][1].intersects(projected[index][1])
            ]
            for neighbor in neighbors:
                remaining.remove(neighbor)
                group.add(neighbor)
                frontier.append(neighbor)
        groups.append(sorted(group))
    regions: list[dict[str, object]] = []
    for group in groups:
        facility_ids = sorted(projected[index][0].facility_id for index in group)
        geometry = transform(
            to_wgs84,
            unary_union([projected[index][1] for index in group]),
        ).normalize()
        canonical_geometry = canonical_json(mapping(geometry))
        region_id = sha256_identity(
            canonical_json(facility_ids),
            canonical_geometry,
            cohort_version,
            f"{aoi_radius_km:.6f}",
        )
        regions.append(
            {
                "analysis_region_id": region_id,
                "facility_ids": facility_ids,
                "geometry_wkb": dumps(geometry, hex=False, big_endian=False),
                "geometry_geojson": json.loads(canonical_geometry),
            }
        )
    return sorted(regions, key=lambda item: tuple(item["facility_ids"]))


def persist_cohort(
    path: Path,
    *,
    require_approved: bool = True,
    expected_cohort_count: int = 75,
    conn=None,
) -> dict[str, int | str]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    cohort_version, facilities, document = load_cohort_manifest(
        path,
        require_approved=require_approved,
        expected_cohort_count=expected_cohort_count,
    )
    settings = get_plumegraph_settings()
    regions = build_analysis_regions(
        facilities,
        cohort_version=cohort_version,
        aoi_radius_km=float(settings.contract["aoi_radius_km"]),
    )
    from shapely.geometry import Point
    from shapely.wkb import loads

    for region in regions:
        geometry = loads(bytes(region["geometry_wkb"]))
        region["facility_ids"] = sorted(
            facility.facility_id
            for facility in facilities
            if facility.is_cohort
            and facility.facility_id in region["facility_ids"]
            or (
                not facility.is_cohort
                and geometry.covers(Point(facility.longitude, facility.latitude))
            )
        )
    body = path.read_bytes()
    manifest_sha = hashlib.sha256(body).hexdigest()
    source_manifest_sha = sha256_file(PLUMEGRAPH_SOURCE_MANIFEST_PATH)
    loaded_at = _db_time(_utc_now())
    source_snapshot_id = sha256_identity("cohort", cohort_version, manifest_sha)
    with _use_conn(conn) as connection:
        connection.begin()
        try:
            previous = connection.execute(
                f"""
                SELECT manifest_sha256
                FROM {plumegraph_ops_tbl("cohort_manifests")}
                WHERE cohort_version = ?
                """,
                [cohort_version],
            ).fetchone()
            if previous and str(previous[0]) != manifest_sha:
                raise ValueError("A frozen PlumeGraph cohort version cannot change")
            connection.execute(
                f"""
                INSERT OR IGNORE INTO {plumegraph_ops_tbl("cohort_manifests")}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    cohort_version,
                    manifest_sha,
                    source_manifest_sha,
                    path.name,
                    sum(facility.is_cohort for facility in facilities),
                    document["review_status"],
                    document.get("approved_by"),
                    loaded_at,
                ],
            )
            for facility in facilities:
                _, Point, _, _, _, dumps = _geometry_modules()
                connection.execute(
                    f"""
                    INSERT OR IGNORE INTO {plumegraph_raw_tbl("facilities")}
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        cohort_version,
                        facility.facility_id,
                        facility.facility_name,
                        facility.latitude,
                        facility.longitude,
                        dumps(
                            Point(facility.longitude, facility.latitude),
                            hex=False,
                            big_endian=False,
                        ),
                        facility.timezone,
                        facility.utc_standard_offset_minutes,
                        facility.annual_nox_tons,
                        facility.is_cohort,
                        facility.review_status,
                        facility.inclusion_reason,
                        source_snapshot_id,
                        loaded_at,
                    ],
                )
            for region in regions:
                connection.execute(
                    f"""
                    INSERT OR IGNORE INTO {plumegraph_raw_tbl("analysis_regions")}
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        cohort_version,
                        region["analysis_region_id"],
                        canonical_json(region["facility_ids"]),
                        region["geometry_wkb"],
                        float(settings.contract["aoi_radius_km"]),
                        loaded_at,
                    ],
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return {
        "cohort_version": cohort_version,
        "facilities": len(facilities),
        "cohort_facilities": sum(facility.is_cohort for facility in facilities),
        "analysis_regions": len(regions),
    }


def _request(
    connector: str,
    source_version: str,
    analysis_region_id: str | None,
    start: datetime,
    end: datetime,
    contract_version: str,
    payload: dict[str, object],
) -> SourceRequest:
    request_id = sha256_identity(
        connector,
        source_version,
        analysis_region_id or "global",
        start.isoformat(),
        end.isoformat(),
        contract_version,
        canonical_json(payload),
    )
    return SourceRequest(
        request_id,
        connector,
        source_version,
        analysis_region_id,
        start,
        end,
        contract_version,
        payload,
    )


def plan_source_requests(
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    backfill: bool = False,
    conn=None,
) -> DiscoveryMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    settings = get_plumegraph_settings()
    end = window_end or _utc_now()
    start = window_start or (
        PLUMEGRAPH_BENCHMARK_START
        if backfill
        else end - timedelta(days=settings.discovery_lookback_days)
    )
    if backfill and window_end is None:
        end = PLUMEGRAPH_BENCHMARK_END
    windows = _month_windows(start, end)
    now = _db_time(_utc_now())
    with _use_conn(conn) as connection:
        from shapely.wkb import loads

        regions = [
            (
                str(row[0]),
                json.loads(str(row[1])),
                tuple(float(value) for value in loads(bytes(row[2])).bounds),
            )
            for row in connection.execute(
                f"""
                SELECT analysis_region_id, facility_ids_json, geometry_wkb
                FROM {plumegraph_raw_tbl("analysis_regions")}
                ORDER BY analysis_region_id
                """
            ).fetchall()
        ]
        if not regions:
            raise RuntimeError(
                "PlumeGraph facility registry is empty; bootstrap an approved "
                "cohort before discovery"
            )
        requests: list[SourceRequest] = []
        for region_id, facility_ids, bounds in regions:
            for request_start, request_end in windows:
                requests.append(
                    _request(
                        "harmony",
                        f"TEMPO_NO2_L2:{settings.contract['collection_version']}",
                        region_id,
                        request_start,
                        request_end,
                        str(settings.contract["contract_version"]),
                        {
                            "concept_id": PLUMEGRAPH_CMR_CONCEPT_ID,
                            "region_id": region_id,
                            "bbox": bounds,
                            "variables": [
                                "main_data_quality_flag",
                                "vertical_column_troposphere",
                                "vertical_column_troposphere_uncertainty",
                                "time",
                                "latitude",
                                "longitude",
                                "latitude_bounds",
                                "longitude_bounds",
                                "effective_cloud_fraction",
                                "snow_ice_fraction",
                                "amf_diagnostic_flag",
                                "solar_zenith_angle",
                                "viewing_zenith_angle",
                                "surface_pressure",
                            ],
                        },
                    )
                )
                requests.append(
                    _request(
                        "hrrr",
                        "hrrr-analysis-f00",
                        region_id,
                        request_start,
                        request_end,
                        str(settings.contract["contract_version"]),
                        {
                            "region_id": region_id,
                            "bbox": bounds,
                            "forecast_hour": 0,
                            "fields": [
                                "u10",
                                "v10",
                                "u80",
                                "v80",
                                "hpbl",
                                "sp",
                                "t2m",
                            ],
                        },
                    )
                )
            requests.append(
                _request(
                    "camd",
                    "camd-apportioned-hourly-2024",
                    region_id,
                    start,
                    end,
                    str(settings.contract["contract_version"]),
                    {"facility_ids": sorted(facility_ids), "year": 2024},
                )
            )
        inserted = 0
        for item in requests:
            before = connection.execute(
                f"SELECT 1 FROM {plumegraph_ops_tbl('source_requests')} "
                "WHERE request_id = ?",
                [item.request_id],
            ).fetchone()
            connection.execute(
                f"""
                INSERT OR IGNORE INTO {plumegraph_ops_tbl("source_requests")}
                (request_id, connector, source_version, analysis_region_id,
                 window_start, window_end, request_json,
                 request_contract_version, status, attempts, planned_at,
                 updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'planned', 0, ?, ?)
                """,
                [
                    item.request_id,
                    item.connector,
                    item.source_version,
                    item.analysis_region_id,
                    _db_time(item.window_start),
                    _db_time(item.window_end),
                    canonical_json(item.request),
                    item.request_contract_version,
                    now,
                    now,
                ],
            )
            inserted += int(before is None)
    return DiscoveryMetrics(
        regions=len(regions),
        requests_planned=inserted,
        requests_existing=len(requests) - inserted,
    )


def _snapshot_destination(
    raw_root: Path,
    connector: str,
    request_id: str,
    checksum: str,
    extension: str,
) -> Path:
    safe_extension = extension.lstrip(".")
    if not safe_extension.isalnum():
        raise ValueError("Snapshot extension must be alphanumeric")
    destination = (
        raw_root / connector / request_id / f"{checksum}.{safe_extension}"
    ).resolve()
    if not destination.is_relative_to(raw_root.resolve()):
        raise ValueError("PlumeGraph snapshot path escapes its raw-data root")
    return destination


def write_source_snapshot(
    body: bytes,
    *,
    request: SourceRequest,
    source_identity: str,
    extension: str,
    schema_fields: Sequence[str],
    row_count: int,
    source_revision_at: datetime | None = None,
    source_etag: str | None = None,
    source_lineage: Mapping[str, object] | None = None,
    collected_at: datetime | None = None,
    raw_data_dir: Path | None = None,
    register: bool = True,
    conn=None,
) -> SourceSnapshot:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    root = (raw_data_dir or get_plumegraph_settings().raw_data_dir).resolve()
    checksum = hashlib.sha256(body).hexdigest()
    destination = _snapshot_destination(
        root,
        request.connector,
        request.request_id,
        checksum,
        extension,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(destination) != checksum:
            raise ValueError("Existing PlumeGraph snapshot checksum mismatch")
    else:
        handle, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
        )
        os.close(handle)
        temporary = Path(temporary_name)
        try:
            temporary.write_bytes(body)
            if sha256_file(temporary) != checksum:
                raise ValueError("PlumeGraph snapshot changed while writing")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    schema_fingerprint = hashlib.sha256(
        canonical_json(sorted(schema_fields)).encode()
    ).hexdigest()
    collected = collected_at or _utc_now()
    source_lineage_json = canonical_json(source_lineage or {})
    if sanitize_source_error(source_lineage_json) != source_lineage_json:
        raise ValueError(
            "PlumeGraph source lineage must not contain credentials or signed URLs"
        )
    snapshot = SourceSnapshot(
        snapshot_id=sha256_identity(
            request.request_id,
            source_identity,
            checksum,
            source_etag or "",
        ),
        request_id=request.request_id,
        connector=request.connector,
        source_identity=source_identity,
        source_revision_at=source_revision_at,
        artifact_uri=destination.relative_to(root).as_posix(),
        content_sha256=checksum,
        source_etag=source_etag,
        schema_fingerprint=schema_fingerprint,
        row_count=row_count,
        collected_at=collected,
        source_lineage_json=source_lineage_json,
    )
    if register:
        with _use_conn(conn) as connection:
            _insert_source_snapshot(connection, snapshot)
    return snapshot


def _insert_source_snapshot(connection, snapshot: SourceSnapshot) -> int:
    before = connection.execute(
        f"""
        SELECT 1
        FROM {plumegraph_ops_tbl("source_snapshots")}
        WHERE snapshot_id = ?
        """,
        [snapshot.snapshot_id],
    ).fetchone()
    connection.execute(
        f"""
        INSERT OR IGNORE INTO {plumegraph_ops_tbl("source_snapshots")}
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            snapshot.snapshot_id,
            snapshot.request_id,
            snapshot.connector,
            snapshot.source_identity,
            (
                _db_time(snapshot.source_revision_at)
                if snapshot.source_revision_at
                else None
            ),
            snapshot.artifact_uri,
            snapshot.content_sha256,
            snapshot.source_etag,
            snapshot.schema_fingerprint,
            snapshot.row_count,
            _db_time(snapshot.collected_at),
            snapshot.source_lineage_json,
        ],
    )
    return int(before is None)


def write_normalized_artifact(
    records: Sequence[Mapping[str, object]],
    *,
    snapshot: SourceSnapshot,
    analysis_region_id: str | None,
    partition_date: date,
    raw_data_dir: Path | None = None,
) -> NormalizedArtifact:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = (raw_data_dir or get_plumegraph_settings().raw_data_dir).resolve()
    region_component = analysis_region_id or "global"
    if (
        not snapshot.connector.replace("-", "").replace("_", "").isalnum()
        or not region_component.replace("-", "").replace("_", "").isalnum()
    ):
        raise ValueError("Normalized artifact partition contains an unsafe component")
    destination = (
        root
        / "normalized"
        / snapshot.connector
        / f"analysis_region_id={region_component}"
        / f"date={partition_date.isoformat()}"
        / f"{snapshot.snapshot_id}.parquet"
    ).resolve()
    if not destination.is_relative_to(root):
        raise ValueError("Normalized artifact path escapes its raw-data root")
    table = pa.Table.from_pylist([dict(record) for record in records])
    schema_fingerprint = hashlib.sha256(str(table.schema).encode()).hexdigest()
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        pq.write_table(table, temporary, compression="zstd")
        checksum = sha256_file(temporary)
        if destination.exists():
            if sha256_file(destination) != checksum:
                raise ValueError("Existing normalized artifact checksum mismatch")
        else:
            os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    created_at = snapshot.collected_at
    return NormalizedArtifact(
        normalized_artifact_id=sha256_identity(
            snapshot.snapshot_id,
            partition_date,
            checksum,
        ),
        source_snapshot_id=snapshot.snapshot_id,
        connector=snapshot.connector,
        analysis_region_id=analysis_region_id,
        partition_date=partition_date,
        artifact_uri=destination.relative_to(root).as_posix(),
        content_sha256=checksum,
        schema_fingerprint=schema_fingerprint,
        row_count=table.num_rows,
        created_at=created_at,
    )


def _insert_normalized_artifact(
    connection,
    artifact: NormalizedArtifact,
) -> None:
    connection.execute(
        f"""
        INSERT OR IGNORE INTO {plumegraph_ops_tbl("normalized_artifacts")}
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            artifact.normalized_artifact_id,
            artifact.source_snapshot_id,
            artifact.connector,
            artifact.analysis_region_id,
            artifact.partition_date,
            artifact.artifact_uri,
            artifact.content_sha256,
            artifact.schema_fingerprint,
            artifact.row_count,
            _db_time(artifact.created_at),
        ],
    )


def normalize_tempo_pixel(
    record: Mapping[str, object],
    *,
    analysis_region_id: str,
    granule_id: str,
    upstream_revision: str,
    snapshot: SourceSnapshot,
) -> dict[str, object]:
    original_time = float(record["time_gps_seconds"])
    observation_time = gps_seconds_to_utc(original_time)
    pixel_id = pixel_identity(
        PLUMEGRAPH_CMR_CONCEPT_ID,
        granule_id,
        int(record["mirror_step"]),
        int(record["xtrack"]),
        observation_time,
    )
    canonical = canonical_json(record)
    return {
        "pixel_revision_id": pixel_revision_identity(
            pixel_id,
            upstream_revision,
            snapshot.content_sha256,
            canonical,
        ),
        "pixel_id": pixel_id,
        "analysis_region_id": analysis_region_id,
        "granule_id": granule_id,
        "mirror_step": int(record["mirror_step"]),
        "xtrack": int(record["xtrack"]),
        "observation_time": observation_time,
        "original_time": original_time,
        "time_standard": "GPS",
        "latitude": _nullable_float(record.get("latitude")),
        "longitude": _nullable_float(record.get("longitude")),
        "geometry_wkb": record.get("geometry_wkb"),
        "pixel_area_km2": _nullable_float(record.get("pixel_area_km2")),
        "no2_vertical_column": _nullable_float(record.get("no2_vertical_column")),
        "no2_uncertainty": _nullable_float(record.get("no2_uncertainty")),
        "no2_unit": str(record.get("no2_unit") or "molecules/cm2"),
        "quality_flag": _nullable_int(record.get("quality_flag")),
        "cloud_fraction": _nullable_float(record.get("cloud_fraction")),
        "snow_ice_fraction": _nullable_float(record.get("snow_ice_fraction")),
        "amf_diagnostic_flag": _nullable_int(record.get("amf_diagnostic_flag")),
        "solar_zenith_angle": _nullable_float(record.get("solar_zenith_angle")),
        "viewing_zenith_angle": _nullable_float(record.get("viewing_zenith_angle")),
        "surface_pressure_hpa": _nullable_float(record.get("surface_pressure_hpa")),
        "collection_name": "TEMPO_NO2_L2",
        "collection_version": str(record.get("collection_version") or "V04"),
        "source_revision_at": snapshot.source_revision_at,
        "source_snapshot_id": snapshot.snapshot_id,
        "canonical_record_json": canonical,
        "collected_at": snapshot.collected_at,
    }


def normalize_camd_hour(
    record: Mapping[str, object],
    *,
    timezone_name: str,
    utc_standard_offset_minutes: int,
    snapshot: SourceSnapshot,
) -> dict[str, object]:
    operating_date = date.fromisoformat(str(record["operating_date"]))
    operating_hour = int(record["operating_hour"])
    facility_id = str(record["facility_id"])
    unit_id = str(record["unit_id"])
    stable_id = emission_identity(
        facility_id,
        unit_id,
        operating_date,
        operating_hour,
    )
    canonical = canonical_json(record)
    return {
        "emission_revision_id": sha256_identity(
            stable_id,
            snapshot.source_revision_at or "",
            snapshot.content_sha256,
            canonical,
        ),
        "emission_id": stable_id,
        "facility_id": facility_id,
        "unit_id": unit_id,
        "operating_date": operating_date,
        "operating_hour": operating_hour,
        "observation_start_utc": camd_local_standard_hour_to_utc(
            operating_date,
            operating_hour,
            timezone_name=timezone_name,
            utc_standard_offset_minutes=utc_standard_offset_minutes,
        ),
        "nox_mass_lbs": _nullable_float(record.get("nox_mass_lbs")),
        "operating_time_hours": _nullable_float(record.get("operating_time_hours")),
        "heat_input_mmbtu": _nullable_float(record.get("heat_input_mmbtu")),
        "gross_load_mw": _nullable_float(record.get("gross_load_mw")),
        "source_quality": (
            None
            if record.get("source_quality") in (None, "")
            else str(record["source_quality"])
        ),
        "source_revision_at": snapshot.source_revision_at,
        "source_snapshot_id": snapshot.snapshot_id,
        "canonical_record_json": canonical,
        "collected_at": snapshot.collected_at,
    }


def _nullable_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _nullable_int(value: object) -> int | None:
    return None if value in (None, "") else int(value)


def _insert_dicts(
    connection,
    table: str,
    rows: Iterable[Mapping[str, object]],
) -> int:
    inserted = 0
    for row in rows:
        columns = tuple(row)
        before = connection.execute(
            f"SELECT 1 FROM {table} WHERE {columns[0]} = ?",
            [row[columns[0]]],
        ).fetchone()
        connection.execute(
            f"""
            INSERT OR IGNORE INTO {table}
            ({", ".join(columns)})
            VALUES ({", ".join("?" for _ in columns)})
            """,
            [
                _db_time(value) if isinstance(value, datetime) else value
                for value in row.values()
            ],
        )
        inserted += int(before is None)
    return inserted


def persist_normalized_records(
    *,
    pixels: Sequence[Mapping[str, object]] = (),
    meteorology: Sequence[Mapping[str, object]] = (),
    emissions: Sequence[Mapping[str, object]] = (),
    snapshot: SourceSnapshot | None = None,
    successful_request: SourceRequest | None = None,
    raw_data_dir: Path | None = None,
    conn=None,
) -> SourceIngestMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    normalized_groups = [
        records for records in (pixels, meteorology, emissions) if records
    ]
    if snapshot is not None and len(normalized_groups) > 1:
        raise ValueError("A source snapshot must normalize to exactly one record type")
    artifacts: list[NormalizedArtifact] = []
    if snapshot is not None:
        partitioned: dict[date, list[Mapping[str, object]]] = {}
        for record in normalized_groups[0] if normalized_groups else ():
            partition_value = next(
                (
                    record.get(field)
                    for field in (
                        "observation_time",
                        "valid_time",
                        "observation_start_utc",
                    )
                    if record.get(field) is not None
                ),
                None,
            )
            if not isinstance(partition_value, datetime):
                raise ValueError("Normalized source record has no partition timestamp")
            partitioned.setdefault(partition_value.date(), []).append(record)
        if not partitioned:
            fallback_date = (
                successful_request.window_start.date()
                if successful_request is not None
                else snapshot.collected_at.date()
            )
            partitioned[fallback_date] = []
        artifacts = [
            write_normalized_artifact(
                rows,
                snapshot=snapshot,
                analysis_region_id=(
                    successful_request.analysis_region_id
                    if successful_request is not None
                    else None
                ),
                partition_date=partition_date,
                raw_data_dir=raw_data_dir,
            )
            for partition_date, rows in sorted(partitioned.items())
        ]
    with _use_conn(conn) as connection:
        connection.begin()
        try:
            snapshot_count = (
                _insert_source_snapshot(connection, snapshot)
                if snapshot is not None
                else 0
            )
            for artifact in artifacts:
                _insert_normalized_artifact(connection, artifact)
            pixel_count = _insert_dicts(
                connection,
                plumegraph_raw_tbl("retrieval_pixel_revisions"),
                pixels,
            )
            meteorology_count = _insert_dicts(
                connection,
                plumegraph_raw_tbl("meteorology_observations"),
                meteorology,
            )
            emission_count = _insert_dicts(
                connection,
                plumegraph_raw_tbl("hourly_emission_revisions"),
                emissions,
            )
            if successful_request is not None:
                now = _db_time(_utc_now())
                connection.execute(
                    f"""
                    UPDATE {plumegraph_ops_tbl("source_requests")}
                    SET status = 'success', attempts = greatest(attempts, 1),
                        started_at = coalesce(started_at, ?), finished_at = ?,
                        updated_at = ?, error_message = NULL
                    WHERE request_id = ?
                    """,
                    [now, now, now, successful_request.request_id],
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return SourceIngestMetrics(
        snapshots=snapshot_count,
        pixels_inserted=pixel_count,
        meteorology_rows_inserted=meteorology_count,
        emission_revisions_inserted=emission_count,
    )


def source_request_from_row(row: Sequence[object]) -> SourceRequest:
    return SourceRequest(
        request_id=str(row[0]),
        connector=str(row[1]),
        source_version=str(row[2]),
        analysis_region_id=None if row[3] is None else str(row[3]),
        window_start=row[4].replace(tzinfo=timezone.utc),
        window_end=row[5].replace(tzinfo=timezone.utc),
        request_contract_version=str(row[6]),
        request=json.loads(str(row[7])),
    )


def pending_source_requests(*, connector: str | None = None, conn=None):
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    connector_filter = "" if connector is None else " AND connector = ?"
    params: list[object] = [] if connector is None else [connector]
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"""
            SELECT request_id, connector, source_version, analysis_region_id,
                   window_start, window_end, request_contract_version,
                   request_json
            FROM {plumegraph_ops_tbl("source_requests")}
            WHERE (
                status IN ('planned', 'failed')
                OR (
                    status = 'running'
                    AND started_at < current_timestamp - INTERVAL '1 hour'
                )
            )
            {connector_filter}
            ORDER BY window_start, connector, analysis_region_id, request_id
            """,
            params,
        ).fetchall()
    return [source_request_from_row(row) for row in rows]


def sanitize_source_error(message: str, *, secrets: Sequence[str] = ()) -> str:
    sanitized = message
    for secret in secrets:
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    for marker in ("token=", "api_key=", "apikey=", "signature="):
        position = sanitized.lower().find(marker)
        if position >= 0:
            end = sanitized.find("&", position)
            sanitized = (
                sanitized[: position + len(marker)]
                + "[REDACTED]"
                + (sanitized[end:] if end >= 0 else "")
            )
    return sanitized[:2000]


__all__ = [
    "DiscoveryMetrics",
    "Facility",
    "NormalizedArtifact",
    "SourceIngestMetrics",
    "SourceRequest",
    "SourceSnapshot",
    "build_analysis_regions",
    "load_cohort_manifest",
    "normalize_camd_hour",
    "normalize_tempo_pixel",
    "pending_source_requests",
    "persist_cohort",
    "persist_normalized_records",
    "plan_source_requests",
    "sanitize_source_error",
    "source_request_from_row",
    "write_source_snapshot",
    "write_normalized_artifact",
]
