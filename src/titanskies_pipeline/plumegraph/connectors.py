"""Live Harmony, HRRR Zarr, and EPA CAMD connector execution."""

from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

from titanskies_pipeline.config.settings_plumegraph import get_plumegraph_settings
from titanskies_pipeline.plumegraph.identity import canonical_json, sha256_identity
from titanskies_pipeline.plumegraph.sources import (
    SourceRequest,
    normalize_camd_hour,
    normalize_tempo_pixel,
    pending_source_requests,
    persist_normalized_records,
    sanitize_source_error,
    write_source_snapshot,
)
from titanskies_pipeline.plumegraph.tempo_l2 import read_tempo_l2_netcdf
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    plumegraph_ops_tbl,
    plumegraph_raw_tbl,
)


@dataclass(frozen=True)
class ConnectorMetrics:
    connector: str
    requests_succeeded: int
    requests_failed: int
    snapshots_written: int
    rows_inserted: int


class PlumeGraphConnectorError(RuntimeError):
    def __init__(self, metrics: ConnectorMetrics, request_ids: Sequence[str]):
        super().__init__(
            f"{len(request_ids)} {metrics.connector} request(s) failed after "
            "successful siblings were committed"
        )
        self.metrics = metrics
        self.request_ids = tuple(request_ids)


def _db_time(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _mark_running(connection, request: SourceRequest) -> None:
    now = _db_time(datetime.now(timezone.utc))
    connection.execute(
        f"""
        UPDATE {plumegraph_ops_tbl("source_requests")}
        SET status = 'running', attempts = attempts + 1, started_at = ?,
            updated_at = ?, error_message = NULL
        WHERE request_id = ?
        """,
        [now, now, request.request_id],
    )


def _mark_finished(
    connection,
    request: SourceRequest,
    *,
    status: str,
    error_message: str | None = None,
) -> None:
    now = _db_time(datetime.now(timezone.utc))
    connection.execute(
        f"""
        UPDATE {plumegraph_ops_tbl("source_requests")}
        SET status = ?, finished_at = ?, updated_at = ?, error_message = ?
        WHERE request_id = ?
        """,
        [status, now, now, error_message, request.request_id],
    )


def _http_json(
    url: str,
    *,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    timeout: float = 120,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[object, Mapping[str, str]]:
    import requests

    delays = (1.0, 2.0, 4.0)
    for attempt in range(4):
        try:
            response = requests.get(
                url,
                params=params,
                headers=dict(headers),
                timeout=timeout,
            )
        except requests.Timeout:
            if attempt == 3:
                raise
            sleep(delays[attempt])
            continue
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == 3:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            sleep(float(retry_after) if retry_after else delays[attempt])
            continue
        response.raise_for_status()
        return response.json(), response.headers
    raise AssertionError("HTTP retry loop exhausted unexpectedly")  # pragma: no cover


def _tempo_source_metadata(path: Path) -> tuple[str, datetime]:
    import xarray as xr

    dataset = xr.open_dataset(path, decode_times=False, mask_and_scale=False)
    try:
        attributes = dataset.attrs
        source_identity = next(
            (
                str(attributes[name])
                for name in ("granule_id", "GranuleID", "product_name")
                if attributes.get(name)
            ),
            path.name,
        )
        revision_value = next(
            (
                attributes[name]
                for name in (
                    "date_created",
                    "production_date_time",
                    "production_datetime",
                )
                if attributes.get(name)
            ),
            None,
        )
    finally:
        dataset.close()
    if revision_value is None:
        raise ValueError(
            "Harmony TEMPO subset is missing an authoritative production timestamp"
        )
    revision = datetime.fromisoformat(
        str(revision_value).strip().replace("Z", "+00:00")
    )
    if revision.tzinfo is None:
        raise ValueError("Harmony TEMPO production timestamp must include a timezone")
    return source_identity, revision.astimezone(timezone.utc)


def _fetch_harmony(
    request: SourceRequest,
    *,
    raw_data_dir: Path,
    conn,
) -> tuple[int, int]:
    try:
        from harmony import BBox, Client, Collection, Request
    except ImportError as exc:
        raise RuntimeError(
            "Harmony ingestion requires `uv sync --extra plumegraph`"
        ) from exc
    payload = request.request
    bounds = [float(value) for value in payload["bbox"]]
    client = Client()
    harmony_request = Request(
        collection=Collection(id=str(payload["concept_id"])),
        spatial=BBox(*bounds),
        temporal={
            "start": request.window_start,
            "stop": request.window_end,
        },
        variables=list(payload["variables"]),
        format="application/x-netcdf4",
    )
    if not harmony_request.is_valid():
        raise ValueError("Harmony rejected the pinned PlumeGraph request contract")
    job_id = client.submit(harmony_request)
    paths: list[Path] = []
    with tempfile.TemporaryDirectory(prefix="plumegraph-harmony-") as directory:
        downloads = client.download_all(
            job_id,
            directory=directory,
            overwrite=True,
        )
        for download in downloads:
            result = download.result()
            paths.append(Path(result))
        snapshots = rows_inserted = 0
        for path in paths:
            body = path.read_bytes()
            records = read_tempo_l2_netcdf(path)
            source_identity, source_revision = _tempo_source_metadata(path)
            snapshot = write_source_snapshot(
                body,
                request=request,
                source_identity=source_identity,
                extension="nc",
                schema_fields=sorted(records[0]) if records else (),
                row_count=len(records),
                source_revision_at=source_revision,
                source_lineage={
                    "harmony_job_id": str(job_id),
                    "result_name": path.name,
                    "request_id": request.request_id,
                },
                collected_at=datetime.now(timezone.utc),
                raw_data_dir=raw_data_dir,
                register=False,
                conn=conn,
            )
            normalized = [
                normalize_tempo_pixel(
                    record,
                    analysis_region_id=str(request.analysis_region_id),
                    granule_id=source_identity,
                    upstream_revision=source_revision.isoformat(),
                    snapshot=snapshot,
                )
                for record in records
            ]
            metrics = persist_normalized_records(
                pixels=normalized,
                snapshot=snapshot,
                successful_request=request,
                raw_data_dir=raw_data_dir,
                conn=conn,
            )
            snapshots += 1
            rows_inserted += metrics.pixels_inserted
    return snapshots, rows_inserted


def _hrrr_value(
    base_url: str,
    *,
    level: str,
    variable: str,
    y_index: int,
    x_index: int,
) -> float:
    import xarray as xr

    path = f"{base_url}/{level}/{variable}/{level}"
    dataset = xr.open_zarr(
        path,
        storage_options={"anon": True},
        consolidated=False,
    )
    try:
        data = dataset[variable]
        y_name = next(name for name in data.dims if "y" in name.lower())
        x_name = next(name for name in data.dims if "x" in name.lower())
        return float(data.isel({y_name: y_index, x_name: x_index}).values)
    finally:
        dataset.close()


def _hrrr_grid_index(
    latitude: float,
    longitude: float,
    *,
    store_url: str = "s3://hrrrzarr",
) -> tuple[int, int]:
    import numpy as np
    import xarray as xr

    dataset = xr.open_zarr(
        f"{store_url.rstrip('/')}/grid/HRRR_chunk_index.zarr",
        storage_options={"anon": True},
        consolidated=False,
    )
    try:
        distance = (dataset["latitude"] - latitude) ** 2 + (
            (dataset["longitude"] - longitude + 180) % 360 - 180
        ) ** 2
        flat_index = int(np.nanargmin(distance.values))
        return tuple(
            int(value) for value in np.unravel_index(flat_index, distance.shape)
        )
    finally:
        dataset.close()


def _fetch_hrrr(
    request: SourceRequest,
    *,
    raw_data_dir: Path,
    conn,
) -> tuple[int, int]:
    settings = get_plumegraph_settings()
    bounds = [float(value) for value in request.request["bbox"]]
    longitude = (bounds[0] + bounds[2]) / 2
    latitude = (bounds[1] + bounds[3]) / 2
    y_index, x_index = _hrrr_grid_index(
        latitude,
        longitude,
        store_url=settings.hrrr_store_url,
    )
    rows: list[dict[str, object]] = []
    cursor = request.window_start.replace(minute=0, second=0, microsecond=0)
    while cursor < request.window_end:
        base = (
            f"{settings.hrrr_store_url.rstrip('/')}/sfc/"
            f"{cursor:%Y%m%d}/{cursor:%Y%m%d_%H}z_anl.zarr"
        )
        rows.append(
            {
                "valid_time": cursor,
                "wind_u_10m": _hrrr_value(
                    base,
                    level="10m_above_ground",
                    variable="UGRD",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "wind_v_10m": _hrrr_value(
                    base,
                    level="10m_above_ground",
                    variable="VGRD",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "wind_u_80m": _hrrr_value(
                    base,
                    level="80m_above_ground",
                    variable="UGRD",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "wind_v_80m": _hrrr_value(
                    base,
                    level="80m_above_ground",
                    variable="VGRD",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "pbl_height_m": _hrrr_value(
                    base,
                    level="surface",
                    variable="HPBL",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "surface_pressure_hpa": _hrrr_value(
                    base,
                    level="surface",
                    variable="PRES",
                    y_index=y_index,
                    x_index=x_index,
                )
                / 100,
                "temperature_2m_k": _hrrr_value(
                    base,
                    level="2m_above_ground",
                    variable="TMP",
                    y_index=y_index,
                    x_index=x_index,
                ),
                "source_path": base,
            }
        )
        cursor += timedelta(hours=1)
    body = (json.dumps(rows, default=str, sort_keys=True) + "\n").encode()
    snapshot = write_source_snapshot(
        body,
        request=request,
        source_identity=(
            f"{settings.hrrr_store_url.rstrip('/')}/{request.window_start:%Y%m%d}"
        ),
        extension="json",
        schema_fields=sorted(rows[0]) if rows else (),
        row_count=len(rows),
        source_revision_at=request.window_end,
        source_etag=sha256_identity(
            canonical_json([row["source_path"] for row in rows])
        ),
        source_lineage={
            "etag_semantics": "source-path-manifest-sha256",
            "source_paths": [row["source_path"] for row in rows],
            "grid_index": {"y": y_index, "x": x_index},
        },
        collected_at=datetime.now(timezone.utc),
        raw_data_dir=raw_data_dir,
        register=False,
        conn=conn,
    )
    normalized = [
        {
            "meteorology_revision_id": sha256_identity(
                request.analysis_region_id,
                row["valid_time"],
                snapshot.content_sha256,
                canonical_json(row),
            ),
            "analysis_region_id": request.analysis_region_id,
            "valid_time": row["valid_time"],
            "latitude": latitude,
            "longitude": longitude,
            "wind_u_10m": row["wind_u_10m"],
            "wind_v_10m": row["wind_v_10m"],
            "wind_u_80m": row["wind_u_80m"],
            "wind_v_80m": row["wind_v_80m"],
            "pbl_height_m": row["pbl_height_m"],
            "surface_pressure_hpa": row["surface_pressure_hpa"],
            "temperature_2m_k": row["temperature_2m_k"],
            "source_etag": snapshot.source_etag,
            "source_snapshot_id": snapshot.snapshot_id,
            "collected_at": snapshot.collected_at,
        }
        for row in rows
    ]
    metrics = persist_normalized_records(
        meteorology=normalized,
        snapshot=snapshot,
        successful_request=request,
        raw_data_dir=raw_data_dir,
        conn=conn,
    )
    return 1, metrics.meteorology_rows_inserted


def _fetch_camd(
    request: SourceRequest,
    *,
    raw_data_dir: Path,
    conn,
) -> tuple[int, int]:
    settings = get_plumegraph_settings()
    if not settings.epa_api_key:
        raise RuntimeError("CAMD ingestion requires PLUMEGRAPH_EPA_API_KEY")
    facility_ids = [str(value) for value in request.request["facility_ids"]]
    all_items: list[dict[str, object]] = []
    page = 1
    headers = {"x-api-key": settings.epa_api_key}
    while True:
        payload, _ = _http_json(
            "https://api.epa.gov/easey/emissions-mgmt/emissions/apportioned/hourly",
            params={
                "facilityId": "|".join(facility_ids),
                "beginDate": request.window_start.date().isoformat(),
                "endDate": (request.window_end - timedelta(microseconds=1))
                .date()
                .isoformat(),
                "page": page,
                "perPage": 500,
            },
            headers=headers,
        )
        if not isinstance(payload, dict) or not isinstance(
            payload.get("items"),
            list,
        ):
            raise ValueError("CAMD returned an unexpected response schema")
        items = [item for item in payload["items"] if isinstance(item, dict)]
        all_items.extend(items)
        pagination = payload.get("pagination") or {}
        total_pages = int(pagination.get("totalPages") or page)
        if page >= total_pages or not items:
            break
        page += 1
    body = (json.dumps(all_items, sort_keys=True) + "\n").encode()
    snapshot = write_source_snapshot(
        body,
        request=request,
        source_identity=(
            "https://api.epa.gov/easey/emissions-mgmt/emissions/apportioned/hourly"
        ),
        extension="json",
        schema_fields=sorted(all_items[0]) if all_items else (),
        row_count=len(all_items),
        source_revision_at=datetime.now(timezone.utc),
        source_lineage={
            "api_version": "EASEY emissions-mgmt",
            "endpoint": "/emissions/apportioned/hourly",
            "pages": page,
            "request_id": request.request_id,
        },
        collected_at=datetime.now(timezone.utc),
        raw_data_dir=raw_data_dir,
        register=False,
        conn=conn,
    )
    facilities = {
        str(row[0]): (str(row[1]), int(row[2]))
        for row in conn.execute(
            f"""
            SELECT facility_id, timezone, utc_standard_offset_minutes
            FROM {plumegraph_raw_tbl("facilities")}
            """
        ).fetchall()
    }
    normalized = []
    for item in all_items:
        facility_id = str(item.get("facilityId") or "")
        timezone_config = facilities.get(facility_id)
        if not timezone_config:
            continue
        normalized.append(
            normalize_camd_hour(
                {
                    "facility_id": facility_id,
                    "unit_id": item.get("unitId"),
                    "operating_date": item.get("date"),
                    "operating_hour": item.get("hour"),
                    "nox_mass_lbs": item.get("noxMass"),
                    "operating_time_hours": item.get("opTime"),
                    "heat_input_mmbtu": item.get("heatInput"),
                    "gross_load_mw": item.get("grossLoad"),
                    "source_quality": item.get("noxMassMeasureFlg"),
                },
                timezone_name=timezone_config[0],
                utc_standard_offset_minutes=timezone_config[1],
                snapshot=snapshot,
            )
        )
    metrics = persist_normalized_records(
        emissions=normalized,
        snapshot=snapshot,
        successful_request=request,
        raw_data_dir=raw_data_dir,
        conn=conn,
    )
    return 1, metrics.emission_revisions_inserted


_FETCHERS = {
    "harmony": _fetch_harmony,
    "hrrr": _fetch_hrrr,
    "camd": _fetch_camd,
}


def sync_source_connector(
    connector: str,
    *,
    max_requests: int | None = None,
    raw_data_dir: Path | None = None,
    conn=None,
) -> ConnectorMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    if connector not in _FETCHERS:
        raise ValueError(f"Unsupported PlumeGraph connector {connector!r}")
    root = (raw_data_dir or get_plumegraph_settings().raw_data_dir).resolve()
    succeeded = failed = snapshots = rows = 0
    failed_ids: list[str] = []
    with _use_conn(conn) as connection:
        requests = pending_source_requests(connector=connector, conn=connection)
        if max_requests is not None:
            requests = requests[:max_requests]
        for request in requests:
            _mark_running(connection, request)
            try:
                snapshot_count, row_count = _FETCHERS[connector](
                    request,
                    raw_data_dir=root,
                    conn=connection,
                )
            except Exception as exc:
                failed += 1
                failed_ids.append(request.request_id)
                _mark_finished(
                    connection,
                    request,
                    status="failed",
                    error_message=sanitize_source_error(
                        str(exc),
                        secrets=(os.environ.get("PLUMEGRAPH_EPA_API_KEY", ""),),
                    ),
                )
                continue
            _mark_finished(connection, request, status="success")
            succeeded += 1
            snapshots += snapshot_count
            rows += row_count
    metrics = ConnectorMetrics(
        connector=connector,
        requests_succeeded=succeeded,
        requests_failed=failed,
        snapshots_written=snapshots,
        rows_inserted=rows,
    )
    if failed_ids:
        raise PlumeGraphConnectorError(metrics, failed_ids)
    return metrics


__all__ = [
    "ConnectorMetrics",
    "PlumeGraphConnectorError",
    "sync_source_connector",
]
