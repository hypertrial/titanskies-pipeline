"""Durable RiverPulse request planning and revision-safe snapshot ingestion."""

from __future__ import annotations

import hashlib
import os
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from titanskies_pipeline.config.settings_riverpulse import (
    RIVERPULSE_BACKFILL_START,
    get_riverpulse_settings,
)
from titanskies_pipeline.geography.acquire import sha256_file
from titanskies_pipeline.riverpulse.hydrocron import (
    FetchResult,
    HydrocronFetchError,
    HydrocronRequest,
    ParsedObservation,
    calendar_year_windows,
    fetch_hydrocron,
    parse_csv_response,
    sanitize_error,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    riverpulse_ops_tbl,
    riverpulse_raw_tbl,
)

OBSERVATION_COLUMNS = (
    "observation_revision_id",
    "observation_id",
    "reach_id",
    "observation_time",
    "cycle_id",
    "pass_id",
    "latitude",
    "longitude",
    "river_name",
    "wse",
    "wse_u",
    "wse_r_u",
    "wse_c",
    "wse_c_u",
    "width",
    "width_u",
    "width_c",
    "width_c_u",
    "slope",
    "slope_u",
    "slope_r_u",
    "slope2",
    "slope2_u",
    "slope2_r_u",
    "wse_unit",
    "width_unit",
    "slope_unit",
    "unconstrained_discharge_quality_bits",
    "constrained_discharge_quality_bits",
    "reach_quality",
    "reach_quality_bits",
    "dark_fraction",
    "ice_climatology_flag",
    "ice_dynamic_flag",
    "partial_flag",
    "good_node_count",
    "observed_node_fraction",
    "crossover_calibration_quality",
    "upstream_reach_count",
    "downstream_reach_count",
    "upstream_reach_ids",
    "downstream_reach_ids",
    "distance_to_outlet_m",
    "reach_length_m",
    "continent_id",
    "range_start_time",
    "range_end_time",
    "collection_name",
    "collection_version",
    "crid",
    "sword_version",
    "granule_id",
    "source_ingest_time",
    "collected_at",
    "response_sha256",
    "canonical_record_json",
)
DISCHARGE_COLUMNS = (
    "observation_revision_id",
    "algorithm",
    "is_constrained",
    "discharge_value",
    "discharge_uncertainty",
    "discharge_quality",
    "scale_factor",
    "discharge_unit",
    "collection_name",
    "collection_version",
    "sword_version",
    "response_sha256",
)


@dataclass(frozen=True)
class DiscoveryMetrics:
    reaches: int
    windows: int
    requests_planned: int
    requests_existing: int


@dataclass(frozen=True)
class IngestMetrics:
    requests_succeeded: int
    requests_no_data: int
    requests_failed: int
    source_rows: int
    observation_revisions_inserted: int
    discharge_revisions_inserted: int
    snapshot_links_inserted: int


class RiverPulseBatchError(RuntimeError):
    def __init__(self, metrics: IngestMetrics, failed_request_ids: Sequence[str]):
        super().__init__(
            f"{len(failed_request_ids)} RiverPulse request(s) failed after "
            "successful siblings were committed"
        )
        self.metrics = metrics
        self.failed_request_ids = tuple(failed_request_ids)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db_time(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def plan_source_requests(
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    backfill: bool = False,
    reach_ids: Sequence[str] | None = None,
    conn=None,
) -> DiscoveryMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    settings = get_riverpulse_settings()
    contract = settings.contract
    end = (window_end or _utc_now()).astimezone(timezone.utc)
    start = (
        RIVERPULSE_BACKFILL_START
        if backfill and window_start is None
        else window_start or end - timedelta(days=90)
    ).astimezone(timezone.utc)
    windows = calendar_year_windows(start, end)
    now = _db_time(_utc_now())
    with _use_conn(conn) as connection:
        available = [
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT reach_id
                FROM {riverpulse_raw_tbl("reaches")}
                ORDER BY reach_id
                """
            ).fetchall()
        ]
        if not available:
            raise RuntimeError(
                "RiverPulse network registry is empty; bootstrap the pinned "
                "SWORD network before discovery"
            )
        registered = (
            list(dict.fromkeys(str(value) for value in reach_ids))
            if reach_ids is not None
            else available
        )
        unknown = sorted(set(registered) - set(available))
        if unknown:
            raise ValueError(
                "RiverPulse discovery reaches are not registered: " + ", ".join(unknown)
            )
        if not registered:
            raise ValueError("RiverPulse discovery requires at least one reach")
        inserted = 0
        for reach_id in registered:
            for request_start, request_end in windows:
                request = HydrocronRequest.create(
                    reach_id=reach_id,
                    window_start=request_start,
                    window_end=request_end,
                    field_contract_version=str(contract["field_contract_version"]),
                    collection_name=str(contract["collection_name"]),
                )
                before = connection.execute(
                    f"SELECT 1 FROM {riverpulse_ops_tbl('source_requests')} "
                    "WHERE request_id = ?",
                    [request.request_id],
                ).fetchone()
                connection.execute(
                    f"""
                    INSERT OR IGNORE INTO
                    {riverpulse_ops_tbl("source_requests")}
                    (request_id, connector, collection_name, reach_id,
                     window_start, window_end, field_contract_version, status,
                     attempts, planned_at, updated_at)
                    VALUES (?, 'hydrocron', ?, ?, ?, ?, ?, 'planned', 0, ?, ?)
                    """,
                    [
                        request.request_id,
                        request.collection_name,
                        request.reach_id,
                        _db_time(request.window_start),
                        _db_time(request.window_end),
                        request.field_contract_version,
                        now,
                        now,
                    ],
                )
                inserted += int(before is None)
    total = len(registered) * len(windows)
    return DiscoveryMetrics(
        reaches=len(registered),
        windows=len(windows),
        requests_planned=inserted,
        requests_existing=total - inserted,
    )


def _request_from_row(row: tuple[object, ...]) -> HydrocronRequest:
    return HydrocronRequest(
        request_id=str(row[0]),
        reach_id=str(row[1]),
        window_start=_aware(row[2]),
        window_end=_aware(row[3]),
        field_contract_version=str(row[4]),
        collection_name=str(row[5]),
    )


def pending_requests(
    *, max_requests: int | None = None, conn=None
) -> list[HydrocronRequest]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    limit_sql = "" if max_requests is None else " LIMIT ?"
    params: list[object] = [] if max_requests is None else [max_requests]
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"""
            SELECT request_id, reach_id, window_start, window_end,
                   field_contract_version, collection_name
            FROM {riverpulse_ops_tbl("source_requests")}
            WHERE status IN ('planned', 'failed')
               OR (
                   status = 'running'
                   AND started_at < current_timestamp - INTERVAL '1 hour'
               )
            ORDER BY window_start, reach_id, request_id
            {limit_sql}
            """,
            params,
        ).fetchall()
    return [_request_from_row(row) for row in rows]


def _snapshot_path(
    raw_data_dir: Path,
    request: HydrocronRequest,
    response_sha256: str,
    *,
    no_data: bool,
) -> Path:
    extension = "txt" if no_data else "csv"
    path = (
        raw_data_dir
        / f"{request.window_start.year:04d}"
        / request.reach_id
        / request.request_id
        / f"{response_sha256}.{extension}"
    ).resolve()
    root = raw_data_dir.resolve()
    if not path.is_relative_to(root):
        raise ValueError("RiverPulse snapshot path escapes raw-data root")
    return path


def _atomic_snapshot(body: bytes, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(destination) != hashlib.sha256(body).hexdigest():
            raise ValueError("Existing RiverPulse snapshot checksum mismatch")
        return
    fd, name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(fd)
    temporary = Path(name)
    try:
        temporary.write_bytes(body)
        if sha256_file(temporary) != hashlib.sha256(body).hexdigest():
            raise ValueError("RiverPulse snapshot changed while writing")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _insert_observation(
    connection, observation: ParsedObservation, response_sha256: str
) -> tuple[int, int]:
    values = dict(observation.values)
    values["response_sha256"] = response_sha256
    before = connection.execute(
        f"""
        SELECT 1 FROM {riverpulse_raw_tbl("observation_revisions")}
        WHERE observation_revision_id = ?
        """,
        [values["observation_revision_id"]],
    ).fetchone()
    placeholders = ", ".join("?" for _ in OBSERVATION_COLUMNS)
    connection.execute(
        f"""
        INSERT OR IGNORE INTO {riverpulse_raw_tbl("observation_revisions")}
        ({", ".join(OBSERVATION_COLUMNS)})
        VALUES ({placeholders})
        """,
        [
            _db_time(value) if isinstance(value, datetime) else value
            for value in (values[column] for column in OBSERVATION_COLUMNS)
        ],
    )
    discharge_inserted = 0
    for discharge in observation.discharges:
        discharge_values = dict(discharge)
        discharge_values["response_sha256"] = response_sha256
        discharge_before = connection.execute(
            f"""
            SELECT 1 FROM {riverpulse_raw_tbl("discharge_revisions")}
            WHERE observation_revision_id = ?
              AND algorithm = ?
              AND is_constrained = ?
            """,
            [
                discharge_values["observation_revision_id"],
                discharge_values["algorithm"],
                discharge_values["is_constrained"],
            ],
        ).fetchone()
        connection.execute(
            f"""
            INSERT OR IGNORE INTO {riverpulse_raw_tbl("discharge_revisions")}
            ({", ".join(DISCHARGE_COLUMNS)})
            VALUES ({", ".join("?" for _ in DISCHARGE_COLUMNS)})
            """,
            [discharge_values[column] for column in DISCHARGE_COLUMNS],
        )
        discharge_inserted += int(discharge_before is None)
    return int(before is None), discharge_inserted


def persist_fetch_result(
    request: HydrocronRequest,
    result: FetchResult,
    *,
    collected_at: datetime | None = None,
    raw_data_dir: Path | None = None,
    conn=None,
) -> tuple[int, int, int, int]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    settings = get_riverpulse_settings()
    root = (raw_data_dir or settings.raw_data_dir).resolve()
    collected = collected_at or _utc_now()
    observations = (
        []
        if result.no_data
        else parse_csv_response(result.body, collected_at=collected)
    )
    mismatches = [
        str(observation.values["reach_id"])
        for observation in observations
        if str(observation.values["reach_id"]) != request.reach_id
    ]
    if mismatches:
        raise ValueError(
            "Hydrocron response contains rows for a different reach: "
            + ", ".join(sorted(set(mismatches)))
        )
    response_sha = hashlib.sha256(result.body).hexdigest()
    snapshot_id = hashlib.sha256(
        f"{request.request_id}\x1f{response_sha}".encode()
    ).hexdigest()
    path = _snapshot_path(root, request, response_sha, no_data=result.no_data)
    _atomic_snapshot(result.body, path)
    artifact_uri = str(path.relative_to(root))
    observation_inserted = 0
    discharge_inserted = 0
    link_inserted = 0
    now = _db_time(_utc_now())
    with _use_conn(conn) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"""
                INSERT OR IGNORE INTO
                {riverpulse_ops_tbl("source_snapshots")}
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    snapshot_id,
                    request.request_id,
                    response_sha,
                    artifact_uri,
                    result.status_code,
                    len(observations),
                    _db_time(collected),
                ],
            )
            for observation in observations:
                obs_count, discharge_count = _insert_observation(
                    connection, observation, response_sha
                )
                observation_inserted += obs_count
                discharge_inserted += discharge_count
                before = connection.execute(
                    f"""
                    SELECT 1 FROM
                    {riverpulse_raw_tbl("observation_snapshot_links")}
                    WHERE observation_revision_id = ? AND snapshot_id = ?
                    """,
                    [
                        observation.values["observation_revision_id"],
                        snapshot_id,
                    ],
                ).fetchone()
                connection.execute(
                    f"""
                    INSERT OR IGNORE INTO
                    {riverpulse_raw_tbl("observation_snapshot_links")}
                    VALUES (?, ?, ?)
                    """,
                    [
                        observation.values["observation_revision_id"],
                        snapshot_id,
                        now,
                    ],
                )
                link_inserted += int(before is None)
            connection.execute(
                f"""
                UPDATE {riverpulse_ops_tbl("source_requests")}
                SET status = 'success', attempts = ?, http_status = ?,
                    row_count = ?, error_message = NULL, finished_at = ?,
                    updated_at = ?
                WHERE request_id = ?
                """,
                [
                    result.attempts,
                    result.status_code,
                    len(observations),
                    now,
                    now,
                    request.request_id,
                ],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return (
        len(observations),
        observation_inserted,
        discharge_inserted,
        link_inserted,
    )


def _mark_started(request: HydrocronRequest, *, conn) -> None:
    now = _db_time(_utc_now())
    conn.execute(
        f"""
        UPDATE {riverpulse_ops_tbl("source_requests")}
        SET status = 'running', started_at = ?, error_message = NULL,
            updated_at = ?
        WHERE request_id = ?
        """,
        [now, now, request.request_id],
    )


def _mark_failed(
    request: HydrocronRequest,
    error: Exception,
    *,
    attempts: int,
    status_code: int | None,
    api_key: str | None,
    conn,
) -> None:
    now = _db_time(_utc_now())
    conn.execute(
        f"""
        UPDATE {riverpulse_ops_tbl("source_requests")}
        SET status = 'failed', attempts = ?, http_status = ?,
            error_message = ?, finished_at = ?, updated_at = ?
        WHERE request_id = ?
        """,
        [
            attempts,
            status_code,
            sanitize_error(str(error), secrets=(api_key,)),
            now,
            now,
            request.request_id,
        ],
    )


def sync_pending_requests(
    *,
    max_requests: int | None = None,
    conn=None,
    fetcher: Callable[[HydrocronRequest], FetchResult] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    raw_data_dir: Path | None = None,
) -> IngestMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    settings = get_riverpulse_settings()
    requests_to_run = pending_requests(max_requests=max_requests, conn=conn)
    succeeded = no_data = failed = source_rows = 0
    observations_inserted = discharges_inserted = links_inserted = 0
    failed_ids: list[str] = []
    with _use_conn(conn) as connection:
        for index, request in enumerate(requests_to_run):
            _mark_started(request, conn=connection)
            try:
                result = (
                    fetcher(request)
                    if fetcher
                    else fetch_hydrocron(
                        request,
                        api_key=settings.hydrocron_api_key,
                        sleep=sleep,
                    )
                )
                rows, observations, discharges, links = persist_fetch_result(
                    request,
                    result,
                    raw_data_dir=raw_data_dir,
                    conn=connection,
                )
                succeeded += 1
                no_data += int(result.no_data)
                source_rows += rows
                observations_inserted += observations
                discharges_inserted += discharges
                links_inserted += links
            except Exception as exc:
                attempts = exc.attempts if isinstance(exc, HydrocronFetchError) else 1
                status = (
                    exc.status_code if isinstance(exc, HydrocronFetchError) else None
                )
                _mark_failed(
                    request,
                    exc,
                    attempts=attempts,
                    status_code=status,
                    api_key=settings.hydrocron_api_key,
                    conn=connection,
                )
                failed += 1
                failed_ids.append(request.request_id)
            if index < len(requests_to_run) - 1:
                sleep(max(1.0, settings.request_interval_seconds))
    metrics = IngestMetrics(
        requests_succeeded=succeeded,
        requests_no_data=no_data,
        requests_failed=failed,
        source_rows=source_rows,
        observation_revisions_inserted=observations_inserted,
        discharge_revisions_inserted=discharges_inserted,
        snapshot_links_inserted=links_inserted,
    )
    if failed_ids:
        raise RiverPulseBatchError(metrics, failed_ids)
    return metrics


__all__ = [
    "DiscoveryMetrics",
    "IngestMetrics",
    "RiverPulseBatchError",
    "pending_requests",
    "persist_fetch_result",
    "plan_source_requests",
    "sync_pending_requests",
]
