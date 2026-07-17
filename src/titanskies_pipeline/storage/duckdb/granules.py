"""Granule inventory and exact region-hour persistence."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

from titanskies_pipeline.config.settings_warehouse import BASE_DIR
from titanskies_pipeline.geography.tempo_grid import cell_area_km2
from titanskies_pipeline.ingestion.tempo.aggregate import RegionHourAggregate
from titanskies_pipeline.ingestion.tempo.cmr import DiscoveredGranule
from titanskies_pipeline.ingestion.tempo.netcdf import NetcdfGrid, quality_mask
from titanskies_pipeline.storage.duckdb.connection import _use_conn
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    tempo_ops_tbl,
    tempo_raw_tbl,
)


@dataclass(frozen=True)
class DiscoveryMetrics:
    found: int
    inserted: int
    refreshed: int


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def upsert_discovered_granules(
    granules: list[DiscoveredGranule], *, conn=None
) -> DiscoveryMetrics:
    if not granules:
        return DiscoveryMetrics(found=0, inserted=0, refreshed=0)
    now = _now()
    batch = pa.Table.from_pylist(
        [
            {
                "granule_id": row.granule_id,
                "concept_id": row.concept_id,
                "acquisition_start": row.acquisition_start,
                "acquisition_end": row.acquisition_end,
                "cmr_revision_at": row.cmr_revision_at,
                "download_url": row.download_url,
                "seen_at": now,
            }
            for row in granules
        ]
    )
    with _use_conn(conn) as connection:
        connection.register("_tempo_discovery_batch", batch)
        try:
            inserted = connection.execute(
                f"""
                SELECT count(*) FROM _tempo_discovery_batch AS source
                LEFT JOIN {tempo_ops_tbl("granule_inventory")} AS target USING (granule_id)
                WHERE target.granule_id IS NULL
                """
            ).fetchone()[0]
            connection.execute(
                f"""
                MERGE INTO {tempo_ops_tbl("granule_inventory")} AS target
                USING _tempo_discovery_batch AS source
                ON target.granule_id = source.granule_id
                WHEN MATCHED THEN UPDATE SET
                    concept_id = source.concept_id,
                    acquisition_start = coalesce(source.acquisition_start, target.acquisition_start),
                    acquisition_end = coalesce(source.acquisition_end, target.acquisition_end),
                    cmr_revision_at = coalesce(source.cmr_revision_at, target.cmr_revision_at),
                    download_url = coalesce(source.download_url, target.download_url),
                    last_seen_at = source.seen_at,
                    updated_at = source.seen_at
                WHEN NOT MATCHED THEN INSERT (
                    granule_id, concept_id, acquisition_start, acquisition_end,
                    cmr_revision_at, last_seen_at, download_url, local_path,
                    checksum_sha256, file_size_bytes, observation_time,
                    observation_hour, discovery_status, download_status,
                    validation_status, processing_status, discovered_at,
                    downloaded_at, validated_at, processed_at, error_message, updated_at
                ) VALUES (
                    source.granule_id, source.concept_id, source.acquisition_start,
                    source.acquisition_end, source.cmr_revision_at, source.seen_at,
                    source.download_url, NULL, NULL, NULL, NULL, NULL, 'discovered',
                    'pending', 'pending', 'pending', source.seen_at, NULL, NULL, NULL,
                    NULL, source.seen_at
                )
                """
            )
        finally:
            connection.unregister("_tempo_discovery_batch")
    found = len(granules)
    return DiscoveryMetrics(
        found=found, inserted=int(inserted), refreshed=found - int(inserted)
    )


def mark_granule_status(
    granule_id: str,
    *,
    download_status: str | None = None,
    validation_status: str | None = None,
    processing_status: str | None = None,
    local_path: str | None = None,
    checksum_sha256: str | None = None,
    file_size_bytes: int | None = None,
    observation_time: datetime | None = None,
    observation_hour: datetime | None = None,
    error_message: str | None = None,
    clear_error: bool = False,
    conn=None,
) -> None:
    now = _now()
    fields: list[str] = ["updated_at = ?"]
    values: list[Any] = [now]
    for field, value in (
        ("download_status", download_status),
        ("validation_status", validation_status),
        ("processing_status", processing_status),
        ("local_path", local_path),
        ("checksum_sha256", checksum_sha256),
        ("file_size_bytes", file_size_bytes),
        ("observation_time", observation_time),
        ("observation_hour", observation_hour),
    ):
        if value is not None:
            fields.append(f"{field} = ?")
            values.append(value)
    if download_status == "downloaded":
        fields.append("downloaded_at = ?")
        values.append(now)
    if validation_status == "validated":
        fields.append("validated_at = ?")
        values.append(now)
    if processing_status == "processed":
        fields.append("processed_at = ?")
        values.append(now)
    if error_message is not None:
        fields.append("error_message = ?")
        values.append(error_message)
    elif clear_error:
        fields.append("error_message = NULL")
    values.append(granule_id)
    with _use_conn(conn) as connection:
        connection.execute(
            f"UPDATE {tempo_ops_tbl('granule_inventory')} "
            f"SET {', '.join(fields)} WHERE granule_id = ?",
            values,
        )


def list_pending_granules(*, conn=None) -> list[str]:
    return [granule_id for granule_id, _url in list_pending_granule_records(conn=conn)]


def list_pending_granule_records(*, conn=None) -> list[tuple[str, str | None]]:
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"""
            SELECT granule_id, download_url
            FROM {tempo_ops_tbl("granule_inventory")}
            WHERE download_status IN ('pending', 'failed')
               OR validation_status IN ('pending', 'failed')
               OR processing_status IN ('pending', 'failed')
            ORDER BY discovered_at, granule_id
            """
        ).fetchall()
    return [(str(row[0]), None if row[1] is None else str(row[1])) for row in rows]


def processed_sibling_records(
    observation_hour: datetime, *, exclude_granule_id: str, conn=None
) -> list[tuple[str, str | None, str | None, str | None]]:
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"""
            SELECT granule_id, local_path, download_url, checksum_sha256
            FROM {tempo_ops_tbl("granule_inventory")}
            WHERE observation_hour = ?
              AND processing_status = 'processed'
              AND granule_id <> ?
            ORDER BY granule_id
            """,
            [observation_hour, exclude_granule_id],
        ).fetchall()
    return [
        tuple(None if value is None else str(value) for value in row) for row in rows
    ]


def prune_processed_granule_files(
    *, retention_days: int, raw_dir: Path, now: datetime | None = None, conn=None
) -> int:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    cutoff = (now or _now()) - timedelta(days=retention_days)
    raw_root = raw_dir.expanduser().resolve()
    deleted = 0
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"""
            SELECT granule_id, local_path FROM {tempo_ops_tbl("granule_inventory")}
            WHERE processing_status = 'processed' AND processed_at < ?
              AND local_path IS NOT NULL
            """,
            [cutoff],
        ).fetchall()
        for granule_id, local_path in rows:
            candidate = Path(str(local_path)).expanduser()
            if not candidate.is_absolute():
                candidate = BASE_DIR / candidate
            candidate = candidate.resolve()
            if not candidate.is_relative_to(raw_root):
                raise ValueError(
                    f"refusing to prune granule path outside raw directory: {candidate}"
                )
            existed = candidate.exists()
            candidate.unlink(missing_ok=True)
            connection.execute(
                f"UPDATE {tempo_ops_tbl('granule_inventory')} "
                "SET local_path = NULL WHERE granule_id = ?",
                [granule_id],
            )
            deleted += int(existed)
    return deleted


def replace_region_hour_aggregates(
    aggregates: list[RegionHourAggregate], *, conn=None
) -> int:
    if not aggregates:
        raise ValueError("Region-hour replacement cannot be empty")
    hours = {row.observation_hour for row in aggregates}
    if len(hours) != 1:
        raise ValueError("Region-hour replacement must contain exactly one hour")
    now = _now()
    with _use_conn(conn) as connection:
        revision = connection.execute(
            "SELECT nextval('tempo_no2_hour_revision')"
        ).fetchone()[0]
        connection.execute(
            f"DELETE FROM {tempo_raw_tbl('region_hour_aggregates')} "
            "WHERE observation_hour = CAST(? AS TIMESTAMP)",
            [next(iter(hours))],
        )
        connection.executemany(
            f"""
            INSERT INTO {tempo_raw_tbl("region_hour_aggregates")}
            (observation_hour, canonical_region_id, country_code, region_type,
             no2_mean, no2_median, no2_p90, valid_pixel_count, total_pixel_count,
             valid_area_km2, total_area_km2, coverage_fraction,
             quality_flag_accepted, source_granule_count, all_granules_validated,
             revision, geometry_version, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    row.observation_hour,
                    row.canonical_region_id,
                    row.country_code,
                    row.region_type,
                    row.no2_mean,
                    row.no2_median,
                    row.no2_p90,
                    row.valid_pixel_count,
                    row.total_pixel_count,
                    row.valid_area_km2,
                    row.total_area_km2,
                    row.coverage_fraction,
                    row.quality_flag_accepted,
                    row.source_granule_count,
                    row.all_granules_validated,
                    revision,
                    row.geometry_version,
                    now,
                ]
                for row in aggregates
            ],
        )
    return len(aggregates)


def grid_latest_batch(
    *,
    granule_id: str,
    grid: NetcdfGrid,
    supported_mask: np.ndarray,
    accepted_flags: set[int],
    ingested_at: datetime | None = None,
) -> pa.Table:
    if supported_mask.shape != grid.no2.shape:
        raise ValueError("Supported-country mask does not match NetCDF grid")
    observed = np.isfinite(grid.no2) | (grid.quality != -9999)
    rows, cols = np.nonzero(supported_mask & observed)
    no2 = np.asarray(grid.no2[rows, cols], dtype=float)
    quality = np.asarray(grid.quality[rows, cols], dtype=np.int32)
    accepted = quality_mask(quality, accepted_flags)
    if grid.lat.ndim == 1:
        latitude = np.asarray(grid.lat[rows], dtype=float)
        longitude = np.asarray(grid.lon[cols], dtype=float)
    else:
        latitude = np.asarray(grid.lat[rows, cols], dtype=float)
        longitude = np.asarray(grid.lon[rows, cols], dtype=float)
    count = rows.size

    def repeated_text(value: str) -> pa.DictionaryArray:
        return pa.DictionaryArray.from_arrays(
            pa.array(np.zeros(count, dtype=np.int8)), pa.array([value])
        )

    observation_time = grid.observation_time or datetime.fromisoformat(
        grid.observation_hour
    )
    timestamp = ingested_at or _now()
    return pa.table(
        {
            "grid_row": pa.array(rows, type=pa.int32()),
            "grid_col": pa.array(cols, type=pa.int32()),
            "latitude": latitude,
            "longitude": longitude,
            "cell_area_km2": cell_area_km2(latitude),
            "observation_time": pa.array([observation_time] * count),
            "observation_hour": repeated_text(grid.observation_hour),
            "no2": pa.array(no2, mask=~np.isfinite(no2), type=pa.float64()),
            "quality_flag": quality,
            "quality_flag_accepted": accepted,
            "granule_id": repeated_text(granule_id),
            "ingested_at": pa.array(np.full(count, np.datetime64(timestamp))),
        }
    )


def upsert_grid_latest(batch: pa.Table, *, conn=None) -> int:
    with _use_conn(conn) as connection:
        connection.register("_tempo_grid_latest_batch", batch)
        try:
            connection.execute(
                f"""
                INSERT INTO {tempo_raw_tbl("grid_latest")}
                SELECT grid_row, grid_col, latitude, longitude, cell_area_km2,
                       observation_time, CAST(observation_hour AS TIMESTAMP), no2,
                       quality_flag, quality_flag_accepted, granule_id, ingested_at
                FROM _tempo_grid_latest_batch
                ON CONFLICT (grid_row, grid_col) DO UPDATE SET
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    cell_area_km2 = excluded.cell_area_km2,
                    observation_time = excluded.observation_time,
                    observation_hour = excluded.observation_hour,
                    no2 = excluded.no2,
                    quality_flag = excluded.quality_flag,
                    quality_flag_accepted = excluded.quality_flag_accepted,
                    granule_id = excluded.granule_id,
                    ingested_at = excluded.ingested_at
                WHERE excluded.observation_time > grid_latest.observation_time
                   OR (excluded.observation_time = grid_latest.observation_time
                       AND excluded.ingested_at > grid_latest.ingested_at)
                """
            )
        finally:
            connection.unregister("_tempo_grid_latest_batch")
    return batch.num_rows


def load_region_meta(*, conn=None) -> dict[str, tuple[str, str]]:
    with _use_conn(conn) as connection:
        rows = connection.execute(
            f"SELECT canonical_region_id, country_code, region_type "
            f"FROM {tempo_ops_tbl('region_registry')}"
        ).fetchall()
    return {str(row[0]): (str(row[1]), str(row[2])) for row in rows}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "DiscoveryMetrics",
    "grid_latest_batch",
    "list_pending_granule_records",
    "list_pending_granules",
    "load_region_meta",
    "mark_granule_status",
    "processed_sibling_records",
    "prune_processed_granule_files",
    "replace_region_hour_aggregates",
    "sha256_file",
    "upsert_discovered_granules",
    "upsert_grid_latest",
]
