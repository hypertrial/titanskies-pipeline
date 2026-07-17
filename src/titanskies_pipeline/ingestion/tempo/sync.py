"""TEMPO NO2 discovery and exact region-hour processing entry points."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from titanskies_pipeline.config.settings import (
    TEMPO_NO2_CMR_CONCEPT_ID,
    TEMPO_NO2_CONTRACT,
    TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS,
    TEMPO_NO2_RAW_DATA_DIR,
    TEMPO_NO2_RAW_RETENTION_DAYS,
)
from titanskies_pipeline.geography.registry import (
    load_geo_artifacts,
    persist_geo_artifacts,
)
from titanskies_pipeline.ingestion.tempo.aggregate import (
    RegionWeights,
    aggregate_region_hour,
    load_region_weights,
    supported_grid_mask,
)
from titanskies_pipeline.ingestion.tempo.cmr import discover_granules
from titanskies_pipeline.ingestion.tempo.netcdf import NetcdfGrid, extract_grid
from titanskies_pipeline.storage.duckdb.connection import _use_conn, get_connection
from titanskies_pipeline.storage.duckdb.granules import (
    DiscoveryMetrics,
    grid_latest_batch,
    list_pending_granule_records,
    load_region_meta,
    mark_granule_status,
    processed_sibling_records,
    prune_processed_granule_files,
    replace_region_hour_aggregates,
    sha256_file,
    upsert_discovered_granules,
    upsert_grid_latest,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import tempo_ops_tbl


@dataclass(frozen=True)
class SyncMetrics:
    downloaded: int
    processed: int
    aggregates_written: int
    raw_files_pruned: int = 0


def sync_region_registry(
    *, manifest_path: Path | None = None, allow_synthetic: bool = False
) -> dict[str, int]:
    artifacts = load_geo_artifacts(
        manifest_path=manifest_path, allow_synthetic=allow_synthetic
    )
    return persist_geo_artifacts(artifacts)


def sync_granule_discovery(*, lookback_hours: int | None = None) -> DiscoveryMetrics:
    hours = (
        TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS if lookback_hours is None else lookback_hours
    )
    if hours < 1:
        raise ValueError("lookback_hours must be >= 1")
    granules = discover_granules(
        lookback_hours=hours, concept_id=TEMPO_NO2_CMR_CONCEPT_ID
    )
    return upsert_discovered_granules(granules)


def require_registered_geography(*, allow_synthetic: bool = False) -> None:
    with get_connection() as connection:
        row = connection.execute(
            f"SELECT artifact_mode FROM {tempo_ops_tbl('geography_artifact_manifest')} LIMIT 1"
        ).fetchone()
    if not row:
        raise RuntimeError(
            "Production geography is not registered. Materialize "
            "tempo/no2/ops/region_registry before running the pipeline."
        )
    if str(row[0]) != "production" and not allow_synthetic:
        raise RuntimeError("Production pipeline rejects synthetic geography artifacts")


def _ensure_earthaccess_login() -> None:
    import earthaccess

    earthaccess.login(strategy="environment")


def _granule_destination(granule_id: str) -> Path:
    name = Path(granule_id).name
    if not name.endswith(".nc"):
        name = f"{granule_id.replace('/', '_')}.nc"
    return TEMPO_NO2_RAW_DATA_DIR / name


def _default_download(
    granule_id: str, destination: Path, *, download_url: str | None = None
) -> Path:
    import earthaccess

    _ensure_earthaccess_login()
    TEMPO_NO2_RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = download_url or granule_id
    results = earthaccess.download([target], str(destination.parent))
    if not results:
        raise RuntimeError(f"earthaccess returned no files for {granule_id}")
    downloaded = Path(results[0])
    if downloaded.resolve() != destination.resolve():
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.unlink(missing_ok=True)
        shutil.move(str(downloaded), str(destination))
    return destination


def _observation_hour(grid: NetcdfGrid) -> datetime:
    return datetime.fromisoformat(grid.observation_hour)


def _download_with(
    download_fn: Callable[..., Path] | None,
    granule_id: str,
    destination: Path,
    download_url: str | None,
) -> Path:
    if download_fn is None:
        return _default_download(granule_id, destination, download_url=download_url)
    return download_fn(granule_id, destination)


def _load_sibling_grids(
    *,
    current_granule_id: str,
    current_grid: NetcdfGrid,
    production: bool,
    download_fn: Callable[..., Path] | None,
    conn,
) -> tuple[list[tuple[str, NetcdfGrid]], list[tuple[str, str]]]:
    grids = [(current_granule_id, current_grid)]
    restored: list[tuple[str, str]] = []
    siblings = processed_sibling_records(
        _observation_hour(current_grid),
        exclude_granule_id=current_granule_id,
        conn=conn,
    )
    seen = {current_granule_id}
    for sibling_id, local_path, download_url, expected_checksum in siblings:
        if sibling_id in seen:
            continue
        seen.add(sibling_id)
        path = Path(local_path) if local_path else _granule_destination(sibling_id)
        restored_now = not path.exists()
        if restored_now:
            if not expected_checksum:
                raise RuntimeError(
                    f"Cannot restore sibling {sibling_id}: prior checksum is missing"
                )
            _download_with(download_fn, sibling_id, path, download_url)
        try:
            actual_checksum = sha256_file(path)
            if not expected_checksum or actual_checksum != expected_checksum:
                raise RuntimeError(f"Sibling checksum mismatch: {sibling_id}")
            grid = extract_grid(path, production=production)
            if grid.observation_hour != current_grid.observation_hour:
                raise RuntimeError(f"Sibling observation hour mismatch: {sibling_id}")
        except Exception:
            if restored_now:
                path.unlink(missing_ok=True)
            raise
        if restored_now:
            restored.append((sibling_id, str(path)))
        grids.append((sibling_id, grid))
    return grids, restored


def process_downloaded_granule(
    granule_id: str,
    local_path: Path,
    *,
    geometry_version: str,
    accepted_flags: str | None = None,
    weights: RegionWeights | None = None,
    region_meta: dict[str, tuple[str, str]] | None = None,
    country_mask=None,
    production: bool | None = None,
    allow_synthetic: bool = False,
    checksum_sha256: str | None = None,
    download_fn: Callable[..., Path] | None = None,
    conn=None,
) -> int:
    flags = accepted_flags or str(TEMPO_NO2_CONTRACT["accepted_quality_flags"])
    with _use_conn(conn) as connection:
        if weights is None:
            manifest = connection.execute(
                f"""
                SELECT weights_path, weights_checksum, artifact_mode
                FROM {tempo_ops_tbl("geography_artifact_manifest")}
                WHERE geometry_version = ?
                """,
                [geometry_version],
            ).fetchone()
            if not manifest:
                raise RuntimeError("Geography artifact manifest is missing")
            weights_path = Path(str(manifest[0]))
            if sha256_file(weights_path) != str(manifest[1]):
                raise RuntimeError("Grid-region weight artifact checksum mismatch")
            mode = str(manifest[2])
            if mode != "production" and not allow_synthetic:
                raise RuntimeError(
                    "Production ingestion rejects synthetic geography artifacts"
                )
            production = mode == "production"
            weights = load_region_weights(weights_path)
        production = bool(production)
        if region_meta is None:
            region_meta = load_region_meta(conn=connection)

        current_grid = extract_grid(local_path, production=production)
        grids, restored = _load_sibling_grids(
            current_granule_id=granule_id,
            current_grid=current_grid,
            production=production,
            download_fn=download_fn,
            conn=connection,
        )
        aggregates = aggregate_region_hour(
            grids=grids,
            weights=weights,
            region_meta=region_meta,
            geometry_version=geometry_version,
            accepted_flags=flags,
        )
        if country_mask is None:
            country_mask = supported_grid_mask(
                weights, region_meta, current_grid.no2.shape
            )
        accepted = {
            int(value.strip())
            for value in flags.replace("|", ",").split(",")
            if value.strip()
        }
        latest = grid_latest_batch(
            granule_id=granule_id,
            grid=current_grid,
            supported_mask=country_mask,
            accepted_flags=accepted,
        )
        checksum = checksum_sha256 or sha256_file(local_path)

        connection.execute("BEGIN TRANSACTION")
        try:
            written = replace_region_hour_aggregates(aggregates, conn=connection)
            upsert_grid_latest(latest, conn=connection)
            for sibling_id, restored_path in restored:
                mark_granule_status(
                    sibling_id, local_path=restored_path, conn=connection
                )
            mark_granule_status(
                granule_id,
                download_status="downloaded",
                validation_status="validated",
                processing_status="processed",
                local_path=str(local_path),
                checksum_sha256=checksum,
                file_size_bytes=local_path.stat().st_size,
                observation_time=current_grid.observation_time,
                observation_hour=_observation_hour(current_grid),
                clear_error=True,
                conn=connection,
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return written


def process_pending_granules(
    *,
    download_fn: Callable[..., Path] | None = None,
    max_granules: int | None = None,
    allow_synthetic: bool = False,
) -> SyncMetrics:
    downloaded = 0
    processed = 0
    aggregates_written = 0
    failures: list[str] = []

    with get_connection() as conn:
        manifest = conn.execute(
            f"""
            SELECT geometry_version, weights_path, weights_checksum, artifact_mode
            FROM {tempo_ops_tbl("geography_artifact_manifest")} LIMIT 1
            """
        ).fetchone()
        if not manifest:
            raise RuntimeError(
                "Region registry is empty. Run tempo/no2/ops/region_registry first."
            )
        version, weights_path_value, expected_checksum, mode = map(str, manifest)
        if mode != "production" and not allow_synthetic:
            raise RuntimeError(
                "Production ingestion rejects synthetic geography artifacts"
            )
        weights_path = Path(weights_path_value)
        if sha256_file(weights_path) != expected_checksum:
            raise RuntimeError("Grid-region weight artifact checksum mismatch")
        weights = load_region_weights(weights_path)
        region_meta = load_region_meta(conn=conn)
        raw_files_pruned = prune_processed_granule_files(
            retention_days=TEMPO_NO2_RAW_RETENTION_DAYS,
            raw_dir=TEMPO_NO2_RAW_DATA_DIR,
            conn=conn,
        )
        pending = list_pending_granule_records(conn=conn)
        if max_granules is not None:
            pending = pending[:max_granules]

        for granule_id, download_url in pending:
            destination = _granule_destination(granule_id)
            try:
                if not destination.exists():
                    _download_with(download_fn, granule_id, destination, download_url)
                    downloaded += 1
                checksum = sha256_file(destination)
                written = process_downloaded_granule(
                    granule_id,
                    destination,
                    geometry_version=version,
                    weights=weights,
                    region_meta=region_meta,
                    production=mode == "production",
                    allow_synthetic=allow_synthetic,
                    checksum_sha256=checksum,
                    download_fn=download_fn,
                    conn=conn,
                )
                aggregates_written += written
                processed += 1
            except Exception as exc:
                error_message = str(exc)
                try:
                    destination.unlink(missing_ok=True)
                except OSError as cleanup_exc:
                    error_message = f"{error_message}; cleanup failed: {cleanup_exc}"
                mark_granule_status(
                    granule_id,
                    download_status="failed",
                    validation_status="failed",
                    processing_status="failed",
                    error_message=error_message,
                    conn=conn,
                )
                failures.append(granule_id)

    if failures:
        shown = ", ".join(failures[:5])
        remainder = len(failures) - 5
        suffix = f" (+{remainder} more)" if remainder > 0 else ""
        raise RuntimeError(
            f"{len(failures)} TEMPO granule(s) failed: {shown}{suffix}; "
            "see tempo_no2_ops.granule_inventory for details"
        )
    return SyncMetrics(
        downloaded=downloaded,
        processed=processed,
        aggregates_written=aggregates_written,
        raw_files_pruned=raw_files_pruned,
    )


__all__ = [
    "SyncMetrics",
    "process_downloaded_granule",
    "process_pending_granules",
    "require_registered_geography",
    "sync_granule_discovery",
    "sync_region_registry",
]
