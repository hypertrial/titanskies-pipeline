from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest
from scripts.build_region_artifacts import build_artifacts

from titanskies_pipeline.geography.registry import (
    load_geo_artifacts,
    persist_geo_artifacts,
)
from titanskies_pipeline.ingestion.tempo.aggregate import load_region_weights
from titanskies_pipeline.ingestion.tempo.cmr import DiscoveredGranule
from titanskies_pipeline.ingestion.tempo.sync import (
    SyncMetrics,
    _default_download,
    _download_with,
    _granule_destination,
    _load_sibling_grids,
    process_downloaded_granule,
    process_pending_granules,
    require_registered_geography,
    sync_granule_discovery,
    sync_region_registry,
)
from titanskies_pipeline.storage.duckdb.connection import get_connection
from titanskies_pipeline.storage.duckdb.granules import (
    mark_granule_status,
    sha256_file,
    upsert_discovered_granules,
)

NETCDF_FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "netcdf" / "tempo_no2_sample.nc"
)


@pytest.fixture(scope="module")
def netcdf_fixture() -> Path:
    if not NETCDF_FIXTURE.exists():
        from scripts.generate_netcdf_fixtures import write_fixture

        write_fixture(NETCDF_FIXTURE)
    return NETCDF_FIXTURE


@pytest.fixture
def artifacts(tmp_path):
    metrics = build_artifacts(tmp_path / "geo", use_synthetic=True)
    return load_geo_artifacts(metrics["manifest_path"], allow_synthetic=True)


def _granule(granule_id: str) -> DiscoveredGranule:
    observed = datetime(2026, 7, 12, 12)
    return DiscoveredGranule(
        granule_id=granule_id,
        concept_id="TEST",
        acquisition_start=observed,
        acquisition_end=observed,
        download_url="https://example.test/granule.nc",
        cmr_revision_at=observed,
    )


def _seed_registry(artifacts):
    with get_connection() as conn:
        persist_geo_artifacts(artifacts, conn=conn)


def test_sync_region_registry_and_discovery(monkeypatch, duck, artifacts):
    metrics = sync_region_registry(
        manifest_path=artifacts.manifest_path, allow_synthetic=True
    )
    assert metrics == {"regions_loaded": 9, "weights_loaded": 9}
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.discover_granules",
        lambda **_kwargs: [_granule("G-sync")],
    )
    first = sync_granule_discovery(lookback_hours=2)
    second = sync_granule_discovery(lookback_hours=2)
    assert (first.found, first.inserted, first.refreshed) == (1, 1, 0)
    assert (second.found, second.inserted, second.refreshed) == (1, 0, 1)
    with pytest.raises(ValueError, match="lookback_hours"):
        sync_granule_discovery(lookback_hours=0)


def test_process_downloaded_granule_writes_exact_hour(duck, artifacts, netcdf_fixture):
    _seed_registry(artifacts)
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-process")], conn=conn)
        written = process_downloaded_granule(
            "G-process",
            netcdf_fixture,
            geometry_version=artifacts.geometry_version,
            weights=load_region_weights(artifacts.weights_path),
            production=False,
            allow_synthetic=True,
            country_mask=np.ones((2, 2), dtype=bool),
            conn=conn,
        )
        row = conn.execute(
            """
            SELECT source_granule_count, revision
            FROM tempo_no2_raw.region_hour_aggregates LIMIT 1
            """
        ).fetchone()
    assert written == 9
    assert row == (1, 1)


def test_process_downloaded_granule_loads_registered_weights(
    duck, artifacts, netcdf_fixture
):
    _seed_registry(artifacts)
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-registered")], conn=conn)
        assert (
            process_downloaded_granule(
                "G-registered",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                allow_synthetic=True,
                conn=conn,
            )
            == 9
        )


def test_process_replaces_hour_with_exact_pooled_sibling(
    duck, artifacts, netcdf_fixture, tmp_path
):
    _seed_registry(artifacts)
    sibling_path = tmp_path / "sibling.nc"
    shutil.copy(netcdf_fixture, sibling_path)
    with get_connection() as conn:
        upsert_discovered_granules(
            [_granule("G-sibling"), _granule("G-current")], conn=conn
        )
        mark_granule_status(
            "G-sibling",
            download_status="downloaded",
            validation_status="validated",
            processing_status="processed",
            local_path=str(sibling_path),
            checksum_sha256=sha256_file(sibling_path),
            observation_time=datetime(2026, 7, 12, 12),
            observation_hour=datetime(2026, 7, 12, 12),
            conn=conn,
        )
        process_downloaded_granule(
            "G-current",
            netcdf_fixture,
            geometry_version=artifacts.geometry_version,
            weights=load_region_weights(artifacts.weights_path),
            production=False,
            allow_synthetic=True,
            conn=conn,
        )
        rows = conn.execute(
            """
            SELECT distinct source_granule_count, total_pixel_count
            FROM tempo_no2_raw.region_hour_aggregates
            """
        ).fetchall()
    assert all(source_count == 2 for source_count, _pixels in rows)
    assert all(pixels == 2 for _source_count, pixels in rows)


def test_missing_sibling_is_restored_and_checksum_verified(
    duck, artifacts, netcdf_fixture, tmp_path, monkeypatch
):
    _seed_registry(artifacts)
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR", raw_dir
    )
    with get_connection() as conn:
        upsert_discovered_granules(
            [_granule("G-missing"), _granule("G-current")], conn=conn
        )
        mark_granule_status(
            "G-missing",
            processing_status="processed",
            checksum_sha256=sha256_file(netcdf_fixture),
            observation_hour=datetime(2026, 7, 12, 12),
            conn=conn,
        )

        def restore(_granule_id, destination):
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(netcdf_fixture, destination)
            return destination

        process_downloaded_granule(
            "G-current",
            netcdf_fixture,
            geometry_version=artifacts.geometry_version,
            weights=load_region_weights(artifacts.weights_path),
            production=False,
            allow_synthetic=True,
            download_fn=restore,
            conn=conn,
        )
        restored = conn.execute(
            "SELECT local_path FROM tempo_no2_ops.granule_inventory WHERE granule_id='G-missing'"
        ).fetchone()[0]
    assert Path(restored).exists()


@pytest.mark.parametrize(
    ("checksum", "message"), [(None, "prior checksum"), ("bad", "checksum mismatch")]
)
def test_missing_sibling_requires_prior_matching_checksum(
    duck, artifacts, netcdf_fixture, tmp_path, monkeypatch, checksum, message
):
    _seed_registry(artifacts)
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR",
        tmp_path / "raw",
    )
    with get_connection() as conn:
        upsert_discovered_granules(
            [_granule("G-missing"), _granule("G-current")], conn=conn
        )
        mark_granule_status(
            "G-missing",
            processing_status="processed",
            checksum_sha256=checksum,
            observation_hour=datetime(2026, 7, 12, 12),
            conn=conn,
        )

        def restore(_granule_id, destination):
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(netcdf_fixture, destination)
            return destination

        with pytest.raises(RuntimeError, match=message):
            process_downloaded_granule(
                "G-current",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                weights=load_region_weights(artifacts.weights_path),
                production=False,
                allow_synthetic=True,
                download_fn=restore,
                conn=conn,
            )
        if checksum == "bad":
            assert not _granule_destination("G-missing").exists()


def test_sibling_hour_mismatch_and_duplicate_records_are_rejected(
    monkeypatch, tmp_path
):
    sibling = tmp_path / "sibling.nc"
    sibling.write_text("x")
    current = SimpleNamespace(observation_hour="2026-07-12 12:00:00")
    other = SimpleNamespace(observation_hour="2026-07-12 13:00:00")
    checksum = sha256_file(sibling)
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.processed_sibling_records",
        lambda *_a, **_k: [
            ("G-sibling", str(sibling), None, checksum),
            ("G-sibling", str(sibling), None, checksum),
        ],
    )
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.extract_grid",
        lambda *_a, **_k: current,
    )
    grids, _restored = _load_sibling_grids(
        current_granule_id="G-current",
        current_grid=current,
        production=False,
        download_fn=None,
        conn=MagicMock(),
    )
    assert len(grids) == 2
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.extract_grid", lambda *_a, **_k: other
    )
    with pytest.raises(RuntimeError, match="observation hour mismatch"):
        _load_sibling_grids(
            current_granule_id="G-current",
            current_grid=current,
            production=False,
            download_fn=None,
            conn=MagicMock(),
        )


def test_transaction_rollback_preserves_previous_hour(
    duck, artifacts, netcdf_fixture, monkeypatch
):
    _seed_registry(artifacts)
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-rollback")], conn=conn)
        monkeypatch.setattr(
            "titanskies_pipeline.ingestion.tempo.sync.upsert_grid_latest",
            MagicMock(side_effect=RuntimeError("grid write failed")),
        )
        with pytest.raises(RuntimeError, match="grid write failed"):
            process_downloaded_granule(
                "G-rollback",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                weights=load_region_weights(artifacts.weights_path),
                production=False,
                allow_synthetic=True,
                conn=conn,
            )
        assert (
            conn.execute(
                "SELECT count(*) FROM tempo_no2_raw.region_hour_aggregates"
            ).fetchone()[0]
            == 0
        )
        status = conn.execute(
            "SELECT processing_status FROM tempo_no2_ops.granule_inventory"
        ).fetchone()[0]
    assert status == "pending"


def test_processing_manifest_preconditions(duck, artifacts, netcdf_fixture):
    with get_connection() as conn:
        with pytest.raises(RuntimeError, match="manifest is missing"):
            process_downloaded_granule(
                "G-none",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                conn=conn,
            )
    with pytest.raises(RuntimeError, match="Region registry is empty"):
        process_pending_granules(allow_synthetic=True)

    _seed_registry(artifacts)
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-guard")], conn=conn)
        with pytest.raises(RuntimeError, match="rejects synthetic"):
            process_downloaded_granule(
                "G-guard",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                conn=conn,
            )
        conn.execute(
            "UPDATE tempo_no2_ops.geography_artifact_manifest "
            "SET weights_checksum = 'bad'"
        )
        with pytest.raises(RuntimeError, match="checksum mismatch"):
            process_downloaded_granule(
                "G-guard",
                netcdf_fixture,
                geometry_version=artifacts.geometry_version,
                allow_synthetic=True,
                conn=conn,
            )
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        process_pending_granules(allow_synthetic=True)


def test_process_pending_success_and_failure(
    duck, artifacts, netcdf_fixture, tmp_path, monkeypatch
):
    _seed_registry(artifacts)
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR", raw_dir
    )
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-pending")], conn=conn)

    def download(_granule_id, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(netcdf_fixture, destination)
        return destination

    metrics = process_pending_granules(download_fn=download, allow_synthetic=True)
    assert metrics == SyncMetrics(1, 1, 9, 0)

    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-fail")], conn=conn)
    with pytest.raises(RuntimeError, match="G-fail"):
        process_pending_granules(
            download_fn=lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")),
            allow_synthetic=True,
        )


def test_production_guards_and_download_helpers(duck, artifacts, monkeypatch, tmp_path):
    with pytest.raises(RuntimeError, match="not registered"):
        require_registered_geography()
    _seed_registry(artifacts)
    with pytest.raises(RuntimeError, match="rejects synthetic"):
        require_registered_geography()
    require_registered_geography(allow_synthetic=True)
    with pytest.raises(RuntimeError, match="rejects synthetic"):
        process_pending_granules()
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR", tmp_path
    )
    assert _granule_destination("folder/G").name == "folder_G.nc"
    assert _granule_destination("already.nc").name == "already.nc"
    downloaded = tmp_path / "earthaccess.nc"
    earthaccess = SimpleNamespace(
        login=lambda **_kwargs: True,
        download=lambda *_args: [downloaded],
    )
    downloaded.write_text("x")
    monkeypatch.setitem(__import__("sys").modules, "earthaccess", earthaccess)
    destination = tmp_path / "final.nc"
    assert _default_download("G", destination) == destination
    earthaccess.download = lambda *_a: [destination]
    assert _default_download("G-same", destination) == destination
    assert _download_with(None, "G-same", destination, None) == destination
    earthaccess.download = lambda *_a: []
    with pytest.raises(RuntimeError, match="returned no files"):
        _default_download("G-empty", tmp_path / "empty.nc")


def test_pending_limit_existing_file_and_cleanup_error(
    duck, artifacts, netcdf_fixture, tmp_path, monkeypatch
):
    _seed_registry(artifacts)
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR", raw_dir
    )
    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-existing")], conn=conn)
    assert process_pending_granules(
        max_granules=0, allow_synthetic=True
    ) == SyncMetrics(0, 0, 0, 0)

    destination = _granule_destination("G-existing")
    destination.parent.mkdir(parents=True)
    shutil.copy(netcdf_fixture, destination)
    assert process_pending_granules(allow_synthetic=True).downloaded == 0

    with get_connection() as conn:
        upsert_discovered_granules([_granule("G-cleanup")], conn=conn)
    cleanup_destination = _granule_destination("G-cleanup")
    real_unlink = Path.unlink

    def failing_unlink(path, *args, **kwargs):
        if path == cleanup_destination:
            raise OSError("cannot remove")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", failing_unlink)
    with pytest.raises(RuntimeError, match="G-cleanup"):
        process_pending_granules(
            download_fn=lambda *_a: (_ for _ in ()).throw(RuntimeError("boom")),
            allow_synthetic=True,
        )
    with get_connection() as conn:
        error = conn.execute(
            "SELECT error_message FROM tempo_no2_ops.granule_inventory "
            "WHERE granule_id = 'G-cleanup'"
        ).fetchone()[0]
    assert error == "boom; cleanup failed: cannot remove"
