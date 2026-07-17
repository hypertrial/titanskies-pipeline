from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from titanskies_pipeline.ingestion.tempo.aggregate import (
    RegionHourAggregate,
    load_region_weights,
)
from titanskies_pipeline.ingestion.tempo.cmr import DiscoveredGranule
from titanskies_pipeline.ingestion.tempo.netcdf import NetcdfGrid
from titanskies_pipeline.storage.duckdb.connection import get_connection
from titanskies_pipeline.storage.duckdb.granules import (
    grid_latest_batch,
    list_pending_granule_records,
    list_pending_granules,
    load_region_meta,
    mark_granule_status,
    prune_processed_granule_files,
    replace_region_hour_aggregates,
    sha256_file,
    upsert_discovered_granules,
    upsert_grid_latest,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    tempo_ops_tbl,
    tempo_raw_tbl,
)


def _sample_granule(granule_id: str = "G-001") -> DiscoveredGranule:
    now = datetime(2026, 7, 12, 12, 0, 0)
    return DiscoveredGranule(
        granule_id=granule_id,
        concept_id="C3685668637-LARC_CLOUD",
        acquisition_start=now,
        acquisition_end=now,
        download_url="https://example.test/granule.nc",
        cmr_revision_at=now,
    )


def test_upsert_discovered_granules_idempotent(duck):
    granules = [_sample_granule("G-100"), _sample_granule("G-101")]
    with get_connection() as conn:
        first = upsert_discovered_granules(granules, conn=conn)
        second = upsert_discovered_granules(granules, conn=conn)
        count = conn.execute(
            f"SELECT COUNT(*) FROM {tempo_ops_tbl('granule_inventory')}"
        ).fetchone()[0]
    assert (first.found, first.inserted, first.refreshed) == (2, 2, 0)
    assert (second.found, second.inserted, second.refreshed) == (2, 0, 2)
    assert count == 2


def test_empty_discovery_and_invalid_hour_replacements(duck):
    assert upsert_discovered_granules([]).found == 0
    with pytest.raises(ValueError, match="cannot be empty"):
        replace_region_hour_aggregates([])
    first = RegionHourAggregate(
        observation_hour="2026-07-12 12:00:00",
        canonical_region_id="US",
        country_code="US",
        region_type="country",
        no2_mean=1.0,
        no2_median=1.0,
        no2_p90=1.0,
        valid_pixel_count=1,
        total_pixel_count=1,
        valid_area_km2=1.0,
        total_area_km2=1.0,
        coverage_fraction=1.0,
        quality_flag_accepted=True,
        source_granule_count=1,
        all_granules_validated=True,
        geometry_version="test",
    )
    second = RegionHourAggregate(
        **{**first.__dict__, "observation_hour": "2026-07-12 13:00:00"}
    )
    with pytest.raises(ValueError, match="exactly one hour"):
        replace_region_hour_aggregates([first, second])


def test_mark_granule_status_all_branches(duck):
    granule = _sample_granule("G-200")
    with get_connection() as conn:
        upsert_discovered_granules([granule], conn=conn)

        mark_granule_status(
            granule.granule_id,
            download_status="downloaded",
            validation_status="validated",
            processing_status="processed",
            local_path="/tmp/g.nc",
            checksum_sha256="abc123",
            file_size_bytes=42,
            conn=conn,
        )
        row = conn.execute(
            f"""
            SELECT download_status, validation_status, processing_status,
                   local_path, checksum_sha256, file_size_bytes,
                   downloaded_at, validated_at, processed_at, error_message
            FROM {tempo_ops_tbl("granule_inventory")}
            WHERE granule_id = ?
            """,
            [granule.granule_id],
        ).fetchone()

        assert row[0] == "downloaded"
        assert row[1] == "validated"
        assert row[2] == "processed"
        assert row[3] == "/tmp/g.nc"
        assert row[4] == "abc123"
        assert row[5] == 42
        assert row[6] is not None
        assert row[7] is not None
        assert row[8] is not None
        assert row[9] is None

        mark_granule_status(
            granule.granule_id,
            download_status="failed",
            validation_status="failed",
            processing_status="failed",
            error_message="boom",
            conn=conn,
        )
        error_row = conn.execute(
            f"SELECT error_message FROM {tempo_ops_tbl('granule_inventory')} WHERE granule_id = ?",
            [granule.granule_id],
        ).fetchone()
        assert error_row[0] == "boom"

        mark_granule_status(
            granule.granule_id,
            download_status="pending",
            conn=conn,
        )
        pending_row = conn.execute(
            f"SELECT download_status, downloaded_at FROM {tempo_ops_tbl('granule_inventory')} WHERE granule_id = ?",
            [granule.granule_id],
        ).fetchone()
        assert pending_row[0] == "pending"


def test_list_pending_granules(duck):
    with get_connection() as conn:
        upsert_discovered_granules([_sample_granule("G-pending")], conn=conn)
        mark_granule_status(
            "G-pending",
            download_status="downloaded",
            processing_status="pending",
            conn=conn,
        )
        upsert_discovered_granules([_sample_granule("G-download-pending")], conn=conn)
        pending = list_pending_granules(conn=conn)
        records = list_pending_granule_records(conn=conn)
    assert "G-pending" in pending
    assert "G-download-pending" in pending
    assert records[0][0] in pending


def test_replace_region_hour_aggregates(duck):
    aggregate = RegionHourAggregate(
        observation_hour="2026-07-12 12:00:00",
        canonical_region_id="US-CA-037",
        country_code="US",
        region_type="county",
        no2_mean=1.0,
        no2_median=1.0,
        no2_p90=1.0,
        valid_pixel_count=1,
        total_pixel_count=1,
        valid_area_km2=1.0,
        total_area_km2=1.0,
        coverage_fraction=1.0,
        quality_flag_accepted=True,
        source_granule_count=1,
        all_granules_validated=True,
        geometry_version="test-v1",
    )
    with get_connection() as conn:
        written = replace_region_hour_aggregates([aggregate], conn=conn)
        row = conn.execute(
            """
            SELECT canonical_region_id, no2_mean, source_granule_count
            FROM "tempo_no2_raw"."region_hour_aggregates"
            WHERE observation_hour = ?
            """,
            [aggregate.observation_hour],
        ).fetchone()
    assert written == 1
    assert row == ("US-CA-037", 1.0, 1)


def test_load_region_weights_and_meta(duck, geo_fixtures):
    from titanskies_pipeline.geography.registry import (
        load_geo_artifacts,
        persist_geo_artifacts,
    )

    artifacts = load_geo_artifacts(
        manifest_path=geo_fixtures["manifest"],
        allow_synthetic=True,
    )
    with get_connection() as conn:
        persist_geo_artifacts(artifacts, conn=conn)
        weights = load_region_weights(artifacts.weights_path)
        meta = load_region_meta(conn=conn)
    assert weights.rows.size > 0
    assert meta["US-CA-037"] == ("US", "county")


def test_sha256_file(tmp_path):
    import hashlib

    path = tmp_path / "payload.bin"
    path.write_bytes(b"hello-titanskies")
    expected = hashlib.sha256(b"hello-titanskies").hexdigest()
    assert sha256_file(path) == expected


def _latest_batch(granule_id: str, hour: str, value: float):
    grid = NetcdfGrid(
        no2=np.array([[value, np.nan]]),
        quality=np.array([[0, -9999]]),
        lat=np.array([35.0]),
        lon=np.array([-106.0, -105.98]),
        observation_hour=hour,
    )
    return grid_latest_batch(
        granule_id=granule_id,
        grid=grid,
        supported_mask=np.array([[True, True]]),
        accepted_flags={0, 1},
        ingested_at=datetime.fromisoformat(hour) + timedelta(minutes=5),
    )


def test_grid_latest_bulk_upsert_ordering_and_idempotent_replay(duck):
    with get_connection() as conn:
        assert (
            upsert_grid_latest(
                _latest_batch("g-new", "2026-07-12 20:00:00", 2.0), conn=conn
            )
            == 1
        )
        upsert_grid_latest(
            _latest_batch("g-old", "2026-07-12 19:00:00", 1.0), conn=conn
        )
        upsert_grid_latest(
            _latest_batch("g-new", "2026-07-12 20:00:00", 2.0), conn=conn
        )
        rows = conn.execute(
            f"""
            SELECT grid_col, no2, quality_flag_accepted, granule_id
            FROM {tempo_raw_tbl("grid_latest")}
            ORDER BY grid_col
            """
        ).fetchall()
    assert rows == [(0, 2.0, True, "g-new")]


def test_grid_latest_keeps_observed_masked_no2_and_quality_semantics():
    grid = NetcdfGrid(
        no2=np.array([[np.nan, np.nan, 3.0]]),
        quality=np.array([[0, -9999, 2]]),
        lat=np.array([35.0]),
        lon=np.array([-106.0, -105.98, -105.96]),
        observation_hour="2026-07-12 20:00:00",
    )
    batch = grid_latest_batch(
        granule_id="g-masks",
        grid=grid,
        supported_mask=np.ones((1, 3), dtype=bool),
        accepted_flags={0, 1},
    ).to_pylist()
    assert [(row["grid_col"], row["quality_flag_accepted"]) for row in batch] == [
        (0, True),
        (2, False),
    ]
    assert batch[0]["no2"] is None


def test_grid_latest_batch_rejects_mask_shape():
    grid = NetcdfGrid(
        no2=np.ones((1, 1)),
        quality=np.zeros((1, 1), dtype=int),
        lat=np.array([35.0]),
        lon=np.array([-106.0]),
        observation_hour="2026-07-12 20:00:00",
    )
    with pytest.raises(ValueError, match="mask does not match"):
        grid_latest_batch(
            granule_id="g-mask",
            grid=grid,
            supported_mask=np.ones((2, 1), dtype=bool),
            accepted_flags={0},
        )


def test_prune_processed_granule_files_deletes_old_and_reconciles_missing(
    duck, tmp_path, monkeypatch
):
    from titanskies_pipeline.storage.duckdb import granules as granules_module

    monkeypatch.setattr(granules_module, "BASE_DIR", tmp_path)
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    old_file = raw_dir / "old.nc"
    old_file.write_text("old")
    old = datetime(2026, 6, 1)
    now = datetime(2026, 7, 12)
    with get_connection() as conn:
        for granule_id, local_path in (
            ("G-old", Path("raw/old.nc")),
            ("G-missing", raw_dir / "missing.nc"),
            ("G-recent", raw_dir / "recent.nc"),
        ):
            upsert_discovered_granules([_sample_granule(granule_id)], conn=conn)
            mark_granule_status(
                granule_id,
                download_status="downloaded",
                validation_status="validated",
                processing_status="processed",
                local_path=str(local_path),
                conn=conn,
            )
        (raw_dir / "recent.nc").write_text("recent")
        conn.execute(
            f"UPDATE {tempo_ops_tbl('granule_inventory')} SET processed_at = ? WHERE granule_id IN ('G-old', 'G-missing')",
            [old],
        )
        conn.execute(
            f"UPDATE {tempo_ops_tbl('granule_inventory')} SET processed_at = ? WHERE granule_id = 'G-recent'",
            [now - timedelta(days=1)],
        )
        assert (
            prune_processed_granule_files(
                retention_days=30,
                raw_dir=raw_dir,
                now=now,
                conn=conn,
            )
            == 1
        )
        rows = dict(
            conn.execute(
                f"SELECT granule_id, local_path FROM {tempo_ops_tbl('granule_inventory')}"
            ).fetchall()
        )
    assert not old_file.exists()
    assert rows["G-old"] is None
    assert rows["G-missing"] is None
    assert rows["G-recent"].endswith("recent.nc")


def test_prune_processed_granule_files_rejects_unsafe_paths(duck, tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    outside = tmp_path / "outside.nc"
    outside.write_text("keep")
    with get_connection() as conn:
        upsert_discovered_granules([_sample_granule("G-outside")], conn=conn)
        mark_granule_status(
            "G-outside",
            download_status="downloaded",
            validation_status="validated",
            processing_status="processed",
            local_path=str(outside),
            conn=conn,
        )
        conn.execute(
            f"UPDATE {tempo_ops_tbl('granule_inventory')} SET processed_at = ? WHERE granule_id = 'G-outside'",
            [datetime(2026, 6, 1)],
        )
        with pytest.raises(ValueError, match="outside raw directory"):
            prune_processed_granule_files(
                retention_days=30,
                raw_dir=raw_dir,
                now=datetime(2026, 7, 12),
                conn=conn,
            )
        with pytest.raises(ValueError, match="at least 1"):
            prune_processed_granule_files(
                retention_days=0,
                raw_dir=raw_dir,
                conn=conn,
            )
    assert outside.exists()


@pytest.fixture
def geo_fixtures(tmp_path):
    from scripts.build_region_artifacts import build_artifacts

    metrics = build_artifacts(tmp_path / "geo", use_synthetic=True)
    return {
        "manifest": metrics["manifest_path"],
        "registry": metrics["registry_path"],
        "weights": metrics["weights_path"],
    }
