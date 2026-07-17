"""Integration smoke for tempo sync orchestration."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from scripts.build_region_artifacts import build_artifacts

from titanskies_pipeline.geography.registry import (
    load_geo_artifacts,
    persist_geo_artifacts,
)
from titanskies_pipeline.ingestion.tempo.cmr import DiscoveredGranule
from titanskies_pipeline.ingestion.tempo.sync import process_pending_granules
from titanskies_pipeline.storage.duckdb.connection import get_connection
from titanskies_pipeline.storage.duckdb.granules import upsert_discovered_granules


@pytest.fixture
def geo_artifacts(tmp_path):
    metrics = build_artifacts(tmp_path / "geo", use_synthetic=True)
    return load_geo_artifacts(metrics["manifest_path"], allow_synthetic=True)


@pytest.fixture
def netcdf_fixture():
    path = (
        Path(__file__).resolve().parents[2]
        / "fixtures"
        / "netcdf"
        / "tempo_no2_sample.nc"
    )
    if not path.exists():
        from scripts.generate_netcdf_fixtures import write_fixture

        write_fixture(path)
    return path


def test_process_pending_granules_integration(
    monkeypatch, duck, geo_artifacts, netcdf_fixture, tmp_path
):
    with get_connection() as conn:
        persist_geo_artifacts(geo_artifacts, conn=conn)

    granule = DiscoveredGranule(
        granule_id="G-integration",
        concept_id="TEST",
        acquisition_start=datetime(2026, 7, 12, 12),
        acquisition_end=datetime(2026, 7, 12, 13),
        download_url=None,
        cmr_revision_at=None,
    )
    with get_connection() as conn:
        upsert_discovered_granules([granule], conn=conn)
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.sync.TEMPO_NO2_RAW_DATA_DIR",
        raw_dir,
    )

    def download_fn(_granule_id: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(netcdf_fixture.read_bytes())
        return destination

    metrics = process_pending_granules(download_fn=download_fn, allow_synthetic=True)
    assert metrics.processed == 1
    assert metrics.aggregates_written >= 1

    with get_connection() as conn:
        written = upsert_discovered_granules([], conn=conn)
    assert (written.found, written.inserted, written.refreshed) == (0, 0, 0)
