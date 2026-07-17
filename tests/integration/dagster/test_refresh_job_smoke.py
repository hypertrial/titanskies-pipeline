from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.orchestration import assets_tempo_no2 as assets_mod
from titanskies_pipeline.orchestration.definitions import defs


@pytest.fixture
def patched_refresh_runtime(monkeypatch, tmp_path):
    connection.reset_duckdb_connection_state()
    db_path = tmp_path / "refresh.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        f"""
titanskies:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    @contextmanager
    def mock_connection():
        yield MagicMock()

    monkeypatch.setattr(connection, "get_connection", mock_connection)
    monkeypatch.setattr(connection, "init_duck_db", lambda: None)
    monkeypatch.setattr(connection, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_region_registry",
        lambda **_kwargs: {"regions_loaded": 1, "weights_loaded": 1},
    )
    monkeypatch.setattr(
        assets_mod.ops, "require_registered_geography", lambda **_kwargs: None
    )
    from titanskies_pipeline.ingestion.tempo.sync import DiscoveryMetrics, SyncMetrics

    monkeypatch.setattr(
        assets_mod.ops,
        "sync_granule_discovery",
        lambda **_kwargs: DiscoveryMetrics(1, 1, 0),
    )

    monkeypatch.setattr(
        assets_mod.ops,
        "process_pending_granules",
        lambda **_kwargs: SyncMetrics(0, 0, 0, 0),
    )

    def stream_dbt_build(**_kwargs):
        if False:
            yield None

    monkeypatch.setattr(assets_mod, "stream_dbt_build", stream_dbt_build)
    yield


@pytest.mark.parametrize(
    "job_name", ["tempo_no2_hourly_ingest", "tempo_no2_full_pipeline"]
)
def test_refresh_job_smoke(job_name, patched_refresh_runtime):
    del patched_refresh_runtime
    job = next(job for job in defs.resolve_all_job_defs() if job.name == job_name)
    result = job.execute_in_process()
    assert result.success
