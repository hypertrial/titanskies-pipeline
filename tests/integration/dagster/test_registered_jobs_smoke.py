from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")


import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.orchestration import assets_tempo_no2 as assets_mod
from titanskies_pipeline.orchestration.definitions import defs
from titanskies_pipeline.orchestration.scope_registry import (
    SCOPE_STEPS,
    iter_scope_specs,
)


def _expected_public_job_names() -> set[str]:
    return {
        spec.job_for_step(step) for spec in iter_scope_specs() for step in SCOPE_STEPS
    }


@pytest.fixture
def patched_dagster_runtime(monkeypatch, tmp_path):
    connection.reset_duckdb_connection_state()
    db_path = tmp_path / "registered_jobs.duckdb"
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
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    @contextmanager
    def mock_connection():
        yield MagicMock()

    def stream_dbt_build(**_kwargs):
        if False:
            yield None

    monkeypatch.setattr(connection, "get_connection", mock_connection)
    monkeypatch.setattr(connection, "get_persistent_connection", lambda: MagicMock())
    monkeypatch.setattr(connection, "init_duck_db", lambda: None)
    monkeypatch.setattr(connection, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(assets_mod, "stream_dbt_build", stream_dbt_build)
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
    yield


@pytest.mark.parametrize(
    "job_name",
    sorted(_expected_public_job_names()),
)
def test_registered_jobs_smoke(job_name, patched_dagster_runtime):
    del patched_dagster_runtime
    job = next(job for job in defs.resolve_all_job_defs() if job.name == job_name)
    result = job.execute_in_process()
    assert result.success


def test_registered_job_inventory():
    assert sorted(
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    ) == sorted(_expected_public_job_names())
