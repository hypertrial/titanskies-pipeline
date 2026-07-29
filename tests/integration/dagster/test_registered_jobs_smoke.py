from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")


import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.orchestration import (
    assets_plumegraph_events as plumegraph_assets,
)
from titanskies_pipeline.orchestration import (
    assets_reproductions as reproduction_assets,
)
from titanskies_pipeline.orchestration import (
    assets_riverpulse_events as riverpulse_assets,
)
from titanskies_pipeline.orchestration import assets_tempo_no2 as assets_mod
from titanskies_pipeline.orchestration.definitions import defs
from titanskies_pipeline.orchestration.scope_registry import (
    SCOPE_STEPS,
    iter_scope_specs,
)


def _expected_public_job_names() -> set[str]:
    return {
        spec.job_for_step(step) for spec in iter_scope_specs() for step in SCOPE_STEPS
    } | {
        "riverpulse_events_source_discovery",
        "riverpulse_events_observation_ingest",
        "riverpulse_events_dbt_build",
        "riverpulse_events_full_pipeline",
        "plumegraph_events_source_discovery",
        "plumegraph_events_source_ingest",
        "plumegraph_events_analysis",
        "plumegraph_events_dbt_build",
        "plumegraph_events_validation",
        "plumegraph_events_release_build",
        "plumegraph_events_full_pipeline",
        "sun2025_repro_source_preflight",
        "andreadis2025_repro_source_preflight",
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
    from titanskies_pipeline.riverpulse.collection import (
        DiscoveryMetrics as RiverPulseDiscoveryMetrics,
    )
    from titanskies_pipeline.riverpulse.collection import IngestMetrics

    monkeypatch.setattr(
        riverpulse_assets, "_require_registered_network", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        riverpulse_assets,
        "plan_source_requests",
        lambda **_kwargs: RiverPulseDiscoveryMetrics(1, 1, 1, 0),
    )
    monkeypatch.setattr(
        riverpulse_assets,
        "sync_pending_requests",
        lambda **_kwargs: IngestMetrics(1, 0, 0, 1, 1, 14, 1),
    )
    from titanskies_pipeline.plumegraph.analysis import AnalysisMetrics
    from titanskies_pipeline.plumegraph.connectors import ConnectorMetrics
    from titanskies_pipeline.plumegraph.release import ReleaseMetrics
    from titanskies_pipeline.plumegraph.sources import (
        DiscoveryMetrics as PlumeGraphDiscoveryMetrics,
    )
    from titanskies_pipeline.plumegraph.validation import ValidationMetrics

    monkeypatch.setattr(
        plumegraph_assets,
        "persist_cohort",
        lambda *_args, **_kwargs: {
            "cohort_version": "synthetic-v1",
            "facilities": 75,
            "cohort_facilities": 75,
            "analysis_regions": 1,
        },
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "plan_source_requests",
        lambda **_kwargs: PlumeGraphDiscoveryMetrics(1, 3, 0),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "sync_source_connector",
        lambda connector, **_kwargs: ConnectorMetrics(connector, 1, 0, 1, 1),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "run_pending_analysis",
        lambda **_kwargs: AnalysisMetrics(1, 0, 1, ("run",)),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "run_validation",
        lambda *_args, **_kwargs: ValidationMetrics(
            "validation",
            200,
            1.0,
            1.0,
            1.0,
            0.01,
            1.0,
            True,
            True,
        ),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "build_release",
        lambda **_kwargs: ReleaseMetrics(
            "release",
            tmp_path / "release",
            "a" * 64,
            1,
            12,
        ),
    )
    from titanskies_pipeline.reproductions.preflight import PreflightMetrics

    monkeypatch.setattr(
        reproduction_assets,
        "run_preflight",
        lambda profile_id, **kwargs: PreflightMetrics(
            f"{profile_id}-run",
            profile_id,
            "ready",
            "synthetic",
            kwargs["exact_mode"],
            1,
            1,
            1,
            10,
            0,
            (),
            "a" * 64,
            "b" * 64,
            "c" * 64,
        ),
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
