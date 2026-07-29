import importlib

import pytest

pytest.importorskip("dagster")

from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context

from titanskies_pipeline.orchestration import tempo_ops as ops
from titanskies_pipeline.orchestration.config import (
    plumegraph_events_full_pipeline_run_config,
    riverpulse_events_full_pipeline_run_config,
    tempo_no2_full_pipeline_run_config,
)
from titanskies_pipeline.orchestration.definitions import defs
from titanskies_pipeline.orchestration.schedules import (
    plumegraph_events_daily_pipeline_schedule,
    riverpulse_events_pipeline_schedule,
    tempo_no2_hourly_pipeline_schedule,
    tempo_no2_std_pipeline_schedule,
)


def _reload_schedules_module(monkeypatch, *, hourly: bool = False):
    monkeypatch.setenv(
        "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED", "true" if hourly else "false"
    )
    from titanskies_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    import titanskies_pipeline.orchestration.schedules as schedules_mod

    return importlib.reload(schedules_mod)


def test_definitions_expose_shipped_product_jobs():
    expected = {
        "tempo_no2_granule_discovery",
        "tempo_no2_hourly_ingest",
        "tempo_no2_dbt_build",
        "tempo_no2_full_pipeline",
        "tempo_no2_std_granule_discovery",
        "tempo_no2_std_hourly_ingest",
        "tempo_no2_std_dbt_build",
        "tempo_no2_std_full_pipeline",
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
    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_tempo_asset_keys():
    expected = {
        ("tempo", "no2", "ops", "region_registry"),
        ("tempo", "no2", "raw", "granule_inventory"),
        ("tempo", "no2", "raw", "region_hour_aggregates"),
        ("tempo", "no2_std", "ops", "region_registry"),
        ("tempo", "no2_std", "raw", "granule_inventory"),
        ("tempo", "no2_std", "raw", "region_hour_aggregates"),
    }
    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    known_tempo_scopes = {("tempo", "no2"), ("tempo", "no2_std")}
    assert all(key[:2] in known_tempo_scopes for key in asset_keys if key[0] == "tempo")
    assert {
        ("riverpulse", "events", "ops", "network_registry"),
        ("riverpulse", "events", "raw", "source_inventory"),
        ("riverpulse", "events", "raw", "observations"),
    } <= asset_keys
    assert {
        ("sun2025", "repro", "ops", "source_preflight"),
        ("andreadis2025", "repro", "ops", "source_preflight"),
    } <= asset_keys
    assert {
        ("plumegraph", "events", "ops", "facility_registry"),
        ("plumegraph", "events", "raw", "source_inventory"),
        ("plumegraph", "events", "raw", "tempo_snapshots"),
        ("plumegraph", "events", "raw", "hrrr_snapshots"),
        ("plumegraph", "events", "raw", "camd_emissions"),
        ("plumegraph", "events", "intermediate", "analysis_results"),
        ("plumegraph", "events", "observability", "validation"),
        ("plumegraph", "events", "releases", "evidence_ledger"),
    } <= asset_keys


def test_hourly_schedule_targets_full_pipeline_and_config():
    assert tempo_no2_hourly_pipeline_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert tempo_no2_hourly_pipeline_schedule.job_name == "tempo_no2_full_pipeline"

    context = build_schedule_context()
    run_config = (
        tempo_no2_hourly_pipeline_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
    )
    assert run_config == tempo_no2_full_pipeline_run_config()
    cfg = run_config["ops"]["tempo__no2__raw__granule_inventory"]["config"]
    assert cfg["lookback_hours"] == 8


def test_hourly_schedule_enabled_by_env(monkeypatch):
    schedules_mod = _reload_schedules_module(monkeypatch, hourly=True)
    assert schedules_mod.tempo_no2_hourly_pipeline_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )


def test_std_schedule_disabled_by_default():
    assert tempo_no2_std_pipeline_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert tempo_no2_std_pipeline_schedule.job_name == "tempo_no2_std_full_pipeline"


def test_std_schedule_enabled_by_env(monkeypatch):
    monkeypatch.setenv("TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED", "true")
    from titanskies_pipeline.config._reload_settings import (
        reload_all_settings_modules,
    )

    reload_all_settings_modules()
    import titanskies_pipeline.orchestration.schedules as schedules_mod

    reloaded = importlib.reload(schedules_mod)
    assert reloaded.tempo_no2_std_pipeline_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )


def test_tempo_ops_facade_exports_sync_entrypoints():
    assert set(ops.__all__) == {
        "sync_granule_discovery",
        "process_pending_granules",
        "require_registered_geography",
        "sync_region_registry",
    }
    assert callable(ops.sync_region_registry)
    assert callable(ops.sync_granule_discovery)
    assert callable(ops.process_pending_granules)
    assert callable(ops.require_registered_geography)


def test_full_pipeline_job_selects_ingest_and_dbt_assets():
    job = defs.resolve_job_def("tempo_no2_full_pipeline")
    selected = {tuple(key.path) for key in job.asset_layer.selected_asset_keys}
    assert ("tempo", "no2", "ops", "region_registry") not in selected
    assert ("tempo", "no2", "raw", "granule_inventory") in selected
    assert ("tempo", "no2", "raw", "region_hour_aggregates") in selected
    assert any(key[0] == "tempo" and len(key) >= 4 for key in selected)


def test_dbt_sources_preserve_ingestion_order_in_asset_graph():
    graph = defs.resolve_asset_graph()
    raw_regions = AssetKey(["tempo", "no2", "raw", "region_hour_aggregates"])
    staging_regions = AssetKey(["tempo", "no2", "staging", "region_hour_aggregates"])
    assert raw_regions in graph.get(staging_regions).parent_keys


def test_riverpulse_schedule_and_full_pipeline_exclude_network_bootstrap():
    assert riverpulse_events_pipeline_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert (
        riverpulse_events_pipeline_schedule.job_name
        == "riverpulse_events_full_pipeline"
    )
    run_config = (
        riverpulse_events_pipeline_schedule.evaluate_tick(build_schedule_context())
        .run_requests[0]
        .run_config
    )
    assert run_config == riverpulse_events_full_pipeline_run_config()
    job = defs.resolve_job_def("riverpulse_events_full_pipeline")
    selected = {tuple(key.path) for key in job.asset_layer.selected_asset_keys}
    assert ("riverpulse", "events", "ops", "network_registry") not in selected
    assert ("riverpulse", "events", "raw", "source_inventory") in selected
    assert ("riverpulse", "events", "raw", "observations") in selected
    assert any(key[:3] == ("riverpulse", "events", "marts") for key in selected)


def test_riverpulse_dbt_dependency_follows_observation_ingest():
    graph = defs.resolve_asset_graph()
    raw = AssetKey(["riverpulse", "events", "raw", "observations"])
    staging = AssetKey(["riverpulse", "events", "staging", "observation_revisions"])
    assert raw in graph.get(staging).parent_keys


def test_plumegraph_schedule_and_full_pipeline_exclude_bootstrap_and_release():
    assert plumegraph_events_daily_pipeline_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert (
        plumegraph_events_daily_pipeline_schedule.job_name
        == "plumegraph_events_full_pipeline"
    )
    run_config = (
        plumegraph_events_daily_pipeline_schedule.evaluate_tick(
            build_schedule_context()
        )
        .run_requests[0]
        .run_config
    )
    assert run_config == plumegraph_events_full_pipeline_run_config()
    job = defs.resolve_job_def("plumegraph_events_full_pipeline")
    selected = {tuple(key.path) for key in job.asset_layer.selected_asset_keys}
    assert ("plumegraph", "events", "ops", "facility_registry") not in selected
    assert ("plumegraph", "events", "releases", "evidence_ledger") not in selected
    assert ("plumegraph", "events", "raw", "source_inventory") in selected
    assert ("plumegraph", "events", "intermediate", "analysis_results") in selected
    assert ("plumegraph", "events", "observability", "validation") in selected
    assert any(key[:3] == ("plumegraph", "events", "marts") for key in selected)


def test_plumegraph_dbt_dependency_follows_analysis():
    graph = defs.resolve_asset_graph()
    analysis = AssetKey(["plumegraph", "events", "intermediate", "analysis_results"])
    current = AssetKey(["plumegraph", "events", "intermediate", "current_episodes"])
    episodes = AssetKey(["plumegraph", "events", "marts", "episodes"])
    assert analysis in graph.get(current).parent_keys
    assert current in graph.get(episodes).parent_keys
