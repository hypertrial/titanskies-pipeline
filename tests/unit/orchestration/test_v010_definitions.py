import importlib

import pytest

pytest.importorskip("dagster")

from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context

from titanskies_pipeline.orchestration import tempo_ops as ops
from titanskies_pipeline.orchestration.config import tempo_no2_full_pipeline_run_config
from titanskies_pipeline.orchestration.definitions import defs
from titanskies_pipeline.orchestration.schedules import (
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


def test_definitions_expose_tempo_jobs_only():
    expected = {
        "tempo_no2_granule_discovery",
        "tempo_no2_hourly_ingest",
        "tempo_no2_dbt_build",
        "tempo_no2_full_pipeline",
        "tempo_no2_std_granule_discovery",
        "tempo_no2_std_hourly_ingest",
        "tempo_no2_std_dbt_build",
        "tempo_no2_std_full_pipeline",
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
