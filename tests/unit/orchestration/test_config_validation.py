from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from titanskies_pipeline.orchestration.config import (
    DbtBuildConfig,
    GranuleDiscoveryConfig,
    GuardrailConfig,
    HourlyIngestConfig,
    RegionRegistryConfig,
    RiverPulseDiscoveryConfig,
    RiverPulseIngestConfig,
    RiverPulseNetworkConfig,
    riverpulse_events_dbt_run_config,
    riverpulse_events_discovery_run_config,
    riverpulse_events_full_pipeline_run_config,
    riverpulse_events_ingest_run_config,
    scope_run_config,
    tempo_no2_dbt_build_run_config,
    tempo_no2_full_pipeline_run_config,
    tempo_no2_granule_discovery_run_config,
    tempo_no2_hourly_ingest_run_config,
    tempo_no2_region_registry_run_config,
    tempo_no2_std_dbt_build_run_config,
    tempo_no2_std_full_pipeline_run_config,
    tempo_no2_std_granule_discovery_run_config,
    tempo_no2_std_hourly_ingest_run_config,
    tempo_no2_std_region_registry_run_config,
)
from titanskies_pipeline.orchestration.scope_registry import TEMPO_NO2_SCOPE


def test_guardrail_config_rejects_invalid_timeout_order():
    with pytest.raises(ValueError, match="hard_timeout"):
        GuardrailConfig(
            no_progress_soft_timeout_seconds=10,
            no_progress_hard_timeout_seconds=5,
        )


def test_guardrail_config_defaults():
    cfg = GuardrailConfig()
    assert cfg.progress_log_interval_seconds == 60
    assert cfg.progress_poll_seconds == 5


def test_region_registry_config_accepts_manifest():
    cfg = RegionRegistryConfig(manifest_path="/tmp/manifest.json", allow_synthetic=True)
    assert cfg.manifest_path == "/tmp/manifest.json"
    assert cfg.allow_synthetic is True


def test_granule_discovery_config_defaults_lookback_to_scope_runtime():
    cfg = GranuleDiscoveryConfig()
    assert cfg.lookback_hours is None
    with pytest.raises(Exception):
        GranuleDiscoveryConfig(lookback_hours=0)


def test_granule_discovery_config_accepts_explicit_window():
    cfg = GranuleDiscoveryConfig(
        window_start_utc="2026-07-01T00:00:00",
        window_end_utc="2026-07-02T00:00:00",
    )
    assert cfg.window_start_utc == "2026-07-01T00:00:00"


def test_granule_discovery_config_rejects_inverted_window():
    with pytest.raises(Exception, match="strictly before"):
        GranuleDiscoveryConfig(
            window_start_utc="2026-07-02T00:00:00",
            window_end_utc="2026-07-01T00:00:00",
        )


def test_granule_discovery_config_rejects_equal_and_mixed_z_windows():
    with pytest.raises(Exception, match="strictly before"):
        GranuleDiscoveryConfig(
            window_start_utc="2026-06-01T00:00:00Z",
            window_end_utc="2026-06-01T00:00:00Z",
        )
    with pytest.raises(Exception, match="strictly before"):
        GranuleDiscoveryConfig(
            window_start_utc="2026-06-01T00:00:00",
            window_end_utc="2026-06-01T00:00:00Z",
        )
    cfg = GranuleDiscoveryConfig(
        window_start_utc="2026-06-01T00:00:00",
        window_end_utc="2026-06-01T01:00:00Z",
    )
    assert cfg.window_end_utc == "2026-06-01T01:00:00Z"
    with pytest.raises(Exception, match="ISO-8601"):
        GranuleDiscoveryConfig(
            window_start_utc="not-a-timestamp",
            window_end_utc="2026-06-01T01:00:00Z",
        )


def test_granule_discovery_config_rejects_partial_window():
    with pytest.raises(Exception, match="must both be set together"):
        GranuleDiscoveryConfig(window_start_utc="2026-07-01T00:00:00")
    with pytest.raises(Exception, match="must both be set together"):
        GranuleDiscoveryConfig(window_end_utc="2026-07-01T00:00:00")


def test_hourly_ingest_config_is_processing_only():
    cfg = HourlyIngestConfig()
    assert cfg.max_granules is None


def test_dbt_build_config_accepts_scope_selectors():
    cfg = DbtBuildConfig(
        full_refresh=True,
        dbt_select="+tag:tempo,tag:no2",
        dbt_exclude="tag:other",
        fetch_dbt_metadata=False,
    )
    assert cfg.full_refresh is True
    assert cfg.dbt_select == "+tag:tempo,tag:no2"
    assert cfg.dbt_exclude == "tag:other"
    assert cfg.fetch_dbt_metadata is False


def test_tempo_no2_region_registry_run_config():
    cfg = tempo_no2_region_registry_run_config()
    assert "tempo__no2__ops__region_registry" in cfg["ops"]


def test_tempo_no2_granule_discovery_run_config():
    cfg = tempo_no2_granule_discovery_run_config()
    assert "tempo__no2__ops__region_registry" not in cfg["ops"]
    assert "tempo__no2__raw__granule_inventory" in cfg["ops"]


def test_tempo_no2_hourly_ingest_run_config():
    cfg = tempo_no2_hourly_ingest_run_config()
    assert "tempo__no2__raw__granule_inventory" not in cfg["ops"]
    assert "tempo__no2__raw__region_hour_aggregates" in cfg["ops"]


def test_tempo_no2_dbt_build_run_config():
    cfg = tempo_no2_dbt_build_run_config()
    assert "titanskies_dbt" in cfg["ops"]
    dbt_cfg = cfg["ops"]["titanskies_dbt"]["config"]
    assert dbt_cfg["dbt_select"] == "+tag:tempo,tag:no2"


def test_tempo_no2_full_pipeline_run_config_merges_ops():
    cfg = tempo_no2_full_pipeline_run_config()
    ops = cfg["ops"]
    assert "tempo__no2__ops__region_registry" not in ops
    assert "tempo__no2__raw__granule_inventory" in ops
    assert "tempo__no2__raw__region_hour_aggregates" in ops
    assert "titanskies_dbt" in ops


def test_scope_run_config_full_and_rejects_unknown_step():
    assert scope_run_config(TEMPO_NO2_SCOPE, "full") == (
        tempo_no2_full_pipeline_run_config()
    )
    with pytest.raises(ValueError, match="Unsupported scope step"):
        scope_run_config(TEMPO_NO2_SCOPE, "backfill")  # type: ignore[arg-type]


def test_tempo_no2_std_region_registry_run_config():
    cfg = tempo_no2_std_region_registry_run_config()
    assert "tempo__no2_std__ops__region_registry" in cfg["ops"]


def test_tempo_no2_std_granule_discovery_run_config_uses_wider_lookback():
    cfg = tempo_no2_std_granule_discovery_run_config()
    op_cfg = cfg["ops"]["tempo__no2_std__raw__granule_inventory"]["config"]
    assert op_cfg["lookback_hours"] == 24


def test_tempo_no2_std_hourly_ingest_run_config():
    cfg = tempo_no2_std_hourly_ingest_run_config()
    assert "tempo__no2_std__raw__region_hour_aggregates" in cfg["ops"]


def test_tempo_no2_std_dbt_build_run_config():
    cfg = tempo_no2_std_dbt_build_run_config()
    dbt_cfg = cfg["ops"]["titanskies_dbt"]["config"]
    assert dbt_cfg["dbt_select"] == "+tag:tempo,tag:no2_std"


def test_tempo_no2_std_full_pipeline_run_config_merges_ops():
    cfg = tempo_no2_std_full_pipeline_run_config()
    ops = cfg["ops"]
    assert "tempo__no2_std__ops__region_registry" not in ops
    assert "tempo__no2_std__raw__granule_inventory" in ops
    assert "tempo__no2_std__raw__region_hour_aggregates" in ops
    assert "titanskies_dbt" in ops


def test_riverpulse_configs_and_run_configs():
    network = RiverPulseNetworkConfig(
        manifest_path="/tmp/network.json", allow_synthetic=True
    )
    assert network.allow_synthetic is True
    discovery = RiverPulseDiscoveryConfig(
        window_start_utc="2024-01-01T00:00:00Z",
        window_end_utc="2025-01-01T00:00:00Z",
        reach_ids=["1"],
        backfill=True,
    )
    assert discovery.reach_ids == ["1"]
    assert RiverPulseIngestConfig(max_requests=1).max_requests == 1
    with pytest.raises(Exception, match="must both be set"):
        RiverPulseDiscoveryConfig(window_start_utc="2024-01-01T00:00:00Z")
    with pytest.raises(Exception, match="strictly before"):
        RiverPulseDiscoveryConfig(
            window_start_utc="2025-01-01T00:00:00Z",
            window_end_utc="2024-01-01T00:00:00Z",
        )

    assert (
        "riverpulse__events__raw__source_inventory"
        in riverpulse_events_discovery_run_config()["ops"]
    )
    assert (
        "riverpulse__events__raw__observations"
        in riverpulse_events_ingest_run_config()["ops"]
    )
    dbt = riverpulse_events_dbt_run_config()["ops"]["titanskies_dbt"]["config"]
    assert dbt["dbt_select"] == "tag:riverpulse,tag:events"
    full = riverpulse_events_full_pipeline_run_config()["ops"]
    assert set(full) == {
        "riverpulse__events__raw__source_inventory",
        "riverpulse__events__raw__observations",
        "titanskies_dbt",
    }
