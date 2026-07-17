from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from titanskies_pipeline.orchestration.config import (
    DbtBuildConfig,
    GranuleDiscoveryConfig,
    GuardrailConfig,
    HourlyIngestConfig,
    RegionRegistryConfig,
    tempo_no2_dbt_build_run_config,
    tempo_no2_full_pipeline_run_config,
    tempo_no2_granule_discovery_run_config,
    tempo_no2_hourly_ingest_run_config,
    tempo_no2_region_registry_run_config,
)


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


def test_granule_discovery_config_requires_positive_lookback():
    cfg = GranuleDiscoveryConfig()
    assert cfg.lookback_hours == 8
    with pytest.raises(Exception):
        GranuleDiscoveryConfig(lookback_hours=0)


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
