from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

from titanskies_pipeline.ingestion.tempo.sync import DiscoveryMetrics, SyncMetrics
from titanskies_pipeline.orchestration import assets_tempo_no2 as assets_mod
from titanskies_pipeline.orchestration import config as orch_config
from titanskies_pipeline.orchestration.assets_tempo_no2 import (
    tempo_no2_ops_region_registry,
    tempo_no2_raw_granule_inventory,
    tempo_no2_raw_region_hour_aggregates,
    titanskies_dbt,
)


def test_region_registry_asset(monkeypatch):
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_region_registry",
        lambda **_kwargs: {"regions_loaded": 2, "weights_loaded": 4},
    )
    ctx = MagicMock()
    result = tempo_no2_ops_region_registry.op.compute_fn.decorated_fn(
        ctx,
        orch_config.RegionRegistryConfig(
            manifest_path="artifacts/geo/tempo_geography_artifacts.json",
            allow_synthetic=True,
        ),
    )
    assert result.metadata["regions_loaded"] == 2


def test_granule_inventory_asset(monkeypatch):
    monkeypatch.setattr(
        assets_mod.ops, "require_registered_geography", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_granule_discovery",
        lambda **_kwargs: DiscoveryMetrics(3, 2, 1),
    )
    ctx = MagicMock()
    result = tempo_no2_raw_granule_inventory.op.compute_fn.decorated_fn(
        ctx, orch_config.GranuleDiscoveryConfig(lookback_hours=4, allow_synthetic=True)
    )
    assert result.metadata == {"found": 3, "inserted": 2, "refreshed": 1}


def test_granule_inventory_asset_uses_explicit_window(monkeypatch):
    monkeypatch.setattr(
        assets_mod.ops, "require_registered_geography", lambda **_kwargs: None
    )
    captured = {}
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_granule_discovery",
        lambda **kwargs: captured.update(kwargs) or DiscoveryMetrics(1, 1, 0),
    )
    ctx = MagicMock()
    result = tempo_no2_raw_granule_inventory.op.compute_fn.decorated_fn(
        ctx,
        orch_config.GranuleDiscoveryConfig(
            window_start_utc="2026-07-01T00:00:00",
            window_end_utc="2026-07-02T00:00:00",
            allow_synthetic=True,
        ),
    )
    assert result.metadata == {"found": 1, "inserted": 1, "refreshed": 0}
    from datetime import datetime

    assert captured["window_start"] == datetime(2026, 7, 1, 0, 0, 0)
    assert captured["window_end"] == datetime(2026, 7, 2, 0, 0, 0)
    assert captured["lookback_hours"] is None


def test_hourly_ingest_asset(monkeypatch):
    calls = []
    monkeypatch.setattr(
        assets_mod.ops,
        "process_pending_granules",
        lambda **kwargs: calls.append(kwargs) or SyncMetrics(1, 1, 5),
    )
    ctx = MagicMock()
    result = tempo_no2_raw_region_hour_aggregates.op.compute_fn.decorated_fn(
        ctx, orch_config.HourlyIngestConfig(max_granules=2, allow_synthetic=True)
    )
    assert result.metadata["aggregates_written"] == 5
    assert calls == [{"scope": "no2", "max_granules": 2, "allow_synthetic": True}]


def test_titanskies_dbt_asset_streams(monkeypatch):
    monkeypatch.setattr(
        assets_mod,
        "stream_dbt_build",
        lambda **_kwargs: iter(["event"]),
    )

    events = list(
        titanskies_dbt.op.compute_fn.decorated_fn(
            MagicMock(),
            MagicMock(),
            orch_config.DbtBuildConfig(),
        )
    )
    assert events == ["event"]
