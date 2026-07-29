from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

from titanskies_pipeline.ingestion.tempo.sync import DiscoveryMetrics, SyncMetrics
from titanskies_pipeline.orchestration import (
    assets_riverpulse_events as riverpulse_assets,
)
from titanskies_pipeline.orchestration import assets_tempo_no2 as assets_mod
from titanskies_pipeline.orchestration import config as orch_config
from titanskies_pipeline.orchestration.assets_riverpulse_events import (
    riverpulse_events_ops_network_registry,
    riverpulse_events_raw_observations,
    riverpulse_events_raw_source_inventory,
)
from titanskies_pipeline.orchestration.assets_tempo_no2 import (
    tempo_no2_ops_region_registry,
    tempo_no2_raw_granule_inventory,
    tempo_no2_raw_region_hour_aggregates,
    tempo_no2_std_raw_granule_inventory,
    titanskies_dbt,
)
from titanskies_pipeline.riverpulse.collection import (
    DiscoveryMetrics as RiverPulseDiscoveryMetrics,
)
from titanskies_pipeline.riverpulse.collection import IngestMetrics


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
    assert result.metadata == {
        "found": 3,
        "inserted": 2,
        "refreshed": 1,
        "requeued": 0,
    }


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
    assert result.metadata == {
        "found": 1,
        "inserted": 1,
        "refreshed": 0,
        "requeued": 0,
    }
    from datetime import datetime

    assert captured["window_start"] == datetime(2026, 7, 1, 0, 0, 0)
    assert captured["window_end"] == datetime(2026, 7, 2, 0, 0, 0)
    assert captured["lookback_hours"] is None


def test_granule_inventory_asset_parses_zulu_window(monkeypatch):
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
    tempo_no2_raw_granule_inventory.op.compute_fn.decorated_fn(
        ctx,
        orch_config.GranuleDiscoveryConfig(
            window_start_utc="2026-07-01T00:00:00Z",
            window_end_utc="2026-07-02T00:00:00Z",
            allow_synthetic=True,
        ),
    )
    from datetime import datetime

    assert captured["window_start"] == datetime(2026, 7, 1, 0, 0, 0)
    assert captured["window_end"] == datetime(2026, 7, 2, 0, 0, 0)


def test_std_granule_inventory_asset_defers_lookback_to_scope(monkeypatch):
    monkeypatch.setattr(
        assets_mod.ops, "require_registered_geography", lambda **_kwargs: None
    )
    captured = {}
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_granule_discovery",
        lambda **kwargs: captured.update(kwargs) or DiscoveryMetrics(0, 0, 0),
    )
    ctx = MagicMock()
    tempo_no2_std_raw_granule_inventory.op.compute_fn.decorated_fn(
        ctx, orch_config.GranuleDiscoveryConfig(allow_synthetic=True)
    )
    assert captured["scope"] == "no2_std"
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


def test_riverpulse_network_asset(monkeypatch, tmp_path):
    artifacts = MagicMock(
        network_version="17b",
        artifact_mode="synthetic",
        build_id="build",
    )
    monkeypatch.setattr(
        riverpulse_assets, "load_network_artifacts", lambda *_a, **_k: artifacts
    )
    monkeypatch.setattr(
        riverpulse_assets,
        "persist_network_artifacts",
        lambda _artifacts: {"reaches_loaded": 9, "edges_loaded": 15},
    )
    result = riverpulse_events_ops_network_registry.op.compute_fn.decorated_fn(
        MagicMock(),
        orch_config.RiverPulseNetworkConfig(
            manifest_path=str(tmp_path / "network.json"),
            allow_synthetic=True,
        ),
    )
    assert result.metadata["network_version"] == "17b"
    assert result.metadata["reaches_loaded"] == 9


@pytest.mark.parametrize(
    ("registered", "allow_synthetic", "error"),
    [
        (None, False, "registry is empty"),
        (("synthetic",), False, "cannot use a synthetic"),
        (("synthetic",), True, None),
        (("production",), False, None),
    ],
)
def test_riverpulse_registered_network_guard(
    monkeypatch, registered, allow_synthetic, error
):
    manager = MagicMock()
    connection = manager.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = registered
    monkeypatch.setattr(riverpulse_assets, "get_connection", lambda: manager)

    if error:
        with pytest.raises(RuntimeError, match=error):
            riverpulse_assets._require_registered_network(
                allow_synthetic=allow_synthetic
            )
    else:
        riverpulse_assets._require_registered_network(allow_synthetic=allow_synthetic)


def test_riverpulse_discovery_and_ingest_assets(monkeypatch, tmp_path):
    monkeypatch.setattr(
        riverpulse_assets, "_require_registered_network", lambda **_kwargs: None
    )
    captured = {}
    monkeypatch.setattr(
        riverpulse_assets,
        "plan_source_requests",
        lambda **kwargs: (
            captured.update(kwargs) or RiverPulseDiscoveryMetrics(1, 1, 1, 0)
        ),
    )
    discovery = riverpulse_events_raw_source_inventory.op.compute_fn.decorated_fn(
        MagicMock(),
        orch_config.RiverPulseDiscoveryConfig(
            window_start_utc="2024-01-01T00:00:00Z",
            window_end_utc="2025-01-01T00:00:00Z",
            reach_ids=["RP1001"],
            allow_synthetic=True,
        ),
    )
    assert discovery.metadata["requests_planned"] == 1
    assert captured["reach_ids"] == ["RP1001"]

    monkeypatch.setattr(
        riverpulse_assets,
        "sync_pending_requests",
        lambda **kwargs: captured.update(kwargs) or IngestMetrics(1, 0, 0, 3, 3, 42, 3),
    )
    ingest = riverpulse_events_raw_observations.op.compute_fn.decorated_fn(
        MagicMock(),
        orch_config.RiverPulseIngestConfig(
            max_requests=1, raw_data_dir=str(tmp_path / "raw")
        ),
    )
    assert ingest.metadata["observation_revisions_inserted"] == 3
    assert captured["max_requests"] == 1
