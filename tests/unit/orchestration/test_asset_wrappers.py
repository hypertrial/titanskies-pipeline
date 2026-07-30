from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

from titanskies_pipeline.ingestion.tempo.sync import DiscoveryMetrics, SyncMetrics
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
from titanskies_pipeline.plumegraph.analysis import AnalysisMetrics
from titanskies_pipeline.plumegraph.connectors import ConnectorMetrics
from titanskies_pipeline.plumegraph.release import ReleaseMetrics
from titanskies_pipeline.plumegraph.sources import (
    DiscoveryMetrics as PlumeGraphDiscoveryMetrics,
)
from titanskies_pipeline.plumegraph.validation import ValidationMetrics
from titanskies_pipeline.reproductions.preflight import PreflightMetrics
from titanskies_pipeline.reproductions.readiness import ResolutionMetrics
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


def test_reproduction_preflight_assets(monkeypatch, tmp_path):
    captured = []

    def fake_preflight(profile_id, **kwargs):
        captured.append((profile_id, kwargs))
        return PreflightMetrics(
            preflight_run_id=f"{profile_id}-run",
            profile_id=profile_id,
            status="ready",
            inventory_mode="synthetic",
            exact_mode=kwargs["exact_mode"],
            source_count=1,
            required_source_count=1,
            object_count=1,
            total_bytes=10,
            planned_max_bytes=10,
            unknown_size_count=0,
            unbounded_size_count=0,
            blocking_sources=(),
            manifest_sha256="a" * 64,
            scientific_contract_sha256="b" * 64,
            inventory_sha256="c" * 64,
        )

    monkeypatch.setattr(reproduction_assets, "run_preflight", fake_preflight)
    config = orch_config.ReproductionPreflightConfig(
        manifest_path=str(tmp_path / "manifest.json"),
        inventory_path=str(tmp_path / "inventory.json"),
        max_objects=5,
        max_bytes=20,
    )
    result = reproduction_assets.sun2025_repro_source_preflight_asset.op.compute_fn.decorated_fn(
        MagicMock(), config
    )
    assert result.metadata["profile_id"] == "sun2025"
    assert captured[0][1]["manifest_path"].is_absolute()
    assert captured[0][1]["inventory_path"].is_absolute()

    result = reproduction_assets.andreadis2025_repro_source_preflight_asset.op.compute_fn.decorated_fn(
        MagicMock(), orch_config.ReproductionPreflightConfig()
    )
    assert result.metadata["profile_id"] == "andreadis2025"
    assert captured[1][1]["manifest_path"] is None
    assert captured[1][1]["inventory_path"] is None


def test_reproduction_inventory_assets(monkeypatch, tmp_path):
    captured = []

    def fake_resolver(profile_id, **kwargs):
        captured.append((profile_id, kwargs))
        return ResolutionMetrics(
            profile_id=profile_id,
            status="complete",
            inventory_path=str(kwargs["output_path"]),
            inventory_sha256="a" * 64,
            resolution_bundle_sha256="b" * 64,
            source_count=1,
            object_count=1,
            resolved_source_count=1,
            operator_input_required_count=0,
            transient_error_count=0,
            definitively_unavailable_count=0,
        )

    monkeypatch.setattr(
        reproduction_assets, "resolve_reproduction_sources", fake_resolver
    )
    config = orch_config.ReproductionDiscoveryConfig(
        manifest_path=str(tmp_path / "manifest.json"),
        evidence_path=str(tmp_path / "evidence.json"),
        import_directory=str(tmp_path / "imports"),
        output_inventory_path=str(tmp_path / "inventory.json"),
        timeout_seconds=12,
    )
    result = reproduction_assets.sun2025_repro_source_inventory_asset.op.compute_fn.decorated_fn(
        MagicMock(), config
    )
    assert result.metadata["profile_id"] == "sun2025"
    assert captured[0][1]["timeout_seconds"] == 12
    assert captured[0][1]["evidence_path"].is_absolute()

    result = reproduction_assets.andreadis2025_repro_source_inventory_asset.op.compute_fn.decorated_fn(
        MagicMock(), orch_config.ReproductionDiscoveryConfig()
    )
    assert result.metadata["profile_id"] == "andreadis2025"
    assert captured[1][1]["manifest_path"] is None
    assert ".cache/reproduction_readiness/andreadis2025" in str(
        captured[1][1]["output_path"]
    )


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


def test_plumegraph_asset_wrappers(monkeypatch, tmp_path):
    context = MagicMock()
    captured = {}
    monkeypatch.setattr(
        plumegraph_assets,
        "get_plumegraph_settings",
        lambda: MagicMock(cohort_manifest_path=tmp_path / "default-cohort.json"),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "persist_cohort",
        lambda path, **kwargs: (
            captured.update(path=path, **kwargs)
            or {
                "cohort_version": "v1",
                "facilities": 75,
                "cohort_facilities": 75,
                "analysis_regions": 1,
            }
        ),
    )
    registry = plumegraph_assets.plumegraph_events_ops_facility_registry.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphFacilityRegistryConfig(allow_synthetic=True),
    )
    assert registry.metadata["facilities"] == 75
    assert captured["path"] == tmp_path / "default-cohort.json"
    explicit = tmp_path / "explicit.json"
    plumegraph_assets.plumegraph_events_ops_facility_registry.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphFacilityRegistryConfig(
            manifest_path=str(explicit),
        ),
    )
    assert captured["path"] == explicit
    assert captured["require_approved"] is True

    monkeypatch.setattr(
        plumegraph_assets,
        "plan_source_requests",
        lambda **kwargs: captured.update(kwargs) or PlumeGraphDiscoveryMetrics(1, 3, 0),
    )
    discovery = plumegraph_assets.plumegraph_events_raw_source_inventory.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphDiscoveryConfig(
            window_start_utc="2024-01-01T00:00:00Z",
            window_end_utc="2024-01-02T00:00:00Z",
            backfill=True,
        ),
    )
    assert discovery.metadata["requests_planned"] == 3
    assert captured["backfill"]

    monkeypatch.setattr(
        plumegraph_assets,
        "sync_source_connector",
        lambda connector, **kwargs: (
            captured.update(connector=connector, **kwargs)
            or ConnectorMetrics(connector, 1, 0, 1, 2)
        ),
    )
    ingest = plumegraph_assets.plumegraph_events_raw_tempo_snapshots.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphIngestConfig(
            max_requests=1,
            raw_data_dir=str(tmp_path / "raw"),
        ),
    )
    assert ingest.metadata["rows_inserted"] == 2
    assert captured["connector"] == "harmony"
    assert captured["raw_data_dir"] == tmp_path / "raw"

    monkeypatch.setattr(
        plumegraph_assets,
        "run_pending_analysis",
        lambda **kwargs: captured.update(kwargs) or AnalysisMetrics(1, 0, 1, ("run",)),
    )
    analyzed = (
        plumegraph_assets.plumegraph_events_analysis_results.op.compute_fn.decorated_fn(
            context,
            orch_config.PlumeGraphAnalysisConfig(partition_dates=["2024-01-01"]),
        )
    )
    assert analyzed.metadata["generation_ids"] == ["run"]

    monkeypatch.setattr(
        plumegraph_assets,
        "load_benchmark",
        lambda path, **kwargs: captured.update(benchmark_path=path, **kwargs),
    )
    monkeypatch.setattr(
        plumegraph_assets,
        "run_validation",
        lambda *_args, **_kwargs: ValidationMetrics(
            "validation",
            200,
            1,
            1,
            1,
            0,
            1,
            True,
            True,
        ),
    )
    validated = (
        plumegraph_assets.plumegraph_events_validation.op.compute_fn.decorated_fn(
            context,
            orch_config.PlumeGraphValidationConfig(
                benchmark_path=str(tmp_path / "benchmark.json"),
                allow_incomplete=True,
            ),
        )
    )
    assert validated.metadata["passed"]
    assert captured["benchmark_path"] == tmp_path / "benchmark.json"
    captured.pop("benchmark_path")
    plumegraph_assets.plumegraph_events_validation.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphValidationConfig(allow_incomplete=True),
    )
    assert "benchmark_path" not in captured

    monkeypatch.setattr(
        plumegraph_assets,
        "build_release",
        lambda **kwargs: (
            captured.update(kwargs)
            or ReleaseMetrics("release", tmp_path / "release", "a" * 64, 1, 2)
        ),
    )
    released = plumegraph_assets.plumegraph_events_release.op.compute_fn.decorated_fn(
        context,
        orch_config.PlumeGraphReleaseConfig(
            release_version="v1",
            output_dir=str(tmp_path / "releases"),
        ),
    )
    assert released.metadata["release_id"] == "release"
    assert released.metadata["release_path"] == str(tmp_path / "release")
