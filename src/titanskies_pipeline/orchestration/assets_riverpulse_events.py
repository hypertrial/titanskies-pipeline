"""Explicit Dagster assets for the RiverPulse events lane."""

from dataclasses import asdict
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from titanskies_pipeline.config.settings_riverpulse import (
    get_riverpulse_settings,
)
from titanskies_pipeline.orchestration.config import (
    RiverPulseDiscoveryConfig,
    RiverPulseIngestConfig,
    RiverPulseNetworkConfig,
)
from titanskies_pipeline.orchestration.timestamps import parse_iso_utc
from titanskies_pipeline.riverpulse.collection import (
    plan_source_requests,
    sync_pending_requests,
)
from titanskies_pipeline.riverpulse.network import (
    load_network_artifacts,
    persist_network_artifacts,
)
from titanskies_pipeline.storage.duckdb.connection import get_connection
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    riverpulse_ops_tbl,
)

RIVERPULSE_EVENTS_OPS_NETWORK_REGISTRY = AssetKey(
    ["riverpulse", "events", "ops", "network_registry"]
)
RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY = AssetKey(
    ["riverpulse", "events", "raw", "source_inventory"]
)
RIVERPULSE_EVENTS_RAW_OBSERVATIONS = AssetKey(
    ["riverpulse", "events", "raw", "observations"]
)


@asset(
    key=RIVERPULSE_EVENTS_OPS_NETWORK_REGISTRY,
    group_name="ingestion",
)
def riverpulse_events_ops_network_registry(
    context: AssetExecutionContext,
    config: RiverPulseNetworkConfig,
) -> MaterializeResult:
    settings = get_riverpulse_settings()
    path = Path(config.manifest_path) if config.manifest_path else None
    artifacts = load_network_artifacts(
        (path or settings.network_manifest_path).resolve(),
        allow_synthetic=config.allow_synthetic,
    )
    metrics = persist_network_artifacts(artifacts)
    context.log.info("Registered RiverPulse network: %s", metrics)
    return MaterializeResult(
        metadata={
            **metrics,
            "network_version": artifacts.network_version,
            "artifact_mode": artifacts.artifact_mode,
            "build_id": artifacts.build_id,
        }
    )


def _require_registered_network(*, allow_synthetic: bool) -> None:
    with get_connection() as connection:
        registered = connection.execute(
            f"""
            SELECT artifact_mode
            FROM {riverpulse_ops_tbl("network_artifact_manifest")}
            LIMIT 1
            """
        ).fetchone()
    if not registered:
        raise RuntimeError(
            "RiverPulse network registry is empty; run the network bootstrap asset"
        )
    if str(registered[0]) != "production" and not allow_synthetic:
        raise RuntimeError(
            "Production RiverPulse discovery cannot use a synthetic network"
        )


@asset(
    key=RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY,
    deps=[RIVERPULSE_EVENTS_OPS_NETWORK_REGISTRY],
    group_name="ingestion",
)
def riverpulse_events_raw_source_inventory(
    context: AssetExecutionContext,
    config: RiverPulseDiscoveryConfig,
) -> MaterializeResult:
    _require_registered_network(allow_synthetic=config.allow_synthetic)
    start = parse_iso_utc(config.window_start_utc) if config.window_start_utc else None
    end = parse_iso_utc(config.window_end_utc) if config.window_end_utc else None
    metrics = plan_source_requests(
        window_start=start,
        window_end=end,
        backfill=config.backfill,
        reach_ids=config.reach_ids,
    )
    metadata = asdict(metrics)
    context.log.info("Planned RiverPulse Hydrocron requests: %s", metadata)
    return MaterializeResult(metadata=metadata)


@asset(
    key=RIVERPULSE_EVENTS_RAW_OBSERVATIONS,
    deps=[RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY],
    group_name="ingestion",
)
def riverpulse_events_raw_observations(
    context: AssetExecutionContext,
    config: RiverPulseIngestConfig,
) -> MaterializeResult:
    metrics = sync_pending_requests(
        max_requests=config.max_requests,
        raw_data_dir=Path(config.raw_data_dir).resolve()
        if config.raw_data_dir
        else None,
    )
    metadata = asdict(metrics)
    context.log.info("Ingested RiverPulse Hydrocron observations: %s", metadata)
    return MaterializeResult(metadata=metadata)


__all__ = [
    "RIVERPULSE_EVENTS_OPS_NETWORK_REGISTRY",
    "RIVERPULSE_EVENTS_RAW_OBSERVATIONS",
    "RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY",
    "riverpulse_events_ops_network_registry",
    "riverpulse_events_raw_observations",
    "riverpulse_events_raw_source_inventory",
]
