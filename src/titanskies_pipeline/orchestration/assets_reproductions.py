"""Explicit preflight assets for the two pinned paper-reproduction profiles."""

from dataclasses import asdict
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from titanskies_pipeline.config.settings import BASE_DIR
from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.orchestration.config import (
    ReproductionDiscoveryConfig,
    ReproductionPreflightConfig,
)
from titanskies_pipeline.reproductions.preflight import run_preflight
from titanskies_pipeline.reproductions.readiness import resolve_reproduction_sources

SUN2025_REPRO_SOURCE_INVENTORY = AssetKey(
    ["sun2025", "repro", "ops", "source_inventory"]
)
SUN2025_REPRO_SOURCE_PREFLIGHT = AssetKey(
    ["sun2025", "repro", "ops", "source_preflight"]
)
ANDREADIS2025_REPRO_SOURCE_INVENTORY = AssetKey(
    ["andreadis2025", "repro", "ops", "source_inventory"]
)
ANDREADIS2025_REPRO_SOURCE_PREFLIGHT = AssetKey(
    ["andreadis2025", "repro", "ops", "source_preflight"]
)


def _materialize_inventory(
    context: AssetExecutionContext,
    config: ReproductionDiscoveryConfig,
    *,
    profile_id: str,
) -> MaterializeResult:
    cache_dir = BASE_DIR / ".cache" / "reproduction_readiness" / profile_id
    metrics = resolve_reproduction_sources(
        profile_id,
        manifest_path=Path(config.manifest_path).resolve()
        if config.manifest_path
        else None,
        evidence_path=(
            Path(config.evidence_path).resolve()
            if config.evidence_path
            else BASE_DIR / "config" / "reproductions" / f"{profile_id}_resolution.json"
        ),
        import_dir=(
            Path(config.import_directory).resolve()
            if config.import_directory
            else cache_dir / "imports"
        ),
        output_path=(
            Path(config.output_inventory_path).resolve()
            if config.output_inventory_path
            else cache_dir / "inventory.json"
        ),
        timeout_seconds=config.timeout_seconds,
    )
    metadata = asdict(metrics)
    context.log.info("Resolved %s reproduction sources: %s", profile_id, metadata)
    return MaterializeResult(metadata=metadata)


@asset(
    key=SUN2025_REPRO_SOURCE_INVENTORY,
    group_name="research_reproductions",
)
def sun2025_repro_source_inventory_asset(
    context: AssetExecutionContext,
    config: ReproductionDiscoveryConfig,
) -> MaterializeResult:
    return _materialize_inventory(context, config, profile_id=SOURCE_SUN2025)


@asset(
    key=ANDREADIS2025_REPRO_SOURCE_INVENTORY,
    group_name="research_reproductions",
)
def andreadis2025_repro_source_inventory_asset(
    context: AssetExecutionContext,
    config: ReproductionDiscoveryConfig,
) -> MaterializeResult:
    return _materialize_inventory(context, config, profile_id=SOURCE_ANDREADIS2025)


def _materialize_preflight(
    context: AssetExecutionContext,
    config: ReproductionPreflightConfig,
    *,
    profile_id: str,
) -> MaterializeResult:
    metrics = run_preflight(
        profile_id,
        manifest_path=Path(config.manifest_path).resolve()
        if config.manifest_path
        else None,
        inventory_path=Path(config.inventory_path).resolve()
        if config.inventory_path
        else None,
        exact_mode=config.exact_mode,
        max_objects=config.max_objects,
        max_bytes=config.max_bytes,
        fail_on_blocked=config.fail_on_blocked,
    )
    metadata = asdict(metrics)
    metadata["blocking_sources"] = list(metrics.blocking_sources)
    context.log.info("Completed %s reproduction preflight: %s", profile_id, metadata)
    return MaterializeResult(metadata=metadata)


@asset(
    key=SUN2025_REPRO_SOURCE_PREFLIGHT,
    group_name="research_reproductions",
    deps=[SUN2025_REPRO_SOURCE_INVENTORY],
)
def sun2025_repro_source_preflight_asset(
    context: AssetExecutionContext,
    config: ReproductionPreflightConfig,
) -> MaterializeResult:
    return _materialize_preflight(context, config, profile_id=SOURCE_SUN2025)


@asset(
    key=ANDREADIS2025_REPRO_SOURCE_PREFLIGHT,
    group_name="research_reproductions",
    deps=[ANDREADIS2025_REPRO_SOURCE_INVENTORY],
)
def andreadis2025_repro_source_preflight_asset(
    context: AssetExecutionContext,
    config: ReproductionPreflightConfig,
) -> MaterializeResult:
    return _materialize_preflight(context, config, profile_id=SOURCE_ANDREADIS2025)


__all__ = [
    "ANDREADIS2025_REPRO_SOURCE_INVENTORY",
    "ANDREADIS2025_REPRO_SOURCE_PREFLIGHT",
    "SUN2025_REPRO_SOURCE_INVENTORY",
    "SUN2025_REPRO_SOURCE_PREFLIGHT",
    "andreadis2025_repro_source_inventory_asset",
    "andreadis2025_repro_source_preflight_asset",
    "sun2025_repro_source_inventory_asset",
    "sun2025_repro_source_preflight_asset",
]
