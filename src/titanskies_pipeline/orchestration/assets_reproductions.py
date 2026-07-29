"""Explicit preflight assets for the two pinned paper-reproduction profiles."""

from dataclasses import asdict
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.orchestration.config import ReproductionPreflightConfig
from titanskies_pipeline.reproductions.preflight import run_preflight

SUN2025_REPRO_SOURCE_PREFLIGHT = AssetKey(
    ["sun2025", "repro", "ops", "source_preflight"]
)
ANDREADIS2025_REPRO_SOURCE_PREFLIGHT = AssetKey(
    ["andreadis2025", "repro", "ops", "source_preflight"]
)


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
)
def sun2025_repro_source_preflight_asset(
    context: AssetExecutionContext,
    config: ReproductionPreflightConfig,
) -> MaterializeResult:
    return _materialize_preflight(context, config, profile_id=SOURCE_SUN2025)


@asset(
    key=ANDREADIS2025_REPRO_SOURCE_PREFLIGHT,
    group_name="research_reproductions",
)
def andreadis2025_repro_source_preflight_asset(
    context: AssetExecutionContext,
    config: ReproductionPreflightConfig,
) -> MaterializeResult:
    return _materialize_preflight(context, config, profile_id=SOURCE_ANDREADIS2025)


__all__ = [
    "ANDREADIS2025_REPRO_SOURCE_PREFLIGHT",
    "SUN2025_REPRO_SOURCE_PREFLIGHT",
    "andreadis2025_repro_source_preflight_asset",
    "sun2025_repro_source_preflight_asset",
]
