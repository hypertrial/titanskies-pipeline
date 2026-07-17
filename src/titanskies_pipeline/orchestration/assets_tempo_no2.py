from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset
from dagster_dbt import DbtCliResource, dbt_assets

from titanskies_pipeline.naming import SCOPE_NO2, SOURCE_TEMPO, asset_key
from titanskies_pipeline.orchestration import tempo_ops as ops
from titanskies_pipeline.orchestration.config import (
    DbtBuildConfig,
    GranuleDiscoveryConfig,
    HourlyIngestConfig,
    RegionRegistryConfig,
)
from titanskies_pipeline.orchestration.dbt_build import stream_dbt_build
from titanskies_pipeline.orchestration.dbt_project import DBT_PROJECT
from titanskies_pipeline.orchestration.translators import TempoDagsterDbtTranslator

TEMPO_NO2_OPS_REGION_REGISTRY = asset_key(
    SOURCE_TEMPO, SCOPE_NO2, "ops", "region_registry"
)
TEMPO_NO2_RAW_GRANULE_INVENTORY = asset_key(
    SOURCE_TEMPO, SCOPE_NO2, "raw", "granule_inventory"
)
TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES = asset_key(
    SOURCE_TEMPO, SCOPE_NO2, "raw", "region_hour_aggregates"
)


@asset(
    key=TEMPO_NO2_OPS_REGION_REGISTRY,
    group_name="ingestion",
)
def tempo_no2_ops_region_registry(
    context: AssetExecutionContext,
    config: RegionRegistryConfig,
) -> MaterializeResult:
    manifest_path = Path(config.manifest_path) if config.manifest_path else None
    metrics = ops.sync_region_registry(
        manifest_path=manifest_path,
        allow_synthetic=config.allow_synthetic,
    )
    context.log.info("Loaded region registry: %s", metrics)
    return MaterializeResult(metadata=metrics)


@asset(
    key=TEMPO_NO2_RAW_GRANULE_INVENTORY,
    group_name="ingestion",
)
def tempo_no2_raw_granule_inventory(
    context: AssetExecutionContext,
    config: GranuleDiscoveryConfig,
) -> MaterializeResult:
    ops.require_registered_geography(allow_synthetic=config.allow_synthetic)
    metrics = ops.sync_granule_discovery(lookback_hours=config.lookback_hours)
    context.log.info("Granule discovery metrics: %s", metrics)
    return MaterializeResult(
        metadata={
            "found": metrics.found,
            "inserted": metrics.inserted,
            "refreshed": metrics.refreshed,
        }
    )


@asset(
    key=TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES,
    deps=[TEMPO_NO2_RAW_GRANULE_INVENTORY],
    group_name="ingestion",
)
def tempo_no2_raw_region_hour_aggregates(
    context: AssetExecutionContext,
    config: HourlyIngestConfig,
) -> MaterializeResult:
    metrics = ops.process_pending_granules(
        max_granules=config.max_granules,
        allow_synthetic=config.allow_synthetic,
    )
    context.log.info("Hourly ingest metrics: %s", metrics)
    return MaterializeResult(
        metadata={
            "downloaded": metrics.downloaded,
            "processed": metrics.processed,
            "aggregates_written": metrics.aggregates_written,
            "raw_files_pruned": metrics.raw_files_pruned,
        }
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    dagster_dbt_translator=TempoDagsterDbtTranslator(),
    name="titanskies_dbt",
)
def titanskies_dbt(
    context: AssetExecutionContext, dbt: DbtCliResource, config: DbtBuildConfig
):
    yield from stream_dbt_build(
        asset_name="titanskies_dbt",
        context=context,
        dbt=dbt,
        config=config,
    )


__all__ = [
    "TEMPO_NO2_OPS_REGION_REGISTRY",
    "TEMPO_NO2_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES",
    "tempo_no2_ops_region_registry",
    "tempo_no2_raw_granule_inventory",
    "tempo_no2_raw_region_hour_aggregates",
    "titanskies_dbt",
]
