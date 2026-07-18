from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from dagster_dbt import DbtCliResource, dbt_assets

from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD, SOURCE_TEMPO, asset_key
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

TEMPO_NO2_STD_OPS_REGION_REGISTRY = asset_key(
    SOURCE_TEMPO, SCOPE_NO2_STD, "ops", "region_registry"
)
TEMPO_NO2_STD_RAW_GRANULE_INVENTORY = asset_key(
    SOURCE_TEMPO, SCOPE_NO2_STD, "raw", "granule_inventory"
)
TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES = asset_key(
    SOURCE_TEMPO, SCOPE_NO2_STD, "raw", "region_hour_aggregates"
)


def _build_region_registry_asset(*, scope: str, key: AssetKey):
    @asset(key=key, group_name="ingestion")
    def _region_registry_asset(
        context: AssetExecutionContext,
        config: RegionRegistryConfig,
    ) -> MaterializeResult:
        manifest_path = Path(config.manifest_path) if config.manifest_path else None
        metrics = ops.sync_region_registry(
            manifest_path=manifest_path,
            scope=scope,
            allow_synthetic=config.allow_synthetic,
        )
        context.log.info("Loaded region registry (%s): %s", scope, metrics)
        return MaterializeResult(metadata=metrics)

    return _region_registry_asset


def _build_granule_inventory_asset(*, scope: str, key: AssetKey):
    @asset(key=key, group_name="ingestion")
    def _granule_inventory_asset(
        context: AssetExecutionContext,
        config: GranuleDiscoveryConfig,
    ) -> MaterializeResult:
        ops.require_registered_geography(
            scope=scope, allow_synthetic=config.allow_synthetic
        )
        window_start = (
            None
            if config.window_start_utc is None
            else _parse_iso_utc(config.window_start_utc)
        )
        window_end = (
            None
            if config.window_end_utc is None
            else _parse_iso_utc(config.window_end_utc)
        )
        metrics = ops.sync_granule_discovery(
            scope=scope,
            lookback_hours=None if window_start else config.lookback_hours,
            window_start=window_start,
            window_end=window_end,
        )
        context.log.info("Granule discovery metrics (%s): %s", scope, metrics)
        return MaterializeResult(
            metadata={
                "found": metrics.found,
                "inserted": metrics.inserted,
                "refreshed": metrics.refreshed,
            }
        )

    return _granule_inventory_asset


def _build_region_hour_aggregates_asset(*, scope: str, key: AssetKey, deps: list):
    @asset(key=key, deps=deps, group_name="ingestion")
    def _region_hour_aggregates_asset(
        context: AssetExecutionContext,
        config: HourlyIngestConfig,
    ) -> MaterializeResult:
        metrics = ops.process_pending_granules(
            scope=scope,
            max_granules=config.max_granules,
            allow_synthetic=config.allow_synthetic,
        )
        context.log.info("Hourly ingest metrics (%s): %s", scope, metrics)
        return MaterializeResult(
            metadata={
                "downloaded": metrics.downloaded,
                "processed": metrics.processed,
                "aggregates_written": metrics.aggregates_written,
                "raw_files_pruned": metrics.raw_files_pruned,
            }
        )

    return _region_hour_aggregates_asset


def _parse_iso_utc(value: str):
    from datetime import datetime, timezone

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


tempo_no2_ops_region_registry = _build_region_registry_asset(
    scope=SCOPE_NO2, key=TEMPO_NO2_OPS_REGION_REGISTRY
)
tempo_no2_raw_granule_inventory = _build_granule_inventory_asset(
    scope=SCOPE_NO2, key=TEMPO_NO2_RAW_GRANULE_INVENTORY
)
tempo_no2_raw_region_hour_aggregates = _build_region_hour_aggregates_asset(
    scope=SCOPE_NO2,
    key=TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES,
    deps=[TEMPO_NO2_RAW_GRANULE_INVENTORY],
)

tempo_no2_std_ops_region_registry = _build_region_registry_asset(
    scope=SCOPE_NO2_STD, key=TEMPO_NO2_STD_OPS_REGION_REGISTRY
)
tempo_no2_std_raw_granule_inventory = _build_granule_inventory_asset(
    scope=SCOPE_NO2_STD, key=TEMPO_NO2_STD_RAW_GRANULE_INVENTORY
)
tempo_no2_std_raw_region_hour_aggregates = _build_region_hour_aggregates_asset(
    scope=SCOPE_NO2_STD,
    key=TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES,
    deps=[TEMPO_NO2_STD_RAW_GRANULE_INVENTORY],
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
    "TEMPO_NO2_STD_OPS_REGION_REGISTRY",
    "TEMPO_NO2_STD_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES",
    "tempo_no2_ops_region_registry",
    "tempo_no2_raw_granule_inventory",
    "tempo_no2_raw_region_hour_aggregates",
    "tempo_no2_std_ops_region_registry",
    "tempo_no2_std_raw_granule_inventory",
    "tempo_no2_std_raw_region_hour_aggregates",
    "titanskies_dbt",
]
