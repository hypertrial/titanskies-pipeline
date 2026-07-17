from dagster import AssetSelection, define_asset_job, multiprocess_executor
from dagster_dbt import build_dbt_asset_selection

from titanskies_pipeline.naming import SCOPE_NO2, SOURCE_TEMPO, asset_key
from titanskies_pipeline.orchestration.assets_tempo_no2 import titanskies_dbt
from titanskies_pipeline.orchestration.config import (
    tempo_no2_dbt_build_run_config,
    tempo_no2_full_pipeline_run_config,
    tempo_no2_granule_discovery_run_config,
    tempo_no2_hourly_ingest_run_config,
)
from titanskies_pipeline.orchestration.scope_registry import TEMPO_NO2_SCOPE

_ANALYTICS_BUILD_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": 1},
    name="duckdb_serial_multiprocess",
)
_TEMPO_NO2_TAGS = {
    "duckdb_warehouse": "true",
    "source": SOURCE_TEMPO,
    "scope": SCOPE_NO2,
}

TEMPO_NO2_DISCOVERY_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_TEMPO, SCOPE_NO2, "raw", "granule_inventory"),
)

TEMPO_NO2_PROCESSING_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_TEMPO, SCOPE_NO2, "raw", "region_hour_aggregates"),
)

TEMPO_NO2_DBT_SELECTION = build_dbt_asset_selection(
    [titanskies_dbt],
    dbt_select=TEMPO_NO2_SCOPE.dbt_select,
    dbt_exclude=TEMPO_NO2_SCOPE.dbt_exclude,
)

TEMPO_NO2_FULL_PIPELINE_SELECTION = (
    TEMPO_NO2_DISCOVERY_SELECTION
    | TEMPO_NO2_PROCESSING_SELECTION
    | TEMPO_NO2_DBT_SELECTION
)

tempo_no2_granule_discovery = define_asset_job(
    TEMPO_NO2_SCOPE.discovery_job_name,
    selection=TEMPO_NO2_DISCOVERY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=tempo_no2_granule_discovery_run_config(),
    tags=_TEMPO_NO2_TAGS,
)

tempo_no2_hourly_ingest = define_asset_job(
    TEMPO_NO2_SCOPE.ingest_job_name,
    selection=TEMPO_NO2_PROCESSING_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=tempo_no2_hourly_ingest_run_config(),
    tags=_TEMPO_NO2_TAGS,
)

tempo_no2_dbt_build = define_asset_job(
    TEMPO_NO2_SCOPE.dbt_job_name,
    selection=TEMPO_NO2_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=tempo_no2_dbt_build_run_config(),
    tags=_TEMPO_NO2_TAGS,
)

tempo_no2_full_pipeline = define_asset_job(
    TEMPO_NO2_SCOPE.full_job_name,
    selection=TEMPO_NO2_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=tempo_no2_full_pipeline_run_config(),
    tags=_TEMPO_NO2_TAGS,
)

__all__ = [
    "tempo_no2_dbt_build",
    "tempo_no2_full_pipeline",
    "tempo_no2_granule_discovery",
    "tempo_no2_hourly_ingest",
]
