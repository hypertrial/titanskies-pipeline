from dagster import AssetSelection, define_asset_job, multiprocess_executor
from dagster_dbt import build_dbt_asset_selection

from titanskies_pipeline.orchestration.assets_riverpulse_events import (
    RIVERPULSE_EVENTS_RAW_OBSERVATIONS,
    RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY,
)
from titanskies_pipeline.orchestration.assets_tempo_no2 import titanskies_dbt
from titanskies_pipeline.orchestration.config import (
    full_pipeline_run_config,
    riverpulse_events_dbt_run_config,
    riverpulse_events_discovery_run_config,
    riverpulse_events_full_pipeline_run_config,
    riverpulse_events_ingest_run_config,
    scope_run_config,
)
from titanskies_pipeline.orchestration.scope_registry import (
    SHIPPED_SCOPE_SPECS,
    TEMPO_NO2_SCOPE,
    TEMPO_NO2_STD_SCOPE,
    ScopeSpec,
)

_ANALYTICS_BUILD_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": 1},
    name="duckdb_serial_multiprocess",
)

_RIVERPULSE_TAGS = {
    "duckdb_warehouse": "true",
    "source": "riverpulse",
    "scope": "events",
}
RIVERPULSE_EVENTS_DISCOVERY_SELECTION = AssetSelection.assets(
    RIVERPULSE_EVENTS_RAW_SOURCE_INVENTORY
)
RIVERPULSE_EVENTS_INGEST_SELECTION = AssetSelection.assets(
    RIVERPULSE_EVENTS_RAW_OBSERVATIONS
)
RIVERPULSE_EVENTS_DBT_SELECTION = build_dbt_asset_selection(
    [titanskies_dbt],
    dbt_select="tag:riverpulse,tag:events",
)
RIVERPULSE_EVENTS_FULL_PIPELINE_SELECTION = (
    RIVERPULSE_EVENTS_DISCOVERY_SELECTION
    | RIVERPULSE_EVENTS_INGEST_SELECTION
    | RIVERPULSE_EVENTS_DBT_SELECTION
)
riverpulse_events_source_discovery = define_asset_job(
    "riverpulse_events_source_discovery",
    selection=RIVERPULSE_EVENTS_DISCOVERY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=riverpulse_events_discovery_run_config(),
    tags=_RIVERPULSE_TAGS,
)
riverpulse_events_observation_ingest = define_asset_job(
    "riverpulse_events_observation_ingest",
    selection=RIVERPULSE_EVENTS_INGEST_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=riverpulse_events_ingest_run_config(),
    tags=_RIVERPULSE_TAGS,
)
riverpulse_events_dbt_build = define_asset_job(
    "riverpulse_events_dbt_build",
    selection=RIVERPULSE_EVENTS_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=riverpulse_events_dbt_run_config(),
    tags=_RIVERPULSE_TAGS,
)
riverpulse_events_full_pipeline = define_asset_job(
    "riverpulse_events_full_pipeline",
    selection=RIVERPULSE_EVENTS_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=riverpulse_events_full_pipeline_run_config(),
    tags=_RIVERPULSE_TAGS,
)


def _scope_tags(spec: ScopeSpec) -> dict[str, str]:
    return {
        "duckdb_warehouse": "true",
        "source": spec.source,
        "scope": spec.scope,
    }


def _build_scope_jobs(spec: ScopeSpec) -> dict[str, object]:
    discovery_selection = AssetSelection.assets(spec.raw_granule_inventory_key)
    processing_selection = AssetSelection.assets(spec.raw_region_hour_aggregates_key)
    dbt_selection = build_dbt_asset_selection(
        [titanskies_dbt],
        dbt_select=spec.dbt_select,
        dbt_exclude=spec.dbt_exclude,
    )
    full_selection = discovery_selection | processing_selection | dbt_selection
    tags = _scope_tags(spec)
    return {
        spec.discovery_job_name: define_asset_job(
            spec.discovery_job_name,
            selection=discovery_selection,
            executor_def=_ANALYTICS_BUILD_EXECUTOR,
            config=scope_run_config(spec, "discovery"),
            tags=tags,
        ),
        spec.ingest_job_name: define_asset_job(
            spec.ingest_job_name,
            selection=processing_selection,
            executor_def=_ANALYTICS_BUILD_EXECUTOR,
            config=scope_run_config(spec, "ingest"),
            tags=tags,
        ),
        spec.dbt_job_name: define_asset_job(
            spec.dbt_job_name,
            selection=dbt_selection,
            executor_def=_ANALYTICS_BUILD_EXECUTOR,
            config=scope_run_config(spec, "dbt"),
            tags=tags,
        ),
        spec.full_job_name: define_asset_job(
            spec.full_job_name,
            selection=full_selection,
            executor_def=_ANALYTICS_BUILD_EXECUTOR,
            config=full_pipeline_run_config(spec),
            tags=tags,
        ),
    }


SCOPE_JOBS: dict[str, object] = {}
for _spec in SHIPPED_SCOPE_SPECS:
    SCOPE_JOBS.update(_build_scope_jobs(_spec))

tempo_no2_granule_discovery = SCOPE_JOBS[TEMPO_NO2_SCOPE.discovery_job_name]
tempo_no2_hourly_ingest = SCOPE_JOBS[TEMPO_NO2_SCOPE.ingest_job_name]
tempo_no2_dbt_build = SCOPE_JOBS[TEMPO_NO2_SCOPE.dbt_job_name]
tempo_no2_full_pipeline = SCOPE_JOBS[TEMPO_NO2_SCOPE.full_job_name]
tempo_no2_std_granule_discovery = SCOPE_JOBS[TEMPO_NO2_STD_SCOPE.discovery_job_name]
tempo_no2_std_hourly_ingest = SCOPE_JOBS[TEMPO_NO2_STD_SCOPE.ingest_job_name]
tempo_no2_std_dbt_build = SCOPE_JOBS[TEMPO_NO2_STD_SCOPE.dbt_job_name]
tempo_no2_std_full_pipeline = SCOPE_JOBS[TEMPO_NO2_STD_SCOPE.full_job_name]

# Selection constants retained for tests/operators that import them.
TEMPO_NO2_DISCOVERY_SELECTION = AssetSelection.assets(
    TEMPO_NO2_SCOPE.raw_granule_inventory_key
)
TEMPO_NO2_PROCESSING_SELECTION = AssetSelection.assets(
    TEMPO_NO2_SCOPE.raw_region_hour_aggregates_key
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
TEMPO_NO2_STD_DISCOVERY_SELECTION = AssetSelection.assets(
    TEMPO_NO2_STD_SCOPE.raw_granule_inventory_key
)
TEMPO_NO2_STD_PROCESSING_SELECTION = AssetSelection.assets(
    TEMPO_NO2_STD_SCOPE.raw_region_hour_aggregates_key
)
TEMPO_NO2_STD_DBT_SELECTION = build_dbt_asset_selection(
    [titanskies_dbt],
    dbt_select=TEMPO_NO2_STD_SCOPE.dbt_select,
    dbt_exclude=TEMPO_NO2_STD_SCOPE.dbt_exclude,
)
TEMPO_NO2_STD_FULL_PIPELINE_SELECTION = (
    TEMPO_NO2_STD_DISCOVERY_SELECTION
    | TEMPO_NO2_STD_PROCESSING_SELECTION
    | TEMPO_NO2_STD_DBT_SELECTION
)

__all__ = [
    "riverpulse_events_dbt_build",
    "riverpulse_events_full_pipeline",
    "riverpulse_events_observation_ingest",
    "riverpulse_events_source_discovery",
    "tempo_no2_dbt_build",
    "tempo_no2_full_pipeline",
    "tempo_no2_granule_discovery",
    "tempo_no2_hourly_ingest",
    "tempo_no2_std_dbt_build",
    "tempo_no2_std_full_pipeline",
    "tempo_no2_std_granule_discovery",
    "tempo_no2_std_hourly_ingest",
]
