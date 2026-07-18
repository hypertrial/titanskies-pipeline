from __future__ import annotations

from dagster import AssetKey, Config
from pydantic import Field, model_validator

from titanskies_pipeline.config.settings import (
    TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS,
    TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS,
)
from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD, SOURCE_TEMPO, asset_key

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

DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 60
DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS = 900
DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 2700
DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 3600
DEFAULT_PROGRESS_POLL_SECONDS = 5


class GuardrailConfig(Config):
    progress_log_interval_seconds: int = Field(
        default=DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS, ge=1
    )
    no_progress_soft_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS,
        ge=1,
    )
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    progress_poll_seconds: int = Field(default=DEFAULT_PROGRESS_POLL_SECONDS, ge=1)

    @model_validator(mode="after")
    def _validate_soft_hard_timeouts(self) -> "GuardrailConfig":
        soft = self.no_progress_soft_timeout_seconds
        hard = self.no_progress_hard_timeout_seconds
        if soft is not None and hard is not None and hard <= soft:
            raise ValueError(
                "no_progress_hard_timeout_seconds must be greater than "
                "no_progress_soft_timeout_seconds when both are set"
            )
        return self


class RegionRegistryConfig(Config):
    manifest_path: str | None = None
    allow_synthetic: bool = False


class GranuleDiscoveryConfig(Config):
    lookback_hours: int = Field(default=TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS, ge=1)
    allow_synthetic: bool = False
    window_start_utc: str | None = None
    window_end_utc: str | None = None

    @model_validator(mode="after")
    def _validate_window(self) -> "GranuleDiscoveryConfig":
        has_start = self.window_start_utc is not None
        has_end = self.window_end_utc is not None
        if has_start != has_end:
            raise ValueError(
                "window_start_utc and window_end_utc must both be set together"
            )
        return self


class HourlyIngestConfig(Config):
    max_granules: int | None = Field(default=None, ge=1)
    allow_synthetic: bool = False


class DbtBuildConfig(GuardrailConfig):
    progress_log_interval_events: int = Field(default=20, ge=1)
    dbt_select: str | None = None
    dbt_exclude: str | None = None
    full_refresh: bool = False
    fetch_dbt_metadata: bool = True
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )


def _op_name(key: AssetKey) -> str:
    return "__".join(key.path)


def _op_config(key: AssetKey, config: Config) -> dict:
    return {"ops": {_op_name(key): {"config": config.model_dump()}}}


def _merge_op_configs(*configs: dict) -> dict:
    merged = {"ops": {}}
    for config in configs:
        merged["ops"].update(config.get("ops", {}))
    return merged


def tempo_no2_region_registry_run_config() -> dict:
    return _op_config(TEMPO_NO2_OPS_REGION_REGISTRY, RegionRegistryConfig())


def tempo_no2_granule_discovery_run_config() -> dict:
    return _op_config(TEMPO_NO2_RAW_GRANULE_INVENTORY, GranuleDiscoveryConfig())


def tempo_no2_hourly_ingest_run_config() -> dict:
    return _op_config(
        TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES,
        HourlyIngestConfig(),
    )


def tempo_no2_dbt_build_run_config() -> dict:
    from titanskies_pipeline.orchestration.scope_registry import TEMPO_NO2_SCOPE

    return _op_config(
        AssetKey(["titanskies_dbt"]),
        DbtBuildConfig(
            dbt_select=TEMPO_NO2_SCOPE.dbt_select,
            dbt_exclude=TEMPO_NO2_SCOPE.dbt_exclude,
        ),
    )


def tempo_no2_full_pipeline_run_config() -> dict:
    return _merge_op_configs(
        tempo_no2_granule_discovery_run_config(),
        tempo_no2_hourly_ingest_run_config(),
        tempo_no2_dbt_build_run_config(),
    )


def tempo_no2_std_region_registry_run_config() -> dict:
    return _op_config(TEMPO_NO2_STD_OPS_REGION_REGISTRY, RegionRegistryConfig())


def tempo_no2_std_granule_discovery_run_config() -> dict:
    return _op_config(
        TEMPO_NO2_STD_RAW_GRANULE_INVENTORY,
        GranuleDiscoveryConfig(lookback_hours=TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS),
    )


def tempo_no2_std_hourly_ingest_run_config() -> dict:
    return _op_config(
        TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES,
        HourlyIngestConfig(),
    )


def tempo_no2_std_dbt_build_run_config() -> dict:
    from titanskies_pipeline.orchestration.scope_registry import TEMPO_NO2_STD_SCOPE

    return _op_config(
        AssetKey(["titanskies_dbt"]),
        DbtBuildConfig(
            dbt_select=TEMPO_NO2_STD_SCOPE.dbt_select,
            dbt_exclude=TEMPO_NO2_STD_SCOPE.dbt_exclude,
        ),
    )


def tempo_no2_std_full_pipeline_run_config() -> dict:
    return _merge_op_configs(
        tempo_no2_std_granule_discovery_run_config(),
        tempo_no2_std_hourly_ingest_run_config(),
        tempo_no2_std_dbt_build_run_config(),
    )


__all__ = [
    "DbtBuildConfig",
    "GranuleDiscoveryConfig",
    "GuardrailConfig",
    "HourlyIngestConfig",
    "RegionRegistryConfig",
    "tempo_no2_dbt_build_run_config",
    "tempo_no2_full_pipeline_run_config",
    "tempo_no2_granule_discovery_run_config",
    "tempo_no2_hourly_ingest_run_config",
    "tempo_no2_region_registry_run_config",
    "tempo_no2_std_dbt_build_run_config",
    "tempo_no2_std_full_pipeline_run_config",
    "tempo_no2_std_granule_discovery_run_config",
    "tempo_no2_std_hourly_ingest_run_config",
    "tempo_no2_std_region_registry_run_config",
]
