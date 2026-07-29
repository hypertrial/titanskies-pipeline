from __future__ import annotations

from dagster import AssetKey, Config
from pydantic import Field, model_validator

from titanskies_pipeline.config.settings_tempo import get_tempo_scope_settings
from titanskies_pipeline.orchestration.scope_registry import (
    TEMPO_NO2_OPS_REGION_REGISTRY,
    TEMPO_NO2_RAW_GRANULE_INVENTORY,
    TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES,
    TEMPO_NO2_SCOPE,
    TEMPO_NO2_STD_OPS_REGION_REGISTRY,
    TEMPO_NO2_STD_RAW_GRANULE_INVENTORY,
    TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES,
    TEMPO_NO2_STD_SCOPE,
    ScopeSpec,
    ScopeStep,
)
from titanskies_pipeline.orchestration.timestamps import parse_iso_utc

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
    # None means "use the scope's configured discovery lookback" at runtime.
    lookback_hours: int | None = Field(default=None, ge=1)
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
        if has_start and has_end:
            try:
                start = parse_iso_utc(self.window_start_utc or "")
                end = parse_iso_utc(self.window_end_utc or "")
            except ValueError as exc:
                raise ValueError(
                    "window_start_utc and window_end_utc must be ISO-8601 timestamps"
                ) from exc
            if start >= end:
                raise ValueError(
                    "window_start_utc must be strictly before window_end_utc"
                )
        return self


class HourlyIngestConfig(Config):
    max_granules: int | None = Field(default=None, ge=1)
    allow_synthetic: bool = False


class RiverPulseNetworkConfig(Config):
    manifest_path: str | None = None
    allow_synthetic: bool = False


class RiverPulseDiscoveryConfig(Config):
    window_start_utc: str | None = None
    window_end_utc: str | None = None
    backfill: bool = False
    reach_ids: list[str] | None = None
    allow_synthetic: bool = False

    @model_validator(mode="after")
    def _validate_window(self) -> "RiverPulseDiscoveryConfig":
        has_start = self.window_start_utc is not None
        has_end = self.window_end_utc is not None
        if has_start != has_end:
            raise ValueError(
                "window_start_utc and window_end_utc must both be set together"
            )
        if has_start and has_end:
            start = parse_iso_utc(self.window_start_utc or "")
            end = parse_iso_utc(self.window_end_utc or "")
            if start >= end:
                raise ValueError(
                    "window_start_utc must be strictly before window_end_utc"
                )
        return self


class RiverPulseIngestConfig(Config):
    max_requests: int | None = Field(default=None, ge=1)
    raw_data_dir: str | None = None


class PlumeGraphFacilityRegistryConfig(Config):
    manifest_path: str | None = None
    allow_synthetic: bool = False


class PlumeGraphDiscoveryConfig(Config):
    window_start_utc: str | None = None
    window_end_utc: str | None = None
    backfill: bool = False

    @model_validator(mode="after")
    def _validate_window(self) -> "PlumeGraphDiscoveryConfig":
        has_start = self.window_start_utc is not None
        has_end = self.window_end_utc is not None
        if has_start != has_end:
            raise ValueError(
                "window_start_utc and window_end_utc must both be set together"
            )
        if has_start and has_end:
            start = parse_iso_utc(self.window_start_utc or "")
            end = parse_iso_utc(self.window_end_utc or "")
            if start >= end:
                raise ValueError(
                    "window_start_utc must be strictly before window_end_utc"
                )
        return self


class PlumeGraphIngestConfig(Config):
    max_requests: int | None = Field(default=None, ge=1)
    raw_data_dir: str | None = None


class PlumeGraphAnalysisConfig(Config):
    partition_dates: list[str] | None = None


class PlumeGraphValidationConfig(Config):
    benchmark_path: str | None = None
    benchmark_version: str = "plumegraph-benchmark-2024-v1"
    allow_incomplete: bool = False


class PlumeGraphReleaseConfig(Config):
    release_version: str = "v0.6.0"
    output_dir: str | None = None
    validation_run_id: str | None = None
    require_passed_validation: bool = True


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


def riverpulse_events_discovery_run_config() -> dict:
    return _op_config(
        AssetKey(["riverpulse", "events", "raw", "source_inventory"]),
        RiverPulseDiscoveryConfig(),
    )


def riverpulse_events_ingest_run_config() -> dict:
    return _op_config(
        AssetKey(["riverpulse", "events", "raw", "observations"]),
        RiverPulseIngestConfig(),
    )


def riverpulse_events_dbt_run_config() -> dict:
    return _op_config(
        AssetKey(["titanskies_dbt"]),
        DbtBuildConfig(dbt_select="tag:riverpulse,tag:events"),
    )


def riverpulse_events_full_pipeline_run_config() -> dict:
    return _merge_op_configs(
        riverpulse_events_discovery_run_config(),
        riverpulse_events_ingest_run_config(),
        riverpulse_events_dbt_run_config(),
    )


def plumegraph_events_discovery_run_config() -> dict:
    return _op_config(
        AssetKey(["plumegraph", "events", "raw", "source_inventory"]),
        PlumeGraphDiscoveryConfig(),
    )


def plumegraph_events_ingest_run_config() -> dict:
    return _merge_op_configs(
        *[
            _op_config(
                AssetKey(["plumegraph", "events", "raw", asset_name]),
                PlumeGraphIngestConfig(),
            )
            for asset_name in (
                "tempo_snapshots",
                "hrrr_snapshots",
                "camd_emissions",
            )
        ]
    )


def plumegraph_events_analysis_run_config() -> dict:
    return _op_config(
        AssetKey(["plumegraph", "events", "intermediate", "analysis_results"]),
        PlumeGraphAnalysisConfig(),
    )


def plumegraph_events_dbt_run_config() -> dict:
    return _op_config(
        AssetKey(["titanskies_dbt"]),
        DbtBuildConfig(dbt_select="tag:plumegraph,tag:events"),
    )


def plumegraph_events_validation_run_config() -> dict:
    return _op_config(
        AssetKey(["plumegraph", "events", "observability", "validation"]),
        PlumeGraphValidationConfig(),
    )


def plumegraph_events_release_run_config() -> dict:
    return _op_config(
        AssetKey(["plumegraph", "events", "releases", "evidence_ledger"]),
        PlumeGraphReleaseConfig(),
    )


def plumegraph_events_full_pipeline_run_config() -> dict:
    return _merge_op_configs(
        plumegraph_events_discovery_run_config(),
        plumegraph_events_ingest_run_config(),
        plumegraph_events_analysis_run_config(),
        plumegraph_events_dbt_run_config(),
        plumegraph_events_validation_run_config(),
    )


def region_registry_run_config(spec: ScopeSpec) -> dict:
    return _op_config(spec.ops_region_registry_key, RegionRegistryConfig())


def scope_run_config(spec: ScopeSpec, step: ScopeStep) -> dict:
    if step == "discovery":
        lookback = get_tempo_scope_settings(spec.scope).discovery_lookback_hours
        return _op_config(
            spec.raw_granule_inventory_key,
            GranuleDiscoveryConfig(lookback_hours=lookback),
        )
    if step == "ingest":
        return _op_config(spec.raw_region_hour_aggregates_key, HourlyIngestConfig())
    if step == "dbt":
        return _op_config(
            AssetKey(["titanskies_dbt"]),
            DbtBuildConfig(
                dbt_select=spec.dbt_select,
                dbt_exclude=spec.dbt_exclude,
            ),
        )
    if step == "full":
        return full_pipeline_run_config(spec)
    raise ValueError(f"Unsupported scope step {step!r}")


def full_pipeline_run_config(spec: ScopeSpec) -> dict:
    return _merge_op_configs(
        scope_run_config(spec, "discovery"),
        scope_run_config(spec, "ingest"),
        scope_run_config(spec, "dbt"),
    )


def tempo_no2_region_registry_run_config() -> dict:
    return region_registry_run_config(TEMPO_NO2_SCOPE)


def tempo_no2_granule_discovery_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_SCOPE, "discovery")


def tempo_no2_hourly_ingest_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_SCOPE, "ingest")


def tempo_no2_dbt_build_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_SCOPE, "dbt")


def tempo_no2_full_pipeline_run_config() -> dict:
    return full_pipeline_run_config(TEMPO_NO2_SCOPE)


def tempo_no2_std_region_registry_run_config() -> dict:
    return region_registry_run_config(TEMPO_NO2_STD_SCOPE)


def tempo_no2_std_granule_discovery_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_STD_SCOPE, "discovery")


def tempo_no2_std_hourly_ingest_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_STD_SCOPE, "ingest")


def tempo_no2_std_dbt_build_run_config() -> dict:
    return scope_run_config(TEMPO_NO2_STD_SCOPE, "dbt")


def tempo_no2_std_full_pipeline_run_config() -> dict:
    return full_pipeline_run_config(TEMPO_NO2_STD_SCOPE)


__all__ = [
    "DbtBuildConfig",
    "GranuleDiscoveryConfig",
    "GuardrailConfig",
    "HourlyIngestConfig",
    "PlumeGraphAnalysisConfig",
    "PlumeGraphDiscoveryConfig",
    "PlumeGraphFacilityRegistryConfig",
    "PlumeGraphIngestConfig",
    "PlumeGraphReleaseConfig",
    "PlumeGraphValidationConfig",
    "RegionRegistryConfig",
    "RiverPulseDiscoveryConfig",
    "RiverPulseIngestConfig",
    "RiverPulseNetworkConfig",
    "TEMPO_NO2_OPS_REGION_REGISTRY",
    "TEMPO_NO2_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES",
    "TEMPO_NO2_STD_OPS_REGION_REGISTRY",
    "TEMPO_NO2_STD_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES",
    "full_pipeline_run_config",
    "plumegraph_events_analysis_run_config",
    "plumegraph_events_dbt_run_config",
    "plumegraph_events_discovery_run_config",
    "plumegraph_events_full_pipeline_run_config",
    "plumegraph_events_ingest_run_config",
    "plumegraph_events_release_run_config",
    "plumegraph_events_validation_run_config",
    "region_registry_run_config",
    "riverpulse_events_dbt_run_config",
    "riverpulse_events_discovery_run_config",
    "riverpulse_events_full_pipeline_run_config",
    "riverpulse_events_ingest_run_config",
    "scope_run_config",
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
