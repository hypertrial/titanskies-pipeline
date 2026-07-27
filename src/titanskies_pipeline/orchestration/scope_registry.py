"""Static registry of shipped source/scope orchestration surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dagster import AssetKey

from titanskies_pipeline.naming import (
    SCOPE_NO2,
    SCOPE_NO2_STD,
    SOURCE_TEMPO,
    asset_key,
    flat_name,
)

ScopeStep = Literal["discovery", "ingest", "dbt", "full"]
SCOPE_STEPS: tuple[ScopeStep, ...] = ("discovery", "ingest", "dbt", "full")


@dataclass(frozen=True)
class ScopeSpec:
    source: str
    scope: str
    label: str
    discovery_job_name: str
    ingest_job_name: str
    dbt_job_name: str
    full_job_name: str
    dbt_select: str
    schedule_name: str
    schedule_cron: str
    schedule_description: str
    dbt_exclude: str | None = None

    @property
    def key(self) -> str:
        return f"{self.source}:{self.scope}"

    @property
    def namespace(self) -> str:
        return flat_name(self.source, self.scope)

    @property
    def aliases(self) -> tuple[str, str]:
        return (self.key, self.namespace)

    @property
    def supported_steps(self) -> tuple[ScopeStep, ...]:
        return SCOPE_STEPS

    def job_for_step(self, step: ScopeStep) -> str:
        return {
            "discovery": self.discovery_job_name,
            "ingest": self.ingest_job_name,
            "dbt": self.dbt_job_name,
            "full": self.full_job_name,
        }[step]

    @property
    def ops_region_registry_key(self) -> AssetKey:
        return asset_key(self.source, self.scope, "ops", "region_registry")

    @property
    def raw_granule_inventory_key(self) -> AssetKey:
        return asset_key(self.source, self.scope, "raw", "granule_inventory")

    @property
    def raw_region_hour_aggregates_key(self) -> AssetKey:
        return asset_key(self.source, self.scope, "raw", "region_hour_aggregates")


TEMPO_NO2_SCOPE = ScopeSpec(
    source=SOURCE_TEMPO,
    scope=SCOPE_NO2,
    label="TEMPO NO2",
    discovery_job_name="tempo_no2_granule_discovery",
    ingest_job_name="tempo_no2_hourly_ingest",
    dbt_job_name="tempo_no2_dbt_build",
    full_job_name="tempo_no2_full_pipeline",
    dbt_select="+tag:tempo,tag:no2",
    schedule_name="tempo_no2_hourly_pipeline_schedule",
    schedule_cron="0 * * * *",
    schedule_description=(
        "Hourly TEMPO NO2 discovery, exact-hour ingestion, and dbt publication. "
        "Controlled by TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED."
    ),
)

TEMPO_NO2_STD_SCOPE = ScopeSpec(
    source=SOURCE_TEMPO,
    scope=SCOPE_NO2_STD,
    label="TEMPO NO2 Standard",
    discovery_job_name="tempo_no2_std_granule_discovery",
    ingest_job_name="tempo_no2_std_hourly_ingest",
    dbt_job_name="tempo_no2_std_dbt_build",
    full_job_name="tempo_no2_std_full_pipeline",
    dbt_select="+tag:tempo,tag:no2_std",
    schedule_name="tempo_no2_std_pipeline_schedule",
    schedule_cron="30 * * * *",
    schedule_description=(
        "TEMPO NO2 standard (V04) discovery, exact-hour ingestion, and dbt "
        "publication. Runs on a wider lookback window than NRT because standard "
        "granules settle more slowly. Controlled by "
        "TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED."
    ),
)

SHIPPED_SCOPE_SPECS: tuple[ScopeSpec, ...] = (TEMPO_NO2_SCOPE, TEMPO_NO2_STD_SCOPE)

# Stable asset-key constants for assets, run configs, and tests.
TEMPO_NO2_OPS_REGION_REGISTRY = TEMPO_NO2_SCOPE.ops_region_registry_key
TEMPO_NO2_RAW_GRANULE_INVENTORY = TEMPO_NO2_SCOPE.raw_granule_inventory_key
TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES = TEMPO_NO2_SCOPE.raw_region_hour_aggregates_key
TEMPO_NO2_STD_OPS_REGION_REGISTRY = TEMPO_NO2_STD_SCOPE.ops_region_registry_key
TEMPO_NO2_STD_RAW_GRANULE_INVENTORY = TEMPO_NO2_STD_SCOPE.raw_granule_inventory_key
TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES = (
    TEMPO_NO2_STD_SCOPE.raw_region_hour_aggregates_key
)


def iter_scope_specs(*, source: str | None = None) -> tuple[ScopeSpec, ...]:
    if source is None:
        return SHIPPED_SCOPE_SPECS
    return tuple(spec for spec in SHIPPED_SCOPE_SPECS if spec.source == source)


def get_scope_spec(ref: str) -> ScopeSpec:
    ref = ref.strip()
    for spec in SHIPPED_SCOPE_SPECS:
        if ref in spec.aliases:
            return spec
    known = ", ".join(spec.key for spec in SHIPPED_SCOPE_SPECS)
    raise ValueError(f"Unknown scope {ref!r}; expected one of: {known}")


__all__ = [
    "SCOPE_STEPS",
    "SHIPPED_SCOPE_SPECS",
    "ScopeSpec",
    "ScopeStep",
    "TEMPO_NO2_OPS_REGION_REGISTRY",
    "TEMPO_NO2_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_RAW_REGION_HOUR_AGGREGATES",
    "TEMPO_NO2_SCOPE",
    "TEMPO_NO2_STD_OPS_REGION_REGISTRY",
    "TEMPO_NO2_STD_RAW_GRANULE_INVENTORY",
    "TEMPO_NO2_STD_RAW_REGION_HOUR_AGGREGATES",
    "TEMPO_NO2_STD_SCOPE",
    "get_scope_spec",
    "iter_scope_specs",
]
