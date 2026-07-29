"""PlumeGraph source, storage, and scientific-contract settings."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from titanskies_pipeline.config._env import (
    _env_bool,
    _env_int,
    _optional_env_str,
)
from titanskies_pipeline.config.settings_warehouse import BASE_DIR

PLUMEGRAPH_COLLECTION_NAME = "TEMPO_NO2_L2"
PLUMEGRAPH_COLLECTION_VERSION = "V04"
PLUMEGRAPH_CMR_CONCEPT_ID = "C3685896872-LARC_CLOUD"
PLUMEGRAPH_BENCHMARK_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
PLUMEGRAPH_BENCHMARK_END = datetime(2025, 1, 1, tzinfo=timezone.utc)
PLUMEGRAPH_EVIDENCE_FORMAT = "plumegraph-evidence-v1"
PLUMEGRAPH_CONTRACT_PATH = BASE_DIR / "dbt" / "seeds" / "plumegraph_events_contract.csv"
PLUMEGRAPH_SOURCE_MANIFEST_PATH = BASE_DIR / "config" / "plumegraph_sources.json"

_FLOAT_FIELDS = {
    "max_cloud_fraction",
    "aoi_radius_km",
    "seed_radius_km",
    "background_inner_km",
    "background_outer_km",
    "mad_multiplier",
    "uncertainty_multiplier",
    "tracking_advection_residual_km_max",
    "tracking_concentration_ratio_min",
    "dedup_jaccard_min",
    "lineage_temporal_overlap_min",
    "lineage_mean_iou_min",
    "trajectory_weight",
    "concurrent_emissions_weight",
    "distance_weight",
    "annual_emissions_weight",
    "distance_decay_km",
    "calibration_ece_max",
    "temperature_search_min",
    "temperature_search_max",
    "temperature_search_step",
    "likely_probability",
    "likely_margin",
    "plausible_probability",
    "ambiguous_margin",
    "detection_precision_min",
    "detection_recall_min",
    "source_top1_accuracy_min",
    "central_no2_nox_ratio",
    "background_uncertainty_fraction",
    "wind_uncertainty_fraction",
    "geometry_uncertainty_fraction",
}
_INT_FIELDS = {
    "quality_flag_good",
    "min_background_pixels",
    "min_component_pixels",
    "meteorology_max_bracket_minutes",
    "partition_overlap_hours",
    "max_tracking_gap_hours",
    "calibration_bins",
    "central_wind_level_m",
    "central_lifetime_hours",
}
_LIST_FIELDS = {
    "wind_levels_m": int,
    "chemical_lifetimes_hours": int,
    "no2_nox_ratios": float,
}


def load_plumegraph_contract(
    path: Path = PLUMEGRAPH_CONTRACT_PATH,
) -> dict[str, object]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [row for row in reader if row.get("contract_key") == "default"]
        fields = set(reader.fieldnames or ())
    required = {
        "contract_key",
        "contract_version",
        "algorithm_version",
        "collection_name",
        "collection_version",
        "concept_id",
        *_FLOAT_FIELDS,
        *_INT_FIELDS,
        *_LIST_FIELDS,
    }
    missing = required - fields
    if missing:
        raise ValueError(
            "PlumeGraph contract missing columns: " + ", ".join(sorted(missing))
        )
    if len(rows) != 1:
        raise ValueError("PlumeGraph contract must contain exactly one default row")
    row = rows[0]
    try:
        contract: dict[str, object] = dict(row)
        contract.update({name: float(row[name]) for name in _FLOAT_FIELDS})
        contract.update({name: int(row[name]) for name in _INT_FIELDS})
        for name, converter in _LIST_FIELDS.items():
            contract[name] = tuple(
                converter(value) for value in row[name].split("|") if value
            )
    except (TypeError, ValueError) as exc:
        raise ValueError("PlumeGraph contract contains invalid numeric values") from exc
    if (
        contract["collection_name"] != PLUMEGRAPH_COLLECTION_NAME
        or contract["collection_version"] != PLUMEGRAPH_COLLECTION_VERSION
        or contract["concept_id"] != PLUMEGRAPH_CMR_CONCEPT_ID
    ):
        raise ValueError("PlumeGraph contract does not match the pinned TEMPO source")
    if not 0 <= float(contract["max_cloud_fraction"]) <= 1:
        raise ValueError("PlumeGraph max_cloud_fraction must be between 0 and 1")
    if not 0 < float(contract["seed_radius_km"]) <= float(contract["aoi_radius_km"]):
        raise ValueError("PlumeGraph seed radius must be within the AOI")
    if (
        not 0
        <= float(contract["background_inner_km"])
        < float(contract["background_outer_km"])
        <= float(contract["aoi_radius_km"])
    ):
        raise ValueError("PlumeGraph background annulus is invalid")
    probabilities = (
        float(contract["tracking_concentration_ratio_min"]),
        float(contract["dedup_jaccard_min"]),
        float(contract["lineage_temporal_overlap_min"]),
        float(contract["lineage_mean_iou_min"]),
        float(contract["calibration_ece_max"]),
        float(contract["likely_probability"]),
        float(contract["likely_margin"]),
        float(contract["plausible_probability"]),
        float(contract["ambiguous_margin"]),
        float(contract["detection_precision_min"]),
        float(contract["detection_recall_min"]),
        float(contract["source_top1_accuracy_min"]),
    )
    if not all(0 <= value <= 1 for value in probabilities):
        raise ValueError("PlumeGraph probability thresholds must be in [0, 1]")
    weights = (
        float(contract["trajectory_weight"]),
        float(contract["concurrent_emissions_weight"]),
        float(contract["distance_weight"]),
        float(contract["annual_emissions_weight"]),
    )
    if any(value < 0 for value in weights) or not math.isclose(sum(weights), 1):
        raise ValueError(
            "PlumeGraph attribution weights must be nonnegative and sum to 1"
        )
    positive = (
        float(contract["tracking_advection_residual_km_max"]),
        float(contract["distance_decay_km"]),
        float(contract["temperature_search_min"]),
        float(contract["temperature_search_max"]),
        float(contract["temperature_search_step"]),
        int(contract["meteorology_max_bracket_minutes"]),
        int(contract["partition_overlap_hours"]),
        int(contract["max_tracking_gap_hours"]),
        int(contract["calibration_bins"]),
    )
    if any(value <= 0 for value in positive) or float(
        contract["temperature_search_min"]
    ) > float(contract["temperature_search_max"]):
        raise ValueError("PlumeGraph positive policy values are invalid")
    for name in _LIST_FIELDS:
        if not contract[name]:
            raise ValueError(f"PlumeGraph {name} must not be empty")
    return contract


@dataclass(frozen=True)
class PlumeGraphSettings:
    raw_data_dir: Path
    release_dir: Path
    cohort_manifest_path: Path
    discovery_lookback_days: int
    raw_cache_retention_days: int
    schedule_enabled: bool
    epa_api_key: str | None
    hrrr_store_url: str
    contract: dict[str, object]


def get_plumegraph_settings() -> PlumeGraphSettings:
    raw_relative = _optional_env_str("PLUMEGRAPH_RAW_DATA_DIR")
    release_relative = _optional_env_str("PLUMEGRAPH_RELEASE_DIR")
    cohort_relative = _optional_env_str("PLUMEGRAPH_COHORT_MANIFEST_PATH")
    return PlumeGraphSettings(
        raw_data_dir=(
            BASE_DIR / (raw_relative or "data/raw/plumegraph_events")
        ).resolve(),
        release_dir=(
            BASE_DIR / (release_relative or "data/releases/plumegraph_events")
        ).resolve(),
        cohort_manifest_path=(
            BASE_DIR
            / (cohort_relative or "artifacts/plumegraph/plumegraph_cohort.json")
        ).resolve(),
        discovery_lookback_days=_env_int("PLUMEGRAPH_DISCOVERY_LOOKBACK_DAYS", 14),
        raw_cache_retention_days=_env_int("PLUMEGRAPH_RAW_CACHE_RETENTION_DAYS", 30),
        schedule_enabled=_env_bool(
            "PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED", False
        ),
        epa_api_key=_optional_env_str("PLUMEGRAPH_EPA_API_KEY"),
        hrrr_store_url=_optional_env_str("PLUMEGRAPH_HRRR_STORE_URL")
        or "s3://hrrrzarr",
        contract=load_plumegraph_contract(),
    )


_IMPORT_SETTINGS = get_plumegraph_settings()
PLUMEGRAPH_RAW_DATA_DIR = _IMPORT_SETTINGS.raw_data_dir
PLUMEGRAPH_RELEASE_DIR = _IMPORT_SETTINGS.release_dir
PLUMEGRAPH_COHORT_MANIFEST_PATH = _IMPORT_SETTINGS.cohort_manifest_path
PLUMEGRAPH_DISCOVERY_LOOKBACK_DAYS = _IMPORT_SETTINGS.discovery_lookback_days
PLUMEGRAPH_RAW_CACHE_RETENTION_DAYS = _IMPORT_SETTINGS.raw_cache_retention_days
PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED = _IMPORT_SETTINGS.schedule_enabled
PLUMEGRAPH_HRRR_STORE_URL = _IMPORT_SETTINGS.hrrr_store_url
PLUMEGRAPH_CONTRACT = _IMPORT_SETTINGS.contract

__all__ = [
    "PLUMEGRAPH_BENCHMARK_END",
    "PLUMEGRAPH_BENCHMARK_START",
    "PLUMEGRAPH_CMR_CONCEPT_ID",
    "PLUMEGRAPH_COHORT_MANIFEST_PATH",
    "PLUMEGRAPH_COLLECTION_NAME",
    "PLUMEGRAPH_COLLECTION_VERSION",
    "PLUMEGRAPH_CONTRACT",
    "PLUMEGRAPH_CONTRACT_PATH",
    "PLUMEGRAPH_DISCOVERY_LOOKBACK_DAYS",
    "PLUMEGRAPH_EVIDENCE_FORMAT",
    "PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED",
    "PLUMEGRAPH_HRRR_STORE_URL",
    "PLUMEGRAPH_RAW_CACHE_RETENTION_DAYS",
    "PLUMEGRAPH_RAW_DATA_DIR",
    "PLUMEGRAPH_RELEASE_DIR",
    "PLUMEGRAPH_SOURCE_MANIFEST_PATH",
    "PlumeGraphSettings",
    "get_plumegraph_settings",
    "load_plumegraph_contract",
]
