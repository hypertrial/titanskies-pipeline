"""TEMPO NO2 ingestion and geography settings."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from titanskies_pipeline.config._env import (
    _env_bool,
    _env_int,
    _optional_env_str,
)
from titanskies_pipeline.config.settings_warehouse import BASE_DIR
from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD
from titanskies_pipeline.storage.duckdb.schemas.constants import hour_revision_sequence

TEMPO_NO2_CONTRACT_PATH = BASE_DIR / "dbt" / "seeds" / "tempo_no2_contract.csv"
TEMPO_NO2_STD_CONTRACT_PATH = BASE_DIR / "dbt" / "seeds" / "tempo_no2_std_contract.csv"


def load_tempo_no2_contract(path: Path = TEMPO_NO2_CONTRACT_PATH) -> dict[str, object]:
    required = {
        "contract_key",
        "contract_version",
        "min_region_coverage",
        "stale_hours_warn",
        "stale_hours_error",
        "anomaly_baseline_days",
        "anomaly_min_baseline_samples",
        "accepted_quality_flags",
    }
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(
                f"TEMPO contract missing columns: {', '.join(sorted(missing))}"
            )
        rows = [row for row in reader if row["contract_key"] == "default"]
    if len(rows) != 1:
        raise ValueError("TEMPO contract must contain exactly one default row")

    row = rows[0]
    try:
        min_coverage = float(row["min_region_coverage"])
        stale_warn = int(row["stale_hours_warn"])
        stale_error = int(row["stale_hours_error"])
        baseline_days = int(row["anomaly_baseline_days"])
        baseline_samples = int(row["anomaly_min_baseline_samples"])
        quality_flags = row["accepted_quality_flags"].strip()
        _parsed_flags = [int(value) for value in quality_flags.split("|")]
    except (TypeError, ValueError) as exc:
        raise ValueError("TEMPO contract contains invalid numeric values") from exc
    if not 0 <= min_coverage <= 1:
        raise ValueError("TEMPO min_region_coverage must be between 0 and 1")
    if (
        stale_warn < 1
        or stale_error <= stale_warn
        or baseline_days < 1
        or baseline_samples < 1
    ):
        raise ValueError("TEMPO contract time thresholds are invalid")
    return {
        "contract_version": row["contract_version"],
        "min_region_coverage": min_coverage,
        "stale_hours_warn": stale_warn,
        "stale_hours_error": stale_error,
        "anomaly_baseline_days": baseline_days,
        "anomaly_min_baseline_samples": baseline_samples,
        "accepted_quality_flags": quality_flags,
    }


@dataclass(frozen=True)
class TempoScopeSettings:
    cmr_concept_id: str
    discovery_lookback_hours: int
    raw_data_dir: Path
    raw_retention_days: int
    schedule_enabled: bool
    contract: dict[str, object]
    hour_revision_sequence: str


def _resolve_raw_data_dir(env_name: str, default_relative: str) -> Path:
    return (BASE_DIR / (_optional_env_str(env_name) or default_relative)).resolve()


def _build_nrt_scope_settings() -> TempoScopeSettings:
    return TempoScopeSettings(
        cmr_concept_id=_optional_env_str("TEMPO_NO2_CMR_CONCEPT_ID")
        or "C3685668637-LARC_CLOUD",
        discovery_lookback_hours=_env_int("TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS", 8),
        raw_data_dir=_resolve_raw_data_dir(
            "TEMPO_NO2_RAW_DATA_DIR", "data/raw/tempo_no2_nrt"
        ),
        raw_retention_days=_env_int("TEMPO_NO2_RAW_RETENTION_DAYS", 30),
        schedule_enabled=_env_bool("TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED", False),
        contract=load_tempo_no2_contract(TEMPO_NO2_CONTRACT_PATH),
        hour_revision_sequence=hour_revision_sequence(scope=SCOPE_NO2),
    )


def _build_std_scope_settings() -> TempoScopeSettings:
    return TempoScopeSettings(
        cmr_concept_id=_optional_env_str("TEMPO_NO2_STD_CMR_CONCEPT_ID")
        or "C3685896708-LARC_CLOUD",
        discovery_lookback_hours=_env_int("TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS", 24),
        raw_data_dir=_resolve_raw_data_dir(
            "TEMPO_NO2_STD_RAW_DATA_DIR", "data/raw/tempo_no2_std"
        ),
        raw_retention_days=_env_int("TEMPO_NO2_STD_RAW_RETENTION_DAYS", 30),
        schedule_enabled=_env_bool("TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED", False),
        contract=load_tempo_no2_contract(TEMPO_NO2_STD_CONTRACT_PATH),
        hour_revision_sequence=hour_revision_sequence(scope=SCOPE_NO2_STD),
    )


# Import-time constants for Dagster schedule import / `from settings import *`.
_IMPORT_NRT = _build_nrt_scope_settings()
TEMPO_NO2_CMR_CONCEPT_ID = _IMPORT_NRT.cmr_concept_id
TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS = _IMPORT_NRT.discovery_lookback_hours
TEMPO_NO2_RAW_DATA_DIR = _IMPORT_NRT.raw_data_dir
TEMPO_NO2_RAW_RETENTION_DAYS = _IMPORT_NRT.raw_retention_days
TEMPO_GEOGRAPHY_MANIFEST_PATH = (
    BASE_DIR
    / (
        _optional_env_str("TEMPO_GEOGRAPHY_MANIFEST_PATH")
        or "artifacts/geo/tempo_geography_artifacts.json"
    )
).resolve()
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED = _IMPORT_NRT.schedule_enabled
TEMPO_NO2_CONTRACT = _IMPORT_NRT.contract

_IMPORT_STD = _build_std_scope_settings()
TEMPO_NO2_STD_CMR_CONCEPT_ID = _IMPORT_STD.cmr_concept_id
TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS = _IMPORT_STD.discovery_lookback_hours
TEMPO_NO2_STD_RAW_DATA_DIR = _IMPORT_STD.raw_data_dir
TEMPO_NO2_STD_RAW_RETENTION_DAYS = _IMPORT_STD.raw_retention_days
TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED = _IMPORT_STD.schedule_enabled
TEMPO_NO2_STD_CONTRACT = _IMPORT_STD.contract


def get_tempo_scope_settings(scope: str) -> TempoScopeSettings:
    """Return scope settings from the current process environment and contracts."""
    if scope == SCOPE_NO2:
        return _build_nrt_scope_settings()
    if scope == SCOPE_NO2_STD:
        return _build_std_scope_settings()
    raise ValueError(
        f"Unknown TEMPO scope {scope!r}; expected {SCOPE_NO2!r} or {SCOPE_NO2_STD!r}"
    )


def resolve_geo_artifact_path(path: Path) -> Path:
    return path.expanduser().resolve()


__all__ = [
    "TEMPO_GEOGRAPHY_MANIFEST_PATH",
    "TEMPO_NO2_CMR_CONCEPT_ID",
    "TEMPO_NO2_CONTRACT",
    "TEMPO_NO2_CONTRACT_PATH",
    "TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS",
    "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED",
    "TEMPO_NO2_RAW_DATA_DIR",
    "TEMPO_NO2_RAW_RETENTION_DAYS",
    "TEMPO_NO2_STD_CMR_CONCEPT_ID",
    "TEMPO_NO2_STD_CONTRACT",
    "TEMPO_NO2_STD_CONTRACT_PATH",
    "TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS",
    "TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED",
    "TEMPO_NO2_STD_RAW_DATA_DIR",
    "TEMPO_NO2_STD_RAW_RETENTION_DAYS",
    "TempoScopeSettings",
    "get_tempo_scope_settings",
    "load_tempo_no2_contract",
    "resolve_geo_artifact_path",
]
