"""TEMPO NO2 ingestion and geography settings."""

from __future__ import annotations

import csv
from pathlib import Path

from titanskies_pipeline.config._env import (
    _env_bool,
    _env_int,
    _optional_env_str,
)
from titanskies_pipeline.config.settings_warehouse import BASE_DIR

TEMPO_NO2_CONTRACT_PATH = BASE_DIR / "dbt" / "seeds" / "tempo_no2_contract.csv"


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


TEMPO_NO2_CMR_CONCEPT_ID = _optional_env_str("TEMPO_NO2_CMR_CONCEPT_ID") or (
    "C3685668637-LARC_CLOUD"
)
TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS = _env_int("TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS", 8)
TEMPO_NO2_RAW_DATA_DIR = (
    BASE_DIR / (_optional_env_str("TEMPO_NO2_RAW_DATA_DIR") or "data/raw/tempo_no2_nrt")
).resolve()
TEMPO_NO2_RAW_RETENTION_DAYS = _env_int("TEMPO_NO2_RAW_RETENTION_DAYS", 30)
TEMPO_GEOGRAPHY_MANIFEST_PATH = (
    BASE_DIR
    / (
        _optional_env_str("TEMPO_GEOGRAPHY_MANIFEST_PATH")
        or "artifacts/geo/tempo_geography_artifacts.json"
    )
).resolve()
TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED = _env_bool(
    "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED", False
)
TEMPO_NO2_CONTRACT = load_tempo_no2_contract()


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
    "load_tempo_no2_contract",
    "resolve_geo_artifact_path",
]
