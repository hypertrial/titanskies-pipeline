"""RiverPulse operational settings and versioned scientific contract."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from titanskies_pipeline.config._env import (
    _env_bool,
    _env_float,
    _optional_env_str,
)
from titanskies_pipeline.config.settings_warehouse import BASE_DIR

RIVERPULSE_COLLECTION_NAME = "SWOT_L2_HR_RiverSP_reach_D"
RIVERPULSE_SWORD_VERSION = "17b"
RIVERPULSE_BACKFILL_START = datetime(2023, 8, 1, tzinfo=timezone.utc)
RIVERPULSE_HYDROCRON_URL = (
    "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
)
RIVERPULSE_CONTRACT_PATH = BASE_DIR / "dbt" / "seeds" / "riverpulse_events_contract.csv"


def load_riverpulse_contract(
    path: Path = RIVERPULSE_CONTRACT_PATH,
) -> dict[str, object]:
    required = {
        "contract_key",
        "contract_version",
        "field_contract_version",
        "collection_name",
        "collection_version",
        "sword_version",
        "accepted_reach_quality",
        "accepted_discharge_quality",
    }
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(
                "RiverPulse contract missing columns: " + ", ".join(sorted(missing))
            )
        rows = [row for row in reader if row["contract_key"] == "default"]
    if len(rows) != 1:
        raise ValueError("RiverPulse contract must contain exactly one default row")
    row = rows[0]
    try:
        reach_quality = int(row["accepted_reach_quality"])
        discharge_quality = int(row["accepted_discharge_quality"])
    except (TypeError, ValueError) as exc:
        raise ValueError("RiverPulse contract contains invalid quality values") from exc
    if row["collection_name"] != RIVERPULSE_COLLECTION_NAME:
        raise ValueError("RiverPulse contract collection_name is not Version D")
    if row["sword_version"] != RIVERPULSE_SWORD_VERSION:
        raise ValueError("RiverPulse contract sword_version must be 17b")
    return {
        "contract_version": row["contract_version"],
        "field_contract_version": row["field_contract_version"],
        "collection_name": row["collection_name"],
        "collection_version": row["collection_version"],
        "sword_version": row["sword_version"],
        "accepted_reach_quality": reach_quality,
        "accepted_discharge_quality": discharge_quality,
    }


@dataclass(frozen=True)
class RiverPulseSettings:
    raw_data_dir: Path
    network_manifest_path: Path
    hydrocron_api_key: str | None
    request_interval_seconds: float
    schedule_enabled: bool
    contract: dict[str, object]


def get_riverpulse_settings() -> RiverPulseSettings:
    raw_relative = (
        _optional_env_str("RIVERPULSE_RAW_DATA_DIR") or "data/raw/riverpulse_events"
    )
    manifest_relative = (
        _optional_env_str("RIVERPULSE_NETWORK_MANIFEST_PATH")
        or "artifacts/riverpulse/riverpulse_network_artifacts.json"
    )
    interval = _env_float("RIVERPULSE_REQUEST_INTERVAL_SECONDS", 1.0)
    if interval < 0:
        raise ValueError("RIVERPULSE_REQUEST_INTERVAL_SECONDS must be non-negative")
    return RiverPulseSettings(
        raw_data_dir=(BASE_DIR / raw_relative).resolve(),
        network_manifest_path=(BASE_DIR / manifest_relative).resolve(),
        hydrocron_api_key=_optional_env_str("RIVERPULSE_HYDROCRON_API_KEY"),
        request_interval_seconds=interval,
        schedule_enabled=_env_bool(
            "RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED", False
        ),
        contract=load_riverpulse_contract(),
    )


_IMPORT_SETTINGS = get_riverpulse_settings()
RIVERPULSE_RAW_DATA_DIR = _IMPORT_SETTINGS.raw_data_dir
RIVERPULSE_NETWORK_MANIFEST_PATH = _IMPORT_SETTINGS.network_manifest_path
RIVERPULSE_HYDROCRON_API_KEY = _IMPORT_SETTINGS.hydrocron_api_key
RIVERPULSE_REQUEST_INTERVAL_SECONDS = _IMPORT_SETTINGS.request_interval_seconds
RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED = _IMPORT_SETTINGS.schedule_enabled
RIVERPULSE_CONTRACT = _IMPORT_SETTINGS.contract

__all__ = [
    "RIVERPULSE_BACKFILL_START",
    "RIVERPULSE_COLLECTION_NAME",
    "RIVERPULSE_CONTRACT",
    "RIVERPULSE_CONTRACT_PATH",
    "RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED",
    "RIVERPULSE_HYDROCRON_API_KEY",
    "RIVERPULSE_HYDROCRON_URL",
    "RIVERPULSE_NETWORK_MANIFEST_PATH",
    "RIVERPULSE_RAW_DATA_DIR",
    "RIVERPULSE_REQUEST_INTERVAL_SECONDS",
    "RIVERPULSE_SWORD_VERSION",
    "RiverPulseSettings",
    "get_riverpulse_settings",
    "load_riverpulse_contract",
]
