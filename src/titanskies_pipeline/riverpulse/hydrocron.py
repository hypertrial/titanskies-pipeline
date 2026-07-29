"""Version-pinned Hydrocron request, parsing, identity, and retry contract."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping, Sequence

import requests

from titanskies_pipeline.config.settings_riverpulse import (
    RIVERPULSE_COLLECTION_NAME,
    RIVERPULSE_HYDROCRON_URL,
)

CORE_FIELDS = (
    "reach_id",
    "time",
    "time_str",
    "p_lat",
    "p_lon",
    "river_name",
    "wse",
    "wse_u",
    "wse_r_u",
    "wse_c",
    "wse_c_u",
    "slope",
    "slope_u",
    "slope_r_u",
    "slope2",
    "slope2_u",
    "slope2_r_u",
    "width",
    "width_u",
    "width_c",
    "width_c_u",
)
DISCHARGE_CODES = (
    "c",
    "gc",
    "m",
    "gm",
    "b",
    "gb",
    "h",
    "gh",
    "o",
    "go",
    "s",
    "gs",
    "i",
    "gi",
)
DISCHARGE_FIELDS = tuple(
    field
    for code in DISCHARGE_CODES
    for field in (
        f"dschg_{code}",
        f"dschg_{code}_u",
        f"dschg_{code}sf",
        f"dschg_{code}_q",
    )
) + ("dschg_q_b", "dschg_gq_b")
QUALITY_AND_PROVENANCE_FIELDS = (
    "reach_q",
    "reach_q_b",
    "dark_frac",
    "ice_clim_f",
    "ice_dyn_f",
    "partial_f",
    "n_good_nod",
    "obs_frac_n",
    "xovr_cal_q",
    "n_reach_up",
    "n_reach_dn",
    "rch_id_up",
    "rch_id_dn",
    "p_dist_out",
    "p_length",
    "cycle_id",
    "pass_id",
    "continent_id",
    "range_start_time",
    "range_end_time",
    "crid",
    "sword_version",
    "collection_shortname",
    "collection_version",
    "granuleUR",
    "ingest_time",
)
HYDROCRON_FIELDS = tuple(
    dict.fromkeys(CORE_FIELDS + DISCHARGE_FIELDS + QUALITY_AND_PROVENANCE_FIELDS)
)
MAX_RESPONSE_BYTES = 6 * 1024 * 1024
_MISSING = {
    "",
    "nan",
    "null",
    "none",
    "no_data",
    "-9999",
    "-9999.0",
    "-999999999999",
}
_NO_DATA_PATTERNS = (
    "no data",
    "no results",
    "no observations",
    "does not exist for the requested time range",
)


def normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("RiverPulse timestamps must include a UTC offset")
    return value.astimezone(timezone.utc)


def parse_utc(value: object) -> datetime:
    text = str(value).strip()
    if not text:
        raise ValueError("Missing timestamp")
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return datetime(2000, 1, 1, tzinfo=timezone.utc) + timedelta(
            seconds=float(text)
        )
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return normalize_utc(parsed)


def _optional_time(value: object) -> datetime | None:
    return None if _is_missing(value) else parse_utc(value)


def _is_missing(value: object) -> bool:
    return value is None or str(value).strip().casefold() in _MISSING


def _number(value: object) -> float | None:
    if _is_missing(value):
        return None
    try:
        parsed = float(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"Invalid Hydrocron numeric value: {value!r}") from exc
    return parsed if math.isfinite(parsed) else None


def _integer(value: object, *, required: bool = False) -> int | None:
    number = _number(value)
    if number is None:
        if required:
            raise ValueError("Missing required Hydrocron integer")
        return None
    if not number.is_integer():
        raise ValueError(f"Hydrocron integer is not integral: {value!r}")
    return int(number)


def _text(value: object, *, required: bool = False) -> str | None:
    if _is_missing(value):
        if required:
            raise ValueError("Missing required Hydrocron text value")
        return None
    return str(value).strip()


def _canonical_row(row: Mapping[str, object]) -> str:
    normalized = {
        str(key): None if _is_missing(value) else str(value).strip()
        for key, value in row.items()
    }
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def stable_observation_id(
    *,
    collection_name: str,
    reach_id: str,
    observation_time: datetime,
    cycle_id: int,
    pass_id: int,
) -> str:
    collection_family = collection_name.removesuffix("_D")
    parts = (
        collection_family,
        reach_id,
        normalize_utc(observation_time).isoformat(),
        str(cycle_id),
        str(pass_id),
    )
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def revision_id(
    *, observation_id: str, crid: str, granule_id: str, canonical_record: str
) -> str:
    return hashlib.sha256(
        "\x1f".join((observation_id, crid, granule_id, canonical_record)).encode()
    ).hexdigest()


def request_id(
    *,
    collection_name: str,
    reach_id: str,
    window_start: datetime,
    window_end: datetime,
    field_contract_version: str,
) -> str:
    values = (
        "hydrocron",
        collection_name,
        reach_id,
        normalize_utc(window_start).isoformat(),
        normalize_utc(window_end).isoformat(),
        field_contract_version,
    )
    return hashlib.sha256("\x1f".join(values).encode()).hexdigest()


@dataclass(frozen=True)
class HydrocronRequest:
    request_id: str
    reach_id: str
    window_start: datetime
    window_end: datetime
    field_contract_version: str
    collection_name: str = RIVERPULSE_COLLECTION_NAME

    @classmethod
    def create(
        cls,
        *,
        reach_id: str,
        window_start: datetime,
        window_end: datetime,
        field_contract_version: str,
        collection_name: str = RIVERPULSE_COLLECTION_NAME,
    ) -> "HydrocronRequest":
        start = normalize_utc(window_start)
        end = normalize_utc(window_end)
        if start >= end:
            raise ValueError("Hydrocron request window must be non-empty")
        return cls(
            request_id=request_id(
                collection_name=collection_name,
                reach_id=reach_id,
                window_start=start,
                window_end=end,
                field_contract_version=field_contract_version,
            ),
            reach_id=reach_id,
            window_start=start,
            window_end=end,
            field_contract_version=field_contract_version,
            collection_name=collection_name,
        )

    def params(self) -> dict[str, str]:
        return {
            "collection_name": self.collection_name,
            "feature": "Reach",
            "feature_id": self.reach_id,
            "start_time": self.window_start.isoformat().replace("+00:00", "Z"),
            "end_time": self.window_end.isoformat().replace("+00:00", "Z"),
            "output": "csv",
            "fields": ",".join(HYDROCRON_FIELDS),
        }


@dataclass(frozen=True)
class ParsedObservation:
    values: dict[str, object]
    discharges: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    body: bytes
    attempts: int
    no_data: bool


class HydrocronFetchError(RuntimeError):
    def __init__(
        self, message: str, *, attempts: int, status_code: int | None = None
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.status_code = status_code


def parse_csv_response(
    body: bytes, *, collected_at: datetime
) -> list[ParsedObservation]:
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("Hydrocron response exceeds the 6 MB contract")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Hydrocron response is not UTF-8 CSV") from exc
    reader = csv.DictReader(io.StringIO(text))
    required = {
        "reach_id",
        "cycle_id",
        "pass_id",
        "crid",
        "sword_version",
        "collection_version",
        "granuleUR",
        "ingest_time",
    }
    if not reader.fieldnames or required - set(reader.fieldnames):
        raise ValueError("Hydrocron CSV is missing required identity fields")
    parsed: list[ParsedObservation] = []
    collected = normalize_utc(collected_at)
    for line_number, row in enumerate(reader, start=2):
        try:
            time_value = row.get("time_str")
            observation_time = parse_utc(
                row.get("time") if _is_missing(time_value) else time_value
            )
            reach_id = _text(row.get("reach_id"), required=True)
            cycle_id = _integer(row.get("cycle_id"), required=True)
            pass_id = _integer(row.get("pass_id"), required=True)
            collection_name = (
                _text(row.get("collection_shortname")) or RIVERPULSE_COLLECTION_NAME
            )
            collection_version = _text(row.get("collection_version"), required=True)
            crid = _text(row.get("crid"), required=True)
            sword_version = _text(row.get("sword_version"), required=True)
            granule_id = _text(row.get("granuleUR"), required=True)
            source_ingest_time = parse_utc(row.get("ingest_time"))
            canonical = _canonical_row(row)
            observation_id = stable_observation_id(
                collection_name=collection_name,
                reach_id=reach_id,
                observation_time=observation_time,
                cycle_id=cycle_id,
                pass_id=pass_id,
            )
            observation_revision_id = revision_id(
                observation_id=observation_id,
                crid=crid,
                granule_id=granule_id,
                canonical_record=canonical,
            )
            values: dict[str, object] = {
                "observation_revision_id": observation_revision_id,
                "observation_id": observation_id,
                "reach_id": reach_id,
                "observation_time": observation_time,
                "cycle_id": cycle_id,
                "pass_id": pass_id,
                "latitude": _number(row.get("p_lat")),
                "longitude": _number(row.get("p_lon")),
                "river_name": _text(row.get("river_name")),
                "wse": _number(row.get("wse")),
                "wse_u": _number(row.get("wse_u")),
                "wse_r_u": _number(row.get("wse_r_u")),
                "wse_c": _number(row.get("wse_c")),
                "wse_c_u": _number(row.get("wse_c_u")),
                "width": _number(row.get("width")),
                "width_u": _number(row.get("width_u")),
                "width_c": _number(row.get("width_c")),
                "width_c_u": _number(row.get("width_c_u")),
                "slope": _number(row.get("slope")),
                "slope_u": _number(row.get("slope_u")),
                "slope_r_u": _number(row.get("slope_r_u")),
                "slope2": _number(row.get("slope2")),
                "slope2_u": _number(row.get("slope2_u")),
                "slope2_r_u": _number(row.get("slope2_r_u")),
                "wse_unit": _text(row.get("wse_units")) or "m",
                "width_unit": _text(row.get("width_units")) or "m",
                "slope_unit": _text(row.get("slope_units")) or "m/m",
                "unconstrained_discharge_quality_bits": _integer(row.get("dschg_q_b")),
                "constrained_discharge_quality_bits": _integer(row.get("dschg_gq_b")),
                "reach_quality": _integer(row.get("reach_q")),
                "reach_quality_bits": _integer(row.get("reach_q_b")),
                "dark_fraction": _number(row.get("dark_frac")),
                "ice_climatology_flag": _integer(row.get("ice_clim_f")),
                "ice_dynamic_flag": _integer(row.get("ice_dyn_f")),
                "partial_flag": _integer(row.get("partial_f")),
                "good_node_count": _integer(row.get("n_good_nod")),
                "observed_node_fraction": _number(row.get("obs_frac_n")),
                "crossover_calibration_quality": _integer(row.get("xovr_cal_q")),
                "upstream_reach_count": _integer(row.get("n_reach_up")),
                "downstream_reach_count": _integer(row.get("n_reach_dn")),
                "upstream_reach_ids": _text(row.get("rch_id_up")),
                "downstream_reach_ids": _text(row.get("rch_id_dn")),
                "distance_to_outlet_m": _number(row.get("p_dist_out")),
                "reach_length_m": _number(row.get("p_length")),
                "continent_id": _text(row.get("continent_id")),
                "range_start_time": _optional_time(row.get("range_start_time")),
                "range_end_time": _optional_time(row.get("range_end_time")),
                "collection_name": collection_name,
                "collection_version": collection_version,
                "crid": crid,
                "sword_version": sword_version,
                "granule_id": granule_id,
                "source_ingest_time": source_ingest_time,
                "collected_at": collected,
                "canonical_record_json": canonical,
            }
            discharges = tuple(
                {
                    "observation_revision_id": observation_revision_id,
                    "algorithm": code.removeprefix("g"),
                    "is_constrained": code.startswith("g"),
                    "discharge_value": _number(row.get(f"dschg_{code}")),
                    "discharge_uncertainty": _number(row.get(f"dschg_{code}_u")),
                    "discharge_quality": _integer(row.get(f"dschg_{code}_q")),
                    "scale_factor": _number(row.get(f"dschg_{code}sf")),
                    "discharge_unit": _text(row.get(f"dschg_{code}_units")) or "m3/s",
                    "collection_name": collection_name,
                    "collection_version": collection_version,
                    "sword_version": sword_version,
                }
                for code in DISCHARGE_CODES
            )
        except ValueError as exc:
            raise ValueError(f"Invalid Hydrocron CSV row {line_number}: {exc}") from exc
        parsed.append(ParsedObservation(values, discharges))
    return parsed


def is_no_data_response(body: bytes) -> bool:
    text = body.decode("utf-8", errors="replace").casefold()
    return any(pattern in text for pattern in _NO_DATA_PATTERNS)


def _retry_after_seconds(
    response: requests.Response,
    fallback: float,
    *,
    now: datetime | None = None,
) -> float:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return fallback
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return fallback
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (retry_at - current).total_seconds())


def fetch_hydrocron(
    request: HydrocronRequest,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    client = session or requests.Session()
    headers = {"Accept": "text/csv"}
    if api_key:
        headers["x-hydrocron-key"] = api_key
    backoffs = (1.0, 2.0, 4.0)
    attempts = 0
    for retry_number in range(4):
        attempts += 1
        try:
            response = client.get(
                RIVERPULSE_HYDROCRON_URL,
                params=request.params(),
                headers=headers,
                timeout=(30, 120),
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            if retry_number == 3:
                raise HydrocronFetchError(
                    f"Hydrocron request exhausted retries: {type(exc).__name__}",
                    attempts=attempts,
                ) from exc
            sleep(backoffs[retry_number])
            continue
        body = response.content
        if len(body) > MAX_RESPONSE_BYTES:
            raise HydrocronFetchError(
                "Hydrocron response exceeds the 6 MB contract",
                attempts=attempts,
                status_code=response.status_code,
            )
        if response.status_code == 200:
            return FetchResult(200, body, attempts, False)
        if response.status_code == 400 and is_no_data_response(body):
            return FetchResult(400, body, attempts, True)
        if response.status_code == 429 or 500 <= response.status_code < 600:
            if retry_number < 3:
                sleep(_retry_after_seconds(response, backoffs[retry_number]))
                continue
        detail = body.decode("utf-8", errors="replace")[:500]
        raise HydrocronFetchError(
            f"Hydrocron HTTP {response.status_code}: {detail}",
            attempts=attempts,
            status_code=response.status_code,
        )
    raise AssertionError("unreachable")  # pragma: no cover - loop always returns/raises


def calendar_year_windows(
    start: datetime, end: datetime
) -> list[tuple[datetime, datetime]]:
    cursor = normalize_utc(start)
    end_utc = normalize_utc(end)
    if cursor >= end_utc:
        raise ValueError("RiverPulse discovery window must be non-empty")
    windows: list[tuple[datetime, datetime]] = []
    while cursor < end_utc:
        next_year = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        window_end = min(next_year, end_utc)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def sanitize_error(message: str, *, secrets: Sequence[str | None] = ()) -> str:
    sanitized = message
    for secret in secrets:
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    sanitized = re.sub(r"(https?://[^?\s]+)\?\S+", r"\1?[REDACTED]", sanitized)
    return sanitized[:2000]


__all__ = [
    "DISCHARGE_CODES",
    "HYDROCRON_FIELDS",
    "FetchResult",
    "HydrocronFetchError",
    "HydrocronRequest",
    "ParsedObservation",
    "calendar_year_windows",
    "fetch_hydrocron",
    "is_no_data_response",
    "parse_csv_response",
    "parse_utc",
    "request_id",
    "revision_id",
    "sanitize_error",
    "stable_observation_id",
]
