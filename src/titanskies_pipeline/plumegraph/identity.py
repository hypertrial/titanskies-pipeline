"""Canonical identities and time conversions used by PlumeGraph."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

GPS_UTC_OFFSET_2024_SECONDS = 18


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    )


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"Cannot canonicalize {type(value).__name__}")


def sha256_identity(*parts: object) -> str:
    payload = "\x1f".join(str(part) for part in parts).encode()
    return hashlib.sha256(payload).hexdigest()


def pixel_identity(
    collection_concept_id: str,
    granule_id: str,
    mirror_step: int,
    xtrack: int,
    acquisition_time: datetime,
) -> str:
    return sha256_identity(
        collection_concept_id,
        granule_id,
        mirror_step,
        xtrack,
        normalize_utc(acquisition_time).isoformat(),
    )


def pixel_revision_identity(
    pixel_id: str,
    upstream_revision: str,
    snapshot_sha256: str,
    canonical_record: str,
) -> str:
    return sha256_identity(
        pixel_id,
        upstream_revision,
        snapshot_sha256,
        canonical_record,
    )


def emission_identity(
    facility_id: str,
    unit_id: str,
    operating_date: date,
    operating_hour: int,
) -> str:
    return sha256_identity(
        "camd",
        facility_id,
        unit_id,
        operating_date.isoformat(),
        operating_hour,
    )


def analysis_partition_identity(
    analysis_region_id: str,
    partition_date: date,
    contract_version: str,
    algorithm_version: str,
    input_manifest_hashes: list[str] | tuple[str, ...],
) -> str:
    return sha256_identity(
        analysis_region_id,
        partition_date.isoformat(),
        contract_version,
        algorithm_version,
        canonical_json(sorted(input_manifest_hashes)),
    )


def analysis_generation_manifest_identity(
    rows: Sequence[Sequence[object]],
) -> str:
    generations = sorted(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
        )
        for row in rows
    )
    return sha256_identity(canonical_json(generations))


def episode_revision_identity(
    analysis_run_id: str,
    candidate_ids: list[str] | tuple[str, ...],
    edge_ids: list[str] | tuple[str, ...],
    pixel_ids: list[str] | tuple[str, ...],
    scientific_result: object,
) -> str:
    return sha256_identity(
        analysis_run_id,
        canonical_json(sorted(candidate_ids)),
        canonical_json(sorted(edge_ids)),
        canonical_json(sorted(pixel_ids)),
        canonical_json(scientific_result),
    )


def normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc)


def gps_seconds_to_utc(
    seconds_since_1980: float,
    *,
    gps_utc_offset_seconds: int = GPS_UTC_OFFSET_2024_SECONDS,
) -> datetime:
    if not 0 <= seconds_since_1980 < 10_000_000_000:
        raise ValueError("GPS seconds are outside the supported range")
    gps_epoch = datetime(1980, 1, 6, tzinfo=timezone.utc)
    return gps_epoch + timedelta(seconds=seconds_since_1980 - gps_utc_offset_seconds)


def camd_local_standard_hour_to_utc(
    operating_date: date,
    operating_hour: int,
    *,
    timezone_name: str,
    utc_standard_offset_minutes: int,
) -> datetime:
    if not 0 <= operating_hour <= 23:
        raise ValueError("CAMD operating hour must be between 0 and 23")
    zone = ZoneInfo(timezone_name)
    january = datetime(2024, 1, 15, 12, tzinfo=zone)
    standard_offset = january.utcoffset()
    expected = timedelta(minutes=utc_standard_offset_minutes)
    if standard_offset != expected:
        raise ValueError(f"Configured standard offset does not match {timezone_name}")
    local_standard = datetime.combine(
        operating_date,
        datetime.min.time(),
        tzinfo=timezone(expected),
    ) + timedelta(hours=operating_hour)
    return local_standard.astimezone(timezone.utc)


__all__ = [
    "GPS_UTC_OFFSET_2024_SECONDS",
    "analysis_generation_manifest_identity",
    "analysis_partition_identity",
    "camd_local_standard_hour_to_utc",
    "canonical_json",
    "emission_identity",
    "episode_revision_identity",
    "gps_seconds_to_utc",
    "normalize_utc",
    "pixel_identity",
    "pixel_revision_identity",
    "sha256_identity",
]
