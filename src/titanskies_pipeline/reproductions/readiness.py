"""Resolve exact paper-source metadata into deterministic acquisition inventories."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import requests

from titanskies_pipeline.reproductions.preflight import load_profile

RESOLUTION_FORMAT = "reproduction-resolution-v1"
INVENTORY_FORMAT = "reproduction-source-inventory-v2"
RESOLUTION_OUTCOMES = frozenset(
    {
        "resolved",
        "operator_input_required",
        "transient_error",
        "definitively_unavailable",
        "not_required",
    }
)
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_MAX_SIGNED_BIGINT = 9_223_372_036_854_775_807
_SIGNED_QUERY_KEYS = frozenset(
    {
        "access_token",
        "access-token",
        "api_key",
        "api-key",
        "authorization",
        "credential",
        "sig",
        "signature",
        "token",
        "x-amz-credential",
        "x-amz-signature",
    }
)
_EPA_HOURLY_QUARTER_PATTERN = re.compile(
    r"^Emissions-Hourly-(?P<year>\d{4})-Q(?P<quarter>[1-4])\.csv$",
    re.IGNORECASE,
)


class _ProviderResolutionError(RuntimeError):
    """Provider metadata was incomplete without violating a local contract."""


@dataclass(frozen=True)
class ResolutionMetrics:
    profile_id: str
    status: str
    inventory_path: str
    inventory_sha256: str
    resolution_bundle_sha256: str
    source_count: int
    object_count: int
    resolved_source_count: int
    operator_input_required_count: int
    transient_error_count: int
    definitively_unavailable_count: int


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: str | bytes) -> str:
    payload = value.encode() if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path, *, label: str) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Cannot read {label} {path}: {exc}") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label.capitalize()} {path} must contain a JSON object")
    _assert_secret_safe(value)
    return value, _sha256(_canonical_json(value))


def _assert_secret_safe(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("_", "-")
            if any(
                marker in normalized for marker in _SIGNED_QUERY_KEYS
            ) and child not in (
                None,
                "",
                False,
            ):
                raise ValueError(f"Secret-bearing field is forbidden at {path}.{key}")
            _assert_secret_safe(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_secret_safe(child, path=f"{path}[{index}]")
    elif isinstance(value, str):
        parsed = urlsplit(value)
        if not parsed.scheme:
            return
        if parsed.username or parsed.password:
            raise ValueError(f"Credential-bearing URL is forbidden at {path}")
        query_keys = {key.lower() for key, _value in parse_qsl(parsed.query)}
        if query_keys & _SIGNED_QUERY_KEYS:
            raise ValueError(f"Signed or secret-bearing URL is forbidden at {path}")


def _unsigned_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Provider URL must be an absolute HTTP(S) URL: {url!r}")
    if parsed.username or parsed.password:
        raise ValueError("Provider URL must not contain credentials")
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _SIGNED_QUERY_KEYS
    ]
    if len(query) != len(parse_qsl(parsed.query, keep_blank_values=True)):
        raise ValueError("Signed provider URLs must not be persisted")
    query_string = urlencode(query)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query_string, ""))


def _parse_utc(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _validated_import_path(import_dir: Path, relative_name: str) -> Path:
    candidate = Path(relative_name)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("Import filenames must be relative and cannot escape")
    root = import_dir.resolve()
    path = (root / candidate).resolve()
    if path != root and root not in path.parents:
        raise ValueError("Import filename escapes the configured import directory")
    return path


def _validate_sun_cohort(raw: bytes) -> list[dict[str, str]]:
    try:
        rows = list(csv.DictReader(raw.decode().splitlines()))
    except UnicodeDecodeError as exc:
        raise ValueError("Sun et al. cohort must be UTF-8 CSV") from exc
    required_fields = {
        "camd_facility_id",
        "facility_name",
        "original_label",
        "latitude",
        "longitude",
        "analysis_half_width_km",
        "source_locator",
        "crosswalk_source",
    }
    if not rows or not required_fields.issubset(rows[0]):
        raise ValueError("Sun et al. cohort is missing required columns")
    if len(rows) != 14:
        raise ValueError("Sun et al. cohort must contain exactly 14 facilities")
    facility_ids = [row["camd_facility_id"] for row in rows]
    if len(facility_ids) != len(set(facility_ids)) or any(
        not facility_id.isdigit() for facility_id in facility_ids
    ):
        raise ValueError("Sun et al. cohort requires 14 unique CAMD facility IDs")
    for row in rows:
        latitude = float(row["latitude"])
        longitude = float(row["longitude"])
        half_width = float(row["analysis_half_width_km"])
        if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
            raise ValueError("Sun et al. cohort contains invalid coordinates")
        if half_width <= 0:
            raise ValueError("Sun et al. cohort AOI extent must be positive")
        if not all(
            row[field]
            for field in (
                "facility_name",
                "original_label",
                "source_locator",
                "crosswalk_source",
            )
        ):
            raise ValueError(
                "Sun et al. cohort requires labels, locators, and crosswalk evidence"
            )
    return rows


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: float,
    sleep: Callable[[float], None],
    **kwargs: Any,
) -> requests.Response:
    delays = (1.0, 2.0, 4.0)
    for attempt in range(4):  # pragma: no branch - every final attempt returns/raises
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 3:
                raise
        else:
            if response.status_code not in _RETRYABLE_STATUSES:
                response.raise_for_status()
                return response
            if attempt == 3:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    try:
                        retry_at = parsedate_to_datetime(retry_after)
                        delay = (
                            retry_at.astimezone(timezone.utc)
                            - datetime.now(timezone.utc)
                        ).total_seconds()
                    except (TypeError, ValueError, OverflowError):
                        delay = delays[attempt]
                sleep(max(0.0, delay))
                continue
        sleep(delays[attempt])


def _validate_evidence(
    profile_id: str, bundle: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    if bundle.get("format") != RESOLUTION_FORMAT:
        raise ValueError(f"Evidence format must be {RESOLUTION_FORMAT!r}")
    if bundle.get("profile_id") != profile_id:
        raise ValueError(f"Evidence profile_id must be {profile_id!r}")
    sources = bundle.get("sources")
    if not isinstance(sources, list):
        raise ValueError("Evidence sources must be a list")
    indexed: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("Every evidence source must be an object")
        source_id = str(source.get("source_id", ""))
        outcome = source.get("outcome")
        if not source_id or outcome not in RESOLUTION_OUTCOMES:
            raise ValueError("Evidence sources require a source_id and valid outcome")
        if source_id in indexed:
            raise ValueError(f"Evidence repeats source {source_id!r}")
        evidence = source.get("evidence", [])
        if not isinstance(evidence, list) or not evidence:
            raise ValueError(f"Evidence source {source_id!r} requires evidence records")
        for record in evidence:
            if not isinstance(record, dict):
                raise ValueError(f"Evidence for {source_id!r} must be an object")
            required = ("url", "retrieved_at", "sha256")
            if any(not record.get(field) for field in required):
                raise ValueError(
                    f"Evidence for {source_id!r} requires URL, time, and SHA-256"
                )
            _unsigned_url(str(record["url"]))
            _parse_utc(str(record["retrieved_at"]), label="evidence retrieved_at")
            checksum = str(record["sha256"])
            if len(checksum) != 64 or any(
                character not in "0123456789abcdef" for character in checksum
            ):
                raise ValueError(f"Evidence for {source_id!r} has invalid SHA-256")
        indexed[source_id] = source
    return indexed


def _normalized_bundle_for_identity(bundle: dict[str, Any]) -> dict[str, Any]:
    normalized = json.loads(json.dumps(bundle))
    for source in normalized["sources"]:
        source["evidence"] = sorted(
            source["evidence"],
            key=lambda record: (
                record["url"],
                record["retrieved_at"],
                record["sha256"],
            ),
        )
        if isinstance(source.get("objects"), list):
            source["objects"] = sorted(
                source["objects"],
                key=lambda item: (
                    str(item.get("provider_object_id", item.get("object_id", ""))),
                    str(item.get("provider_revision_id", "")),
                    str(item.get("object_id", "")),
                ),
            )
        if isinstance(source.get("repositories"), list):
            source["repositories"] = sorted(
                source["repositories"],
                key=lambda item: (
                    str(item.get("repository", "")),
                    str(item.get("commit_sha", "")),
                ),
            )
    normalized["sources"] = sorted(
        normalized["sources"], key=lambda source: source["source_id"]
    )
    return normalized


def _normalize_object(source_id: str, value: dict[str, Any]) -> dict[str, Any]:
    object_id = str(value.get("object_id", ""))
    url = str(value.get("url", ""))
    if not object_id or not url:
        raise ValueError(f"Resolved objects for {source_id!r} require ID and URL")
    size = value.get("size_bytes")
    upper = value.get("size_upper_bound_bytes")
    for field, amount in (("size_bytes", size), ("size_upper_bound_bytes", upper)):
        if amount is not None and (
            not isinstance(amount, int)
            or isinstance(amount, bool)
            or not 0 <= amount <= _MAX_SIGNED_BIGINT
        ):
            raise ValueError(f"{field} for {object_id!r} must fit BIGINT")
    if size is None and upper is None:
        raise ValueError(
            f"Resolved object {object_id!r} requires a size or upper bound"
        )
    if size is not None and upper is not None and upper < size:
        raise ValueError(f"Resolved object {object_id!r} has an invalid upper bound")
    provider_object_id = str(value.get("provider_object_id") or object_id)
    provider_revision_id = str(
        value.get("provider_revision_id")
        or value.get("source_revision")
        or value.get("etag")
        or value.get("checksum")
        or ""
    )
    return {
        **value,
        "object_id": object_id,
        "provider_object_id": provider_object_id,
        "provider_revision_id": provider_revision_id,
        "url": _unsigned_url(url),
        "size_bytes": size,
        "size_upper_bound_bytes": upper,
    }


def _objects_from_import(
    source_id: str,
    evidence: dict[str, Any],
    import_dir: Path,
) -> list[dict[str, Any]]:
    relative_name = evidence.get("import_filename")
    expected_sha256 = evidence.get("import_sha256")
    if not relative_name:
        raise FileNotFoundError("operator import is not configured")
    path = _validated_import_path(import_dir, str(relative_name))
    raw = path.read_bytes()
    actual_sha256 = _sha256(raw)
    if actual_sha256 != expected_sha256:
        raise ValueError(f"Import SHA-256 mismatch for {relative_name!r}")
    if path.suffix.lower() == ".csv":
        if source_id == "facility_cohort_14":
            _validate_sun_cohort(raw)
        object_value = {
            "object_id": path.name,
            "provider_object_id": f"operator-import:{path.name}",
            "provider_revision_id": evidence.get("source_revision") or actual_sha256,
            "url": str(evidence["canonical_url"]),
            "size_bytes": len(raw),
            "checksum_algorithm": "sha256",
            "checksum": actual_sha256,
            "source_revision": evidence.get("source_revision"),
            "source_extraction_sha256": evidence["evidence"][0]["sha256"],
        }
        return [_normalize_object(source_id, object_value)]
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("source_id") != source_id:
        raise ValueError(f"Import {relative_name!r} has the wrong source identity")
    if evidence.get("canonical_requests") != payload.get("canonical_requests"):
        raise ValueError(f"Import {relative_name!r} does not match canonical requests")
    objects = payload.get("objects")
    if not isinstance(objects, list):
        raise ValueError(f"Import {relative_name!r} must contain an objects list")
    return [_normalize_object(source_id, item) for item in objects]


def _cmr_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    endpoint = str(
        evidence.get(
            "catalog_url", "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
        )
    )
    request_contract = source["request"]
    page = 1
    expected_hits: int | None = None
    seen: set[tuple[str, str]] = set()
    resolved: list[dict[str, Any]] = []
    requested_start = _parse_utc(
        str(request_contract["start"]), label="CMR request start"
    )
    requested_end = _parse_utc(
        str(request_contract["end_exclusive"]), label="CMR request end"
    )
    cmr_end = (
        (requested_end - timedelta(microseconds=1)).isoformat().replace("+00:00", "Z")
    )
    while True:
        response = _request(
            session,
            "GET",
            endpoint,
            timeout=timeout,
            sleep=sleep,
            params={
                "collection_concept_id": source["concept_id"],
                "temporal": f"{request_contract['start']},{cmr_end}",
                "page_num": page,
                "page_size": 2000,
            },
            headers={"Accept": "application/json"},
        )
        if response.headers.get("CMR-Time-Out", "").lower() == "true":
            raise _ProviderResolutionError(
                f"CMR catalog for {source['id']!r} returned a timed-out partial page"
            )
        hits_header = response.headers.get("CMR-Hits")
        if hits_header is not None:
            try:
                page_hits = int(hits_header)
            except ValueError as exc:
                raise ValueError(
                    f"CMR catalog for {source['id']!r} has malformed CMR-Hits"
                ) from exc
            if page_hits < 0:
                raise ValueError(
                    f"CMR catalog for {source['id']!r} has malformed CMR-Hits"
                )
            if expected_hits is None:
                expected_hits = page_hits
            elif page_hits != expected_hits:
                raise _ProviderResolutionError(
                    f"CMR hit count changed during pagination for {source['id']!r}"
                )
        payload = response.json()
        items = payload.get("items")
        if not isinstance(items, list):
            raise ValueError(f"Malformed CMR catalog for {source['id']!r}")
        for item in items:
            meta = item.get("meta", {})
            umm = item.get("umm", {})
            concept_id = str(meta.get("concept-id", ""))
            revision = str(meta.get("revision-id", ""))
            producer_id = str(umm.get("GranuleUR", ""))
            if not concept_id or not revision or not producer_id:
                raise ValueError(f"CMR object for {source['id']!r} lacks identity")
            identity = (concept_id, revision)
            if identity in seen:
                continue
            seen.add(identity)
            collection_reference = umm.get("CollectionReference", {})
            provider_version = str(collection_reference.get("Version", ""))
            requested_version = str(source["version"]).split("/")[0].strip()
            if provider_version:
                normalized_provider = provider_version.lower().lstrip("v0")
                normalized_requested = requested_version.lower().lstrip("v0")
                if normalized_provider != normalized_requested:
                    raise ValueError(
                        f"CMR object {concept_id!r} has collection version "
                        f"{provider_version!r}, expected {requested_version!r}"
                    )
            range_time = umm.get("TemporalExtent", {}).get("RangeDateTime", {})
            beginning = range_time.get("BeginningDateTime")
            ending = range_time.get("EndingDateTime")
            if beginning and ending:
                granule_start = _parse_utc(str(beginning), label="CMR granule start")
                granule_end = _parse_utc(str(ending), label="CMR granule end")
                if granule_end <= requested_start or granule_start >= requested_end:
                    raise ValueError(
                        f"CMR object {concept_id!r} falls outside the request window"
                    )
            urls = [
                entry.get("URL")
                for entry in umm.get("RelatedUrls", [])
                if entry.get("Type") == "GET DATA"
                and str(entry.get("URL", "")).startswith("https://")
            ]
            if not urls:
                raise ValueError(f"CMR object {concept_id!r} lacks a canonical URL")
            canonical_url = sorted(set(urls))[0]
            provider_filename = Path(urlsplit(canonical_url).path).name
            granule = umm.get("DataGranule", {})
            size = granule.get("SizeInBytes")
            upper = None
            if size is None and granule.get("SizeMB") is not None:
                upper = int(float(granule["SizeMB"]) * 1024 * 1024 + 0.999999)
            archive_entries = granule.get("ArchiveAndDistributionInformation", [])
            matching_archives = [
                entry
                for entry in archive_entries
                if entry.get("Name") in {producer_id, provider_filename}
            ]
            archive = max(
                matching_archives,
                key=lambda entry: (
                    entry.get("Size") is not None,
                    bool(entry.get("SizeUnit")),
                    bool(entry.get("Checksum")),
                ),
                default=None,
            )
            if size is None and archive and archive.get("SizeInBytes") is not None:
                size = int(archive["SizeInBytes"])
            if size is None and upper is None and archive:
                unit_multipliers = {
                    "B": 1,
                    "BYTE": 1,
                    "BYTES": 1,
                    "KB": 1024,
                    "MB": 1024**2,
                    "GB": 1024**3,
                    "TB": 1024**4,
                }
                size_unit = str(archive.get("SizeUnit", "")).upper()
                multiplier = unit_multipliers.get(size_unit)
                if multiplier is None:
                    raise ValueError(
                        f"CMR object {concept_id!r} has unsupported size unit "
                        f"{size_unit!r}"
                    )
                upper = math.ceil(float(archive["Size"]) * multiplier)
            if size is None and upper is None:
                upper = evidence.get("size_upper_bound_bytes")
            checksum = (archive or {}).get("Checksum") or granule.get("Checksum") or {}
            resolved.append(
                _normalize_object(
                    str(source["id"]),
                    {
                        "object_id": producer_id,
                        "provider_object_id": concept_id,
                        "provider_revision_id": revision,
                        "provider_filename": provider_filename,
                        "url": canonical_url,
                        "size_bytes": int(size) if size is not None else None,
                        "size_upper_bound_bytes": upper,
                        "checksum_algorithm": checksum.get("Algorithm"),
                        "checksum": checksum.get("Value"),
                        "source_revision": meta.get("revision-date"),
                        "schema_fingerprint": _sha256(
                            _canonical_json(
                                {
                                    "collection": source["concept_id"],
                                    "format": meta.get("format"),
                                    "short_name": collection_reference.get("ShortName"),
                                    "version": provider_version,
                                }
                            )
                        ),
                    },
                )
            )
        if expected_hits is not None:
            if len(seen) > expected_hits:
                raise ValueError(
                    f"CMR catalog for {source['id']!r} exceeded its reported hit count"
                )
            if len(seen) == expected_hits:
                break
            if len(items) < 2000:
                raise _ProviderResolutionError(
                    f"CMR pagination ended after {len(seen)} of {expected_hits} "
                    f"reported objects for {source['id']!r}"
                )
        elif len(items) < 2000:
            break
        page += 1
    return resolved


def _static_archive_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    requested_filename = source.get("request", {}).get("archive_filename")
    record_id = evidence.get("zenodo_record_id") or source.get("request", {}).get(
        "zenodo_record_id"
    )
    if record_id:
        response = _request(
            session,
            "GET",
            str(
                evidence.get(
                    "catalog_url", f"https://zenodo.org/api/records/{record_id}"
                )
            ),
            timeout=timeout,
            sleep=sleep,
        )
        payload = response.json()
        candidates = payload.get("files")
        if not isinstance(candidates, list):
            raise ValueError(f"Malformed Zenodo metadata for {source['id']!r}")
        matches = [
            item
            for item in candidates
            if not requested_filename or item.get("key") == requested_filename
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Zenodo metadata must resolve exactly one file for {source['id']!r}"
            )
        item = matches[0]
        checksum_text = str(item.get("checksum", ""))
        algorithm, _, checksum = checksum_text.partition(":")
        return [
            _normalize_object(
                str(source["id"]),
                {
                    "object_id": str(item["key"]),
                    "provider_object_id": f"zenodo:{record_id}:{item['id']}",
                    "provider_revision_id": str(payload.get("revision", record_id)),
                    "url": item["links"]["self"],
                    "size_bytes": int(item["size"]),
                    "checksum_algorithm": algorithm or None,
                    "checksum": checksum or None,
                    "source_revision": payload.get("updated"),
                },
            )
        ]
    response = _request(
        session,
        "HEAD",
        str(evidence.get("catalog_url", source["url"])),
        timeout=timeout,
        sleep=sleep,
        allow_redirects=True,
    )
    size_header = response.headers.get("Content-Length")
    return [
        _normalize_object(
            str(source["id"]),
            {
                "object_id": requested_filename
                or Path(urlsplit(source["url"]).path).name,
                "provider_object_id": evidence.get("provider_object_id"),
                "provider_revision_id": evidence.get("provider_revision_id"),
                "url": source["url"],
                "size_bytes": int(size_header) if size_header else None,
                "size_upper_bound_bytes": evidence.get("size_upper_bound_bytes"),
                "checksum_algorithm": source.get("checksum_algorithm"),
                "checksum": source.get("checksum"),
                "etag": response.headers.get("ETag"),
                "source_revision": response.headers.get("Last-Modified"),
            },
        )
    ]


def _catalog_payload(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    response = _request(
        session,
        "GET",
        str(evidence["catalog_url"]),
        timeout=timeout,
        sleep=sleep,
        headers={"x-api-key": os.environ["PLUMEGRAPH_EPA_API_KEY"]}
        if source["access_method"] == "epa_api"
        and os.environ.get("PLUMEGRAPH_EPA_API_KEY")
        else None,
    )
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Malformed provider catalog for {source['id']!r}")
    return payload


def _epa_period(item: dict[str, Any]) -> tuple[int, int] | None:
    year = item.get("year")
    quarter = item.get("quarter")
    if year is not None and quarter is not None:
        return int(year), int(quarter)
    match = _EPA_HOURLY_QUARTER_PATTERN.fullmatch(
        str(item.get("filename") or item.get("object_id") or "")
    )
    if match is None:
        return None
    return int(match.group("year")), int(match.group("quarter"))


def _validate_epa_coverage(
    source: dict[str, Any], objects: list[dict[str, Any]]
) -> None:
    requested_years = {int(year) for year in source["request"]["years"]}
    expected_periods = {
        (year, quarter) for year in requested_years for quarter in range(1, 5)
    }
    periods = [_epa_period(item) for item in objects]
    actual_periods = {period for period in periods if period in expected_periods}
    if len(actual_periods) != len([period for period in periods if period is not None]):
        raise ValueError("EPA catalog repeats or includes unexpected hourly quarters")
    if actual_periods != expected_periods:
        missing = sorted(expected_periods - actual_periods)
        raise ValueError(f"EPA catalog is missing requested quarters: {missing}")


def _epa_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    payload = _catalog_payload(source, evidence, session, timeout=timeout, sleep=sleep)
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError(f"Malformed provider catalog for {source['id']!r}")
    requested_years = {int(year) for year in source["request"]["years"]}
    download_base_url = _unsigned_url(
        str(evidence.get("download_base_url", "https://api.epa.gov/easey/bulk-files"))
    ).rstrip("/")
    resolved: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"Malformed provider catalog for {source['id']!r}")
        period = _epa_period(item)
        if period is None or period[0] not in requested_years:
            continue
        filename = str(item.get("filename", ""))
        s3_path = str(item.get("s3Path", ""))
        revision = str(item.get("lastUpdated", ""))
        metadata = item.get("metadata")
        if (
            not filename
            or not s3_path
            or not revision
            or not isinstance(metadata, dict)
        ):
            raise ValueError("EPA bulk-file metadata is missing identity or revision")
        _parse_utc(revision, label="EPA bulk-file revision")
        expected_metadata = {
            "datatype": "emissions",
            "datasubtype": "hourly",
            "grouping": "quarterly",
        }
        normalized_metadata = {
            str(key).replace("_", "").lower(): str(value).lower()
            for key, value in metadata.items()
        }
        for key, expected in expected_metadata.items():
            if key in normalized_metadata and normalized_metadata[key] != expected:
                raise ValueError(
                    f"EPA bulk-file {filename!r} has inconsistent {key} metadata"
                )
        resolved.append(
            _normalize_object(
                str(source["id"]),
                {
                    "object_id": filename,
                    "provider_object_id": s3_path,
                    "provider_revision_id": revision,
                    "url": f"{download_base_url}/{quote(s3_path.lstrip('/'), safe='/')}",
                    "size_bytes": item.get("bytes"),
                    "source_revision": revision,
                    "etag": item.get("etag") or metadata.get("etag"),
                    "schema_fingerprint": _sha256(
                        _canonical_json(
                            {
                                "catalog_fields": sorted(item),
                                "metadata_fields": sorted(metadata),
                            }
                        )
                    ),
                    "year": period[0],
                    "quarter": period[1],
                },
            )
        )
    _validate_epa_coverage(source, resolved)
    return resolved


def _opendap_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    payload = _catalog_payload(source, evidence, session, timeout=timeout, sleep=sleep)
    request = source["request"]
    if payload.get("dataset_family") != request["dataset_family"]:
        raise ValueError("GEOS-CF provider catalog is not the pinned v1 family")
    variables = set(payload.get("variables", []))
    if not set(request["variables"]).issubset(variables):
        raise ValueError("GEOS-CF provider catalog lacks required variables")
    if int(payload.get("year", -1)) != int(request["year"]):
        raise ValueError("GEOS-CF provider catalog lacks the required year")
    dimensions = payload.get("dimensions")
    dtypes = payload.get("dtypes")
    revision = payload.get("revision")
    if not isinstance(dimensions, dict) or not isinstance(dtypes, dict) or not revision:
        raise ValueError("GEOS-CF provider catalog lacks schema/revision evidence")
    items = payload.get("objects")
    if not isinstance(items, list):
        raise ValueError(f"Malformed provider catalog for {source['id']!r}")
    schema_fingerprint = _sha256(
        _canonical_json(
            {
                "dataset_family": payload["dataset_family"],
                "dimensions": dimensions,
                "dtypes": dtypes,
                "variables": sorted(variables),
            }
        )
    )
    return [
        _normalize_object(
            str(source["id"]),
            {
                **item,
                "provider_revision_id": item.get("provider_revision_id") or revision,
                "schema_fingerprint": item.get("schema_fingerprint")
                or schema_fingerprint,
            },
        )
        for item in items
    ]


def _git_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    repositories = evidence.get("repositories")
    if not isinstance(repositories, list) or not repositories:
        raise ValueError("Resolved Git evidence requires repositories")
    cutoff = _parse_utc(
        str(source["request"]["paper_access_cutoff"]), label="paper access cutoff"
    )
    objects: list[dict[str, Any]] = []
    for repository in repositories:
        name = str(repository.get("repository", ""))
        sha = str(repository.get("commit_sha", ""))
        if not name or len(sha) != 40:
            raise ValueError("Git evidence requires repository and full commit SHA")
        response = _request(
            session,
            "GET",
            str(
                repository.get(
                    "metadata_url", f"https://api.github.com/repos/{name}/commits/{sha}"
                )
            ),
            timeout=timeout,
            sleep=sleep,
            headers={"Accept": "application/vnd.github+json"},
        )
        payload = response.json()
        if payload.get("sha") != sha:
            raise ValueError(f"Git provider returned the wrong commit for {name!r}")
        timestamp = payload.get("commit", {}).get("committer", {}).get("date")
        if not timestamp or _parse_utc(timestamp, label="Git commit time") > cutoff:
            raise ValueError(f"Git commit for {name!r} violates the paper cutoff")
        licence = repository.get("license")
        if not licence:
            raise ValueError(f"Git evidence for {name!r} requires a licence")
        objects.append(
            _normalize_object(
                str(source["id"]),
                {
                    "object_id": f"{name}@{sha}",
                    "provider_object_id": name,
                    "provider_revision_id": sha,
                    "url": f"https://github.com/{name}/archive/{sha}.tar.gz",
                    "size_bytes": repository.get("size_bytes"),
                    "size_upper_bound_bytes": repository.get("size_upper_bound_bytes"),
                    "source_revision": timestamp,
                    "license": licence,
                },
            )
        )
    return objects


def _resolve_objects(
    source: dict[str, Any],
    evidence: dict[str, Any],
    import_dir: Path,
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    method = source["access_method"]
    if evidence.get("objects") is not None:
        if not isinstance(evidence["objects"], list):
            raise ValueError(f"Evidence objects for {source['id']!r} must be a list")
        objects = [
            _normalize_object(str(source["id"]), item) for item in evidence["objects"]
        ]
        if method == "epa_api":
            _validate_epa_coverage(source, objects)
        if method == "opendap":
            expected_family = source["request"]["dataset_family"]
            if any(
                item.get("dataset_family") != expected_family
                or not item.get("schema_fingerprint")
                for item in objects
            ):
                raise ValueError(
                    "GEOS-CF evidence objects require the v1 family and schema"
                )
        return objects
    if method == "cmr":
        return _cmr_objects(source, evidence, session, timeout=timeout, sleep=sleep)
    if method == "static_archive":
        return _static_archive_objects(
            source, evidence, session, timeout=timeout, sleep=sleep
        )
    if method == "epa_api":
        return _epa_objects(source, evidence, session, timeout=timeout, sleep=sleep)
    if method == "opendap":
        return _opendap_objects(source, evidence, session, timeout=timeout, sleep=sleep)
    if method == "git_revision":
        return _git_objects(source, evidence, session, timeout=timeout, sleep=sleep)
    return _objects_from_import(str(source["id"]), evidence, import_dir)


def _resolve_source(
    source: dict[str, Any],
    evidence: dict[str, Any],
    import_dir: Path,
    session: requests.Session,
    *,
    timeout: float,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    source_id = str(source["id"])
    outcome = str(evidence["outcome"])
    base = {
        "source_id": source_id,
        "resolution_outcome": outcome,
        "evidence": sorted(
            evidence["evidence"],
            key=lambda record: (
                record["url"],
                record["retrieved_at"],
                record["sha256"],
            ),
        ),
        "evidence_summary": evidence.get("evidence_summary", {}),
        "exactness_status": "exact" if outcome == "resolved" else "unavailable",
        "reason": evidence.get("reason"),
        "objects": [],
    }
    if outcome != "resolved":
        return base
    if (
        source["access_method"] == "epa_api"
        and "objects" not in evidence
        and "import_filename" not in evidence
        and not os.environ.get("PLUMEGRAPH_EPA_API_KEY")
    ):
        return {
            **base,
            "resolution_outcome": "operator_input_required",
            "reason": (
                "PLUMEGRAPH_EPA_API_KEY is required to enumerate the CAMD bulk catalog"
            ),
        }
    if source["access_method"] == "cds_api" and "canonical_requests" not in evidence:
        cohort_name = source.get("request", {}).get("facility_cohort")
        if cohort_name:
            try:
                evidence = {
                    **evidence,
                    "canonical_requests": canonical_era5_requests(
                        source,
                        _validated_import_path(import_dir, str(cohort_name)),
                    ),
                }
            except FileNotFoundError:
                return {
                    **base,
                    "resolution_outcome": "operator_input_required",
                    "reason": "the normalized cohort import is required for CDS requests",
                }
    try:
        objects = _resolve_objects(
            source,
            evidence,
            import_dir,
            session,
            timeout=timeout,
            sleep=sleep,
        )
    except FileNotFoundError as exc:
        return {
            **base,
            "resolution_outcome": "operator_input_required",
            "reason": str(exc),
        }
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        return {
            **base,
            "resolution_outcome": (
                "operator_input_required" if status in {401, 403} else "transient_error"
            ),
            "reason": (
                "provider metadata authorization failed"
                if status in {401, 403}
                else f"provider metadata request failed: HTTP {status}"
            ),
        }
    except _ProviderResolutionError as exc:
        return {
            **base,
            "resolution_outcome": "transient_error",
            "reason": str(exc),
        }
    except requests.exceptions.InvalidJSONError:
        raise
    except (requests.Timeout, requests.ConnectionError, TimeoutError) as exc:
        return {
            **base,
            "resolution_outcome": "transient_error",
            "reason": f"provider metadata request failed: {type(exc).__name__}",
        }
    if not objects:
        return {
            **base,
            "resolution_outcome": "transient_error",
            "reason": "provider returned no exact objects",
        }
    object_ids = [item["object_id"] for item in objects]
    if len(object_ids) != len(set(object_ids)):
        raise ValueError(f"Provider returned conflicting identities for {source_id!r}")
    provider_identities = [
        (
            str(item.get("provider_object_id", item["object_id"])),
            str(item.get("provider_revision_id", "")),
        )
        for item in objects
    ]
    if len(provider_identities) != len(set(provider_identities)):
        raise ValueError(
            f"Provider returned conflicting provider revisions for {source_id!r}"
        )
    cutoff_value = source.get("request", {}).get("paper_access_cutoff")
    if cutoff_value:
        cutoff = _parse_utc(str(cutoff_value), label="paper access cutoff")
        for item in objects:
            revision = item.get("source_revision")
            if not revision:
                raise ValueError(
                    f"Resolved object {item['object_id']!r} requires an authoritative "
                    "revision time"
                )
            if _parse_utc(str(revision), label="source revision") > cutoff:
                raise ValueError(
                    f"Resolved object {item['object_id']!r} violates the paper cutoff"
                )
    missing_provider_identity = [
        item["object_id"]
        for item in objects
        if not item.get("provider_object_id") or not item.get("provider_revision_id")
    ]
    if missing_provider_identity:
        raise ValueError(
            "Resolved provider objects require immutable revision identities: "
            + ", ".join(sorted(missing_provider_identity))
        )
    return {
        **base,
        "objects": sorted(
            objects,
            key=lambda item: (
                str(item.get("provider_object_id", item["object_id"])),
                str(item.get("provider_revision_id", "")),
                item["object_id"],
            ),
        ),
    }


def resolve_reproduction_sources(
    profile_id: str,
    *,
    evidence_path: Path,
    import_dir: Path,
    output_path: Path,
    manifest_path: Path | None = None,
    timeout_seconds: float = 30.0,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ResolutionMetrics:
    """Resolve provider metadata and atomically publish a canonical inventory."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    manifest, _manifest_hash, _contract_hash = load_profile(
        profile_id, manifest_path=manifest_path
    )
    evidence_bundle, _raw_evidence_sha256 = _read_json(
        evidence_path.resolve(), label="resolution evidence"
    )
    evidence = _validate_evidence(profile_id, evidence_bundle)
    evidence_sha256 = _sha256(
        _canonical_json(_normalized_bundle_for_identity(evidence_bundle))
    )
    contracts = {str(source["id"]): source for source in manifest["sources"]}
    unknown_sources = sorted(set(evidence) - set(contracts))
    if unknown_sources:
        raise ValueError(
            f"Evidence contains unknown sources: {', '.join(unknown_sources)}"
        )
    owns_session = session is None
    http = session or requests.Session()
    try:
        resolved_sources: list[dict[str, Any]] = []
        for source_id in sorted(contracts):
            source_evidence = evidence.get(source_id)
            if source_evidence is None:
                source_evidence = {
                    "source_id": source_id,
                    "outcome": "operator_input_required",
                    "reason": "technical evidence is missing",
                    "evidence": [
                        {
                            "url": contracts[source_id]["url"],
                            "retrieved_at": "1970-01-01T00:00:00Z",
                            "sha256": "0" * 64,
                        }
                    ],
                }
            resolved_sources.append(
                _resolve_source(
                    contracts[source_id],
                    source_evidence,
                    import_dir,
                    http,
                    timeout=timeout_seconds,
                    sleep=sleep,
                )
            )
    finally:
        if owns_session:
            http.close()
    inventory = {
        "inventory_format": INVENTORY_FORMAT,
        "inventory_mode": "production",
        "profile_id": profile_id,
        "resolution_format": RESOLUTION_FORMAT,
        "resolution_bundle_sha256": evidence_sha256,
        "sources": resolved_sources,
    }
    encoded = (_canonical_json(inventory) + "\n").encode()
    output = output_path.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=output.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    outcome_counts = {
        outcome: sum(
            source["resolution_outcome"] == outcome for source in resolved_sources
        )
        for outcome in RESOLUTION_OUTCOMES
    }
    status = (
        "transient"
        if outcome_counts["transient_error"]
        or outcome_counts["operator_input_required"]
        else "complete"
    )
    return ResolutionMetrics(
        profile_id=profile_id,
        status=status,
        inventory_path=str(output),
        inventory_sha256=_sha256(_canonical_json(inventory)),
        resolution_bundle_sha256=evidence_sha256,
        source_count=len(resolved_sources),
        object_count=sum(len(source["objects"]) for source in resolved_sources),
        resolved_source_count=outcome_counts["resolved"],
        operator_input_required_count=outcome_counts["operator_input_required"],
        transient_error_count=outcome_counts["transient_error"],
        definitively_unavailable_count=outcome_counts["definitively_unavailable"],
    )


def canonical_era5_requests(
    source: dict[str, Any],
    facilities_path: Path,
) -> list[dict[str, Any]]:
    """Build deterministic dataset × facility × calendar-month CDS requests."""
    request = source["request"]
    start = _parse_utc(str(request["start"]), label="ERA5 start")
    end = _parse_utc(str(request["end_exclusive"]), label="ERA5 end")
    facilities = _validate_sun_cohort(facilities_path.read_bytes())
    result: list[dict[str, Any]] = []
    month = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
    while month < end:
        next_month = (
            datetime(month.year + 1, 1, 1, tzinfo=timezone.utc)
            if month.month == 12
            else datetime(month.year, month.month + 1, 1, tzinfo=timezone.utc)
        )
        window_start = max(start, month)
        window_end = min(end, next_month)
        for facility in sorted(facilities, key=lambda row: row["camd_facility_id"]):
            latitude = float(facility["latitude"])
            longitude = float(facility["longitude"])
            half_width = float(facility["analysis_half_width_km"])
            latitude_delta = half_width / 111.0
            longitude_delta = half_width / (
                111.0 * max(0.01, abs(math.cos(math.radians(latitude))))
            )
            result.append(
                {
                    "dataset": request["dataset"],
                    "facility_id": facility["camd_facility_id"],
                    "window_start": window_start.isoformat().replace("+00:00", "Z"),
                    "window_end_exclusive": window_end.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "area": [
                        round(latitude + latitude_delta, 6),
                        round(longitude - longitude_delta, 6),
                        round(latitude - latitude_delta, 6),
                        round(longitude + longitude_delta, 6),
                    ],
                    "variables": sorted(request["variables"]),
                    "pressure_levels_hpa": sorted(
                        request.get("pressure_levels_hpa", [])
                    ),
                    "hours": [f"{hour:02d}:00" for hour in range(24)],
                }
            )
        month = next_month
    return result


__all__ = [
    "INVENTORY_FORMAT",
    "RESOLUTION_FORMAT",
    "RESOLUTION_OUTCOMES",
    "ResolutionMetrics",
    "canonical_era5_requests",
    "resolve_reproduction_sources",
]
