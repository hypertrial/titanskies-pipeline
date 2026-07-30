"""Validate and persist paper-reproduction source inventories without downloads."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import duckdb
import pyarrow as pa

from titanskies_pipeline.config.settings import BASE_DIR
from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.storage.duckdb.connection import _use_conn
from titanskies_pipeline.storage.duckdb.schemas.constants import reproduction_ops_tbl

EXACTNESS_STATUSES = frozenset(
    {"exact", "provider_reprocessed", "method_equivalent", "unavailable"}
)
PROFILE_MANIFESTS = {
    SOURCE_SUN2025: BASE_DIR / "config" / "reproductions" / "sun2025_sources.json",
    SOURCE_ANDREADIS2025: BASE_DIR
    / "config"
    / "reproductions"
    / "andreadis2025_sources.json",
}
_SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "password",
        "secret",
        "signed_url",
        "sig",
        "token",
        "x_amz_credential",
        "x_amz_signature",
    }
)
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "authorization",
    "credential",
    "password",
    "secret",
    "signature",
    "signed_url",
    "token",
)
INVENTORY_MODES = frozenset({"production", "synthetic"})
MAX_SIGNED_BIGINT = 9_223_372_036_854_775_807
PRODUCTION_INVENTORY_FORMAT = "reproduction-source-inventory-v2"
RESOLUTION_FORMAT = "reproduction-resolution-v1"
RESOLUTION_OUTCOMES = frozenset(
    {
        "resolved",
        "operator_input_required",
        "transient_error",
        "definitively_unavailable",
        "not_required",
    }
)


@dataclass(frozen=True)
class PreflightMetrics:
    preflight_run_id: str
    profile_id: str
    status: str
    inventory_mode: str
    exact_mode: bool
    source_count: int
    required_source_count: int
    object_count: int
    total_bytes: int
    planned_max_bytes: int
    unknown_size_count: int
    unbounded_size_count: int
    blocking_sources: tuple[str, ...]
    manifest_sha256: str
    scientific_contract_sha256: str
    inventory_sha256: str


class PreflightBlockedError(RuntimeError):
    """Raised after a non-ready preflight has been durably recorded."""

    def __init__(self, metrics: PreflightMetrics):
        self.metrics = metrics
        blockers = ", ".join(metrics.blocking_sources)
        super().__init__(
            f"{metrics.profile_id} reproduction preflight is blocked: {blockers}"
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: str | bytes) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def _normalized_key(key: str) -> str:
    return key.lower().replace("-", "_")


def _is_sensitive_key(key: str) -> bool:
    normalized = _normalized_key(key)
    return normalized in _SENSITIVE_KEYS or any(
        marker in normalized for marker in _SENSITIVE_KEY_PARTS
    )


def _assert_secret_safe(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = _normalized_key(str(key))
            if _is_sensitive_key(normalized) and child not in (None, "", False):
                raise ValueError(f"Secret-bearing field is forbidden at {path}.{key}")
            _assert_secret_safe(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_secret_safe(child, path=f"{path}[{index}]")
    elif isinstance(value, str):
        parsed_url = urlsplit(value)
        if not parsed_url.scheme:
            return
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError(f"Credential-bearing URL is forbidden at {path}")
        query_keys = {
            _normalized_key(query_key)
            for query_key, _query_value in parse_qsl(
                parsed_url.query, keep_blank_values=True
            )
        }
        forbidden = sorted(
            query_key for query_key in query_keys if _is_sensitive_key(query_key)
        )
        if forbidden:
            raise ValueError(
                f"Signed or secret-bearing URL is forbidden at {path}: "
                f"{', '.join(forbidden)}"
            )


def _load_json(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Cannot read reproduction input {path}: {exc}") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in reproduction input {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Reproduction input {path} must contain a JSON object")
    _assert_secret_safe(payload)
    return payload, _sha256(_canonical_json(payload))


def _validate_scientific_contract(path: Path) -> str:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Cannot read scientific contract {path}: {exc}") from exc
    rows = list(csv.DictReader(raw.decode("utf-8").splitlines()))
    if not rows or any(not row.get("contract_key") for row in rows):
        raise ValueError(f"Scientific contract {path} must contain keyed policy rows")
    contract_keys = [str(row["contract_key"]) for row in rows]
    if len(contract_keys) != len(set(contract_keys)):
        raise ValueError(f"Scientific contract {path} contains duplicate policy keys")
    return _sha256(raw)


def load_profile(
    profile_id: str,
    *,
    manifest_path: Path | None = None,
) -> tuple[dict[str, Any], str, str]:
    try:
        default_manifest = PROFILE_MANIFESTS[profile_id]
    except KeyError as exc:
        expected = ", ".join(sorted(PROFILE_MANIFESTS))
        raise ValueError(
            f"Unknown reproduction profile {profile_id!r}; expected: {expected}"
        ) from exc
    manifest_file = (manifest_path or default_manifest).resolve()
    manifest, manifest_sha256 = _load_json(manifest_file)
    if manifest.get("profile_id") != profile_id:
        raise ValueError(
            f"Manifest profile_id must be {profile_id!r}, got "
            f"{manifest.get('profile_id')!r}"
        )
    for field in (
        "profile_version",
        "paper_doi",
        "inventory_format",
        "resolution_format",
        "resolution_evidence",
        "scientific_contract",
        "sources",
    ):
        if not manifest.get(field):
            raise ValueError(f"Manifest field {field!r} is required")
    if manifest["inventory_format"] != PRODUCTION_INVENTORY_FORMAT:
        raise ValueError(
            f"Manifest inventory_format must be {PRODUCTION_INVENTORY_FORMAT!r}"
        )
    if manifest["resolution_format"] != RESOLUTION_FORMAT:
        raise ValueError(f"Manifest resolution_format must be {RESOLUTION_FORMAT!r}")
    resolution_path = (
        manifest_file.parent / str(manifest["resolution_evidence"])
    ).resolve()
    if resolution_path.parent != manifest_file.parent:
        raise ValueError("Resolution evidence must be beside its source manifest")
    if not isinstance(manifest["sources"], list):
        raise ValueError("Manifest sources must be a list")
    source_ids: set[str] = set()
    for source in manifest["sources"]:
        if not isinstance(source, dict):
            raise ValueError("Every source contract must be an object")
        required_fields = (
            "id",
            "provider",
            "version",
            "access_method",
            "required",
            "required_exactness",
            "url",
        )
        missing = [
            field
            for field in required_fields
            if field not in source or (field != "required" and not source[field])
        ]
        if missing:
            raise ValueError(
                f"Source contract is missing fields: {', '.join(sorted(missing))}"
            )
        source_id = str(source["id"])
        if source_id in source_ids:
            raise ValueError(f"Duplicate source id {source_id!r}")
        source_ids.add(source_id)
        if source["required_exactness"] not in EXACTNESS_STATUSES - {"unavailable"}:
            raise ValueError(
                f"Source {source_id!r} has unsupported required_exactness "
                f"{source['required_exactness']!r}"
            )
        fallbacks = source.get("allowed_fallbacks", [])
        if not isinstance(fallbacks, list) or any(
            value not in EXACTNESS_STATUSES - {"exact", "unavailable"}
            for value in fallbacks
        ):
            raise ValueError(f"Source {source_id!r} has invalid allowed_fallbacks")
        if not isinstance(source["required"], bool):
            raise ValueError(f"Source {source_id!r} required must be boolean")
    contract_path = (manifest_file.parent / manifest["scientific_contract"]).resolve()
    if contract_path.parent != manifest_file.parent:
        raise ValueError("Scientific contract must be beside its source manifest")
    scientific_contract_sha256 = _validate_scientific_contract(contract_path)
    return manifest, manifest_sha256, scientific_contract_sha256


def _load_inventory(
    profile_id: str,
    inventory_path: Path | None,
) -> tuple[dict[str, Any], str]:
    if inventory_path is None:
        inventory: dict[str, Any] = {
            "inventory_mode": "production",
            "profile_id": profile_id,
            "sources": [],
        }
        return inventory, _sha256(_canonical_json(inventory))
    inventory, inventory_sha256 = _load_json(inventory_path.resolve())
    if inventory.get("profile_id") != profile_id:
        raise ValueError(
            f"Inventory profile_id must be {profile_id!r}, got "
            f"{inventory.get('profile_id')!r}"
        )
    if not isinstance(inventory.get("sources"), list):
        raise ValueError("Inventory sources must be a list")
    if inventory.get("inventory_mode") not in INVENTORY_MODES:
        expected = ", ".join(sorted(INVENTORY_MODES))
        raise ValueError(f"Inventory mode must be one of: {expected}")
    if inventory["inventory_mode"] == "production":
        if inventory.get("inventory_format") != PRODUCTION_INVENTORY_FORMAT:
            raise ValueError(
                f"Production inventory_format must be {PRODUCTION_INVENTORY_FORMAT!r}"
            )
        if inventory.get("resolution_format") != RESOLUTION_FORMAT:
            raise ValueError(
                f"Production resolution_format must be {RESOLUTION_FORMAT!r}"
            )
        resolution_hash = str(inventory.get("resolution_bundle_sha256", ""))
        if len(resolution_hash) != 64 or any(
            character not in "0123456789abcdef" for character in resolution_hash
        ):
            raise ValueError(
                "Production inventory requires a resolution-bundle SHA-256"
            )
    return inventory, inventory_sha256


def _validate_inventory(
    manifest: dict[str, Any],
    inventory: dict[str, Any],
    *,
    exact_mode: bool,
    max_objects: int | None,
    max_bytes: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], tuple[str, ...]]:
    contracts = {str(source["id"]): source for source in manifest["sources"]}
    inventory_sources: dict[str, dict[str, Any]] = {}
    objects: list[dict[str, Any]] = []
    blockers: list[str] = []
    for source in inventory["sources"]:
        if not isinstance(source, dict):
            raise ValueError("Every inventory source must be an object")
        source_id = str(source.get("source_id", ""))
        if source_id not in contracts:
            raise ValueError(f"Inventory contains unknown source {source_id!r}")
        if source_id in inventory_sources:
            raise ValueError(f"Inventory contains duplicate source {source_id!r}")
        status = source.get("exactness_status")
        if status not in EXACTNESS_STATUSES:
            raise ValueError(
                f"Inventory source {source_id!r} has invalid exactness_status "
                f"{status!r}"
            )
        source_objects = source.get("objects", [])
        if not isinstance(source_objects, list):
            raise ValueError(f"Inventory objects for {source_id!r} must be a list")
        if inventory["inventory_mode"] == "production":
            outcome = source.get("resolution_outcome")
            if outcome not in RESOLUTION_OUTCOMES:
                raise ValueError(
                    f"Production source {source_id!r} has invalid resolution outcome"
                )
            evidence = source.get("evidence")
            if not isinstance(evidence, list) or not evidence:
                raise ValueError(
                    f"Production source {source_id!r} requires technical evidence"
                )
            for record in evidence:
                if not isinstance(record, dict):
                    raise ValueError(
                        f"Evidence for production source {source_id!r} "
                        "must be an object"
                    )
                evidence_url = str(record.get("url", ""))
                evidence_hash = str(record.get("sha256", ""))
                retrieved_at = str(record.get("retrieved_at", ""))
                parsed_evidence_url = urlsplit(evidence_url)
                if (
                    parsed_evidence_url.scheme not in {"http", "https"}
                    or not parsed_evidence_url.netloc
                    or len(evidence_hash) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in evidence_hash
                    )
                ):
                    raise ValueError(
                        f"Production source {source_id!r} has malformed evidence"
                    )
                try:
                    timestamp = datetime.fromisoformat(
                        retrieved_at.replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise ValueError(
                        f"Production source {source_id!r} has malformed evidence"
                    ) from exc
                if timestamp.tzinfo is None:
                    raise ValueError(
                        f"Production source {source_id!r} has malformed evidence"
                    )
            if outcome != "resolved" and source_objects:
                raise ValueError(
                    f"Production source {source_id!r} cannot attach objects to "
                    f"outcome {outcome!r}"
                )
        seen_object_ids: set[str] = set()
        source_total_bytes = 0
        source_planned_max_bytes = 0
        unknown_size_count = 0
        unbounded_size_count = 0
        normalized_objects: list[dict[str, Any]] = []
        for item in source_objects:
            if not isinstance(item, dict):
                raise ValueError(
                    f"Inventory object for {source_id!r} must be an object"
                )
            object_id = str(item.get("object_id", ""))
            canonical_url = str(item.get("url", ""))
            if not object_id or not canonical_url:
                raise ValueError(
                    f"Inventory objects for {source_id!r} require object_id and url"
                )
            if inventory["inventory_mode"] == "production":
                parsed_object_url = urlsplit(canonical_url)
                if (
                    parsed_object_url.scheme not in {"http", "https"}
                    or not parsed_object_url.netloc
                ):
                    raise ValueError(
                        f"Production object {object_id!r} requires an absolute "
                        "HTTP(S) URL"
                    )
            if object_id in seen_object_ids:
                raise ValueError(
                    f"Inventory source {source_id!r} repeats object {object_id!r}"
                )
            seen_object_ids.add(object_id)
            size_bytes = item.get("size_bytes")
            size_upper_bound_bytes = item.get("size_upper_bound_bytes")
            if size_bytes is not None and (
                not isinstance(size_bytes, int)
                or isinstance(size_bytes, bool)
                or not 0 <= size_bytes <= MAX_SIGNED_BIGINT
            ):
                raise ValueError(
                    f"Inventory object {object_id!r} size_bytes must fit BIGINT"
                )
            if size_upper_bound_bytes is not None and (
                not isinstance(size_upper_bound_bytes, int)
                or isinstance(size_upper_bound_bytes, bool)
                or not 0 <= size_upper_bound_bytes <= MAX_SIGNED_BIGINT
            ):
                raise ValueError(
                    f"Inventory object {object_id!r} size_upper_bound_bytes "
                    "must fit BIGINT"
                )
            if (
                size_bytes is not None
                and size_upper_bound_bytes is not None
                and size_upper_bound_bytes < size_bytes
            ):
                raise ValueError(
                    f"Inventory object {object_id!r} upper bound is below exact size"
                )
            if size_bytes is None:
                unknown_size_count += 1
                if size_upper_bound_bytes is None:
                    unbounded_size_count += 1
                else:
                    source_planned_max_bytes += size_upper_bound_bytes
            else:
                source_total_bytes += size_bytes
                source_planned_max_bytes += size_bytes
            normalized = {
                **item,
                "object_id": object_id,
                "url": canonical_url,
                "size_bytes": size_bytes,
                "source_id": source_id,
                "exactness_status": status,
            }
            normalized_objects.append(normalized)
            objects.append(normalized)
        contract = contracts[source_id]
        if contract.get("checksum"):
            for item in normalized_objects:
                if (
                    item.get("checksum_algorithm") != contract.get("checksum_algorithm")
                    or item.get("checksum") != contract["checksum"]
                ):
                    raise ValueError(
                        f"Inventory object {item['object_id']!r} does not match "
                        f"the pinned checksum for source {source_id!r}"
                    )
        inventory_sources[source_id] = {
            **source,
            "source_id": source_id,
            "object_count": len(normalized_objects),
            "total_bytes": source_total_bytes,
            "planned_max_bytes": source_planned_max_bytes,
            "unknown_size_count": unknown_size_count,
            "unbounded_size_count": unbounded_size_count,
        }

    completeness: list[dict[str, Any]] = []
    for source_id, contract in contracts.items():
        found = inventory_sources.get(source_id)
        status = found["exactness_status"] if found else "unavailable"
        outcome = found.get("resolution_outcome") if found else None
        object_count = found["object_count"] if found else 0
        allowed = {contract["required_exactness"]}
        if not exact_mode:
            allowed.update(contract.get("allowed_fallbacks", []))
        reason: str | None = None
        if inventory["inventory_mode"] == "production":
            if outcome not in RESOLUTION_OUTCOMES:
                reason = "production source lacks validated resolution evidence"
            elif outcome == "not_required" and contract["required"]:
                reason = "required source cannot be classified as not_required"
            elif outcome != "resolved" and contract["required"]:
                reason = str(
                    (found or {}).get("reason")
                    or f"source resolution outcome is {outcome}"
                )
            elif outcome == "resolved" and object_count == 0:
                reason = "resolved source has no provider objects"
        if reason is None and contract["required"] and object_count == 0:
            reason = str(
                (found or {}).get("reason")
                or "required source has no discovered objects"
            )
        elif reason is None and contract["required"] and status not in allowed:
            reason = (
                f"status {status!r} does not satisfy "
                f"{'exact' if exact_mode else 'allowed'} mode"
            )
        if reason:
            blockers.append(f"{source_id}: {reason}")
        completeness.append(
            {
                "source_id": source_id,
                "exactness_status": status,
                "object_count": object_count,
                "total_bytes": found["total_bytes"] if found else 0,
                "planned_max_bytes": found["planned_max_bytes"] if found else 0,
                "unknown_size_count": found["unknown_size_count"] if found else 0,
                "unbounded_size_count": (found["unbounded_size_count"] if found else 0),
                "resolution_outcome": outcome,
                "blocking_reason": reason,
            }
        )

    if inventory["inventory_mode"] == "production":
        l4 = inventory_sources.get("swot_l4_sos_paper_snapshot")
        grdc = inventory_sources.get("grdc_gauge_fallback")
        l4_embeds_priors = bool(
            (l4 or {}).get("evidence_summary", {}).get("contains_gauge_priors")
        )
        if l4 is not None and not l4_embeds_priors:
            if (
                not grdc
                or grdc.get("resolution_outcome") != "resolved"
                or grdc.get("object_count") == 0
            ):
                blockers.append(
                    "grdc_gauge_fallback: exact gauge evidence is required when "
                    "the pinned L4 objects do not prove embedded gauge priors"
                )
    planned_max_bytes = sum(item["planned_max_bytes"] for item in completeness)
    if planned_max_bytes > MAX_SIGNED_BIGINT:
        raise ValueError("Planned inventory bytes exceed the warehouse BIGINT limit")
    if max_objects is not None and len(objects) > max_objects:
        blockers.append(
            f"object budget exceeded: discovered {len(objects)}, limit {max_objects}"
        )
    if max_bytes is not None and planned_max_bytes > max_bytes:
        blockers.append(
            f"storage budget exceeded: planned maximum {planned_max_bytes} bytes, "
            f"limit {max_bytes}"
        )
    total_unbounded_sizes = sum(item["unbounded_size_count"] for item in completeness)
    if inventory["inventory_mode"] == "production" and total_unbounded_sizes:
        blockers.append(
            "storage plan is unbounded: "
            f"{total_unbounded_sizes} objects have neither an exact size nor an "
            "upper bound"
        )
    elif max_bytes is not None and total_unbounded_sizes:
        blockers.append(
            "storage budget cannot be verified: "
            f"{total_unbounded_sizes} object sizes are unknown"
        )
    return completeness, objects, tuple(blockers)


def _persist(
    conn: duckdb.DuckDBPyConnection,
    *,
    profile_id: str,
    manifest: dict[str, Any],
    manifest_sha256: str,
    scientific_contract_sha256: str,
    inventory_sha256: str,
    metrics: PreflightMetrics,
    completeness: list[dict[str, Any]],
    objects: list[dict[str, Any]],
) -> None:
    contracts_table = reproduction_ops_tbl(profile_id, "source_contracts")
    requests_table = reproduction_ops_tbl(profile_id, "source_requests")
    runs_table = reproduction_ops_tbl(profile_id, "preflight_runs")
    completeness_table = reproduction_ops_tbl(profile_id, "source_completeness")
    objects_table = reproduction_ops_tbl(profile_id, "source_objects")
    links_table = reproduction_ops_tbl(profile_id, "preflight_source_objects")
    generations_table = reproduction_ops_tbl(profile_id, "acquisition_generations")
    report = asdict(metrics)
    report["blocking_sources"] = list(metrics.blocking_sources)
    object_rows: list[dict[str, Any]] = []
    link_rows: list[dict[str, Any]] = []
    for item in objects:
        object_json = _canonical_json(item)
        revision_id = _sha256(
            f"{profile_id}|{item['source_id']}|{item['object_id']}|{object_json}"
        )
        object_rows.append(
            {
                "source_object_revision_id": revision_id,
                "source_id": item["source_id"],
                "object_id": item["object_id"],
                "exactness_status": item["exactness_status"],
                "canonical_url": item["url"],
                "source_revision": item.get("source_revision"),
                "checksum_algorithm": item.get("checksum_algorithm"),
                "checksum": item.get("checksum"),
                "object_etag": item.get("etag"),
                "size_bytes": item.get("size_bytes"),
                "schema_fingerprint": item.get("schema_fingerprint"),
                "object_json": object_json,
            }
        )
        link_rows.append(
            {
                "preflight_run_id": metrics.preflight_run_id,
                "source_object_revision_id": revision_id,
                "source_id": item["source_id"],
                "inventory_sha256": inventory_sha256,
            }
        )
    object_relation = "_reproduction_source_objects_batch"
    link_relation = "_reproduction_source_object_links_batch"
    if object_rows:
        conn.register(object_relation, pa.Table.from_pylist(object_rows))
        conn.register(link_relation, pa.Table.from_pylist(link_rows))
    conn.execute("BEGIN TRANSACTION")
    try:
        for source in manifest["sources"]:
            source_id = str(source["id"])
            conn.execute(
                f"""
                INSERT INTO {contracts_table} VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    timezone('UTC', current_timestamp)
                )
                ON CONFLICT DO NOTHING
                """,
                [
                    source_id,
                    manifest["profile_version"],
                    manifest["paper_doi"],
                    source["provider"],
                    source["version"],
                    source["access_method"],
                    source["required"],
                    source["required_exactness"],
                    _canonical_json(source.get("allowed_fallbacks", [])),
                    source["url"],
                    source.get("concept_id"),
                    source.get("doi"),
                    manifest_sha256,
                    scientific_contract_sha256,
                    _canonical_json(source),
                ],
            )
            request_json = _canonical_json(source.get("request", {}))
            request_id = _sha256(
                f"{profile_id}|{source_id}|{manifest_sha256}|{request_json}"
            )
            conn.execute(
                f"""
                INSERT INTO {requests_table}
                VALUES (
                    ?, ?, ?, ?, ?, timezone('UTC', current_timestamp)
                )
                ON CONFLICT DO NOTHING
                """,
                [
                    request_id,
                    source_id,
                    source["access_method"],
                    request_json,
                    manifest_sha256,
                ],
            )
        conn.execute(
            f"""
            INSERT INTO {runs_table} VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                timezone('UTC', current_timestamp)
            )
            ON CONFLICT DO NOTHING
            """,
            [
                metrics.preflight_run_id,
                manifest_sha256,
                scientific_contract_sha256,
                inventory_sha256,
                metrics.inventory_mode,
                metrics.exact_mode,
                metrics.status,
                metrics.source_count,
                metrics.required_source_count,
                metrics.object_count,
                metrics.total_bytes,
                metrics.unknown_size_count,
                _canonical_json(list(metrics.blocking_sources)),
                _canonical_json(report),
            ],
        )
        for item in completeness:
            conn.execute(
                f"""
                INSERT INTO {completeness_table}
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    timezone('UTC', current_timestamp)
                )
                ON CONFLICT DO NOTHING
                """,
                [
                    metrics.preflight_run_id,
                    item["source_id"],
                    item["exactness_status"],
                    item["object_count"],
                    item["total_bytes"],
                    item["unknown_size_count"],
                    item["blocking_reason"],
                ],
            )
        if object_rows:
            conn.execute(
                f"""
                INSERT INTO {objects_table}
                SELECT
                    source_object_revision_id,
                    source_id,
                    object_id,
                    exactness_status,
                    canonical_url,
                    source_revision,
                    checksum_algorithm,
                    checksum,
                    object_etag,
                    size_bytes,
                    schema_fingerprint,
                    object_json,
                    timezone('UTC', current_timestamp)
                FROM {object_relation}
                ON CONFLICT DO NOTHING
                """
            )
            conn.execute(
                f"""
                INSERT INTO {links_table}
                SELECT
                    preflight_run_id,
                    source_object_revision_id,
                    source_id,
                    inventory_sha256,
                    timezone('UTC', current_timestamp)
                FROM {link_relation}
                ON CONFLICT DO NOTHING
                """
            )
        if metrics.status == "ready":
            generation_id = _sha256(f"{metrics.preflight_run_id}|acquisition")
            generation_status = (
                "planned" if metrics.inventory_mode == "production" else "synthetic"
            )
            input_manifest_sha256 = _sha256(
                _canonical_json(
                    {
                        "inventory_sha256": inventory_sha256,
                        "manifest_sha256": manifest_sha256,
                        "scientific_contract_sha256": scientific_contract_sha256,
                    }
                )
            )
            conn.execute(
                f"""
                INSERT INTO {generations_table}
                VALUES (
                    ?, ?, ?, ?, timezone('UTC', current_timestamp), NULL
                )
                ON CONFLICT DO NOTHING
                """,
                [
                    generation_id,
                    metrics.preflight_run_id,
                    input_manifest_sha256,
                    generation_status,
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        if object_rows:
            conn.unregister(object_relation)
            conn.unregister(link_relation)


def run_preflight(
    profile_id: str,
    *,
    manifest_path: Path | None = None,
    inventory_path: Path | None = None,
    exact_mode: bool = True,
    max_objects: int | None = None,
    max_bytes: int | None = None,
    fail_on_blocked: bool = True,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> PreflightMetrics:
    if max_objects is not None and max_objects < 1:
        raise ValueError("max_objects must be at least 1")
    if max_bytes is not None and max_bytes < 1:
        raise ValueError("max_bytes must be at least 1")
    manifest, manifest_sha256, scientific_contract_sha256 = load_profile(
        profile_id,
        manifest_path=manifest_path,
    )
    inventory, inventory_sha256 = _load_inventory(profile_id, inventory_path)
    completeness, objects, blockers = _validate_inventory(
        manifest,
        inventory,
        exact_mode=exact_mode,
        max_objects=max_objects,
        max_bytes=max_bytes,
    )
    run_identity = _canonical_json(
        {
            "exact_mode": exact_mode,
            "inventory_sha256": inventory_sha256,
            "inventory_mode": inventory["inventory_mode"],
            "manifest_sha256": manifest_sha256,
            "max_bytes": max_bytes,
            "max_objects": max_objects,
            "profile_id": profile_id,
            "scientific_contract_sha256": scientific_contract_sha256,
        }
    )
    metrics = PreflightMetrics(
        preflight_run_id=_sha256(run_identity),
        profile_id=profile_id,
        status="blocked" if blockers else "ready",
        inventory_mode=inventory["inventory_mode"],
        exact_mode=exact_mode,
        source_count=len(manifest["sources"]),
        required_source_count=sum(
            bool(source["required"]) for source in manifest["sources"]
        ),
        object_count=len(objects),
        total_bytes=sum(
            int(item["size_bytes"])
            for item in objects
            if item["size_bytes"] is not None
        ),
        planned_max_bytes=sum(
            int(
                item["size_bytes"]
                if item["size_bytes"] is not None
                else item.get("size_upper_bound_bytes") or 0
            )
            for item in objects
        ),
        unknown_size_count=sum(item["size_bytes"] is None for item in objects),
        unbounded_size_count=sum(
            item["size_bytes"] is None and item.get("size_upper_bound_bytes") is None
            for item in objects
        ),
        blocking_sources=blockers,
        manifest_sha256=manifest_sha256,
        scientific_contract_sha256=scientific_contract_sha256,
        inventory_sha256=inventory_sha256,
    )
    with _use_conn(conn) as connection:
        _persist(
            connection,
            profile_id=profile_id,
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            scientific_contract_sha256=scientific_contract_sha256,
            inventory_sha256=inventory_sha256,
            metrics=metrics,
            completeness=completeness,
            objects=objects,
        )
    if blockers and fail_on_blocked:
        raise PreflightBlockedError(metrics)
    return metrics


__all__ = [
    "EXACTNESS_STATUSES",
    "PRODUCTION_INVENTORY_FORMAT",
    "PROFILE_MANIFESTS",
    "PreflightBlockedError",
    "PreflightMetrics",
    "load_profile",
    "run_preflight",
]
