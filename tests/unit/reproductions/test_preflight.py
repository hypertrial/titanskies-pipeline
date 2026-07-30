from __future__ import annotations

import csv
import json
from copy import deepcopy
from pathlib import Path

import duckdb
import pytest

import titanskies_pipeline.reproductions.preflight as preflight
from titanskies_pipeline.reproductions.preflight import (
    PreflightBlockedError,
    load_profile,
    run_preflight,
)
from titanskies_pipeline.storage.duckdb.schemas.bootstrap import bootstrap_all_tables
from titanskies_pipeline.storage.duckdb.schemas.constants import reproduction_ops_tbl

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "reproductions"


@pytest.fixture
def conn():
    connection = duckdb.connect(":memory:")
    bootstrap_all_tables(connection)
    yield connection
    connection.close()


def _inventory(profile_id: str) -> dict:
    return json.loads((FIXTURES / f"{profile_id}_preflight.json").read_text())


def _production_inventory(profile_id: str) -> dict:
    inventory = _inventory(profile_id)
    inventory.update(
        {
            "inventory_mode": "production",
            "inventory_format": "reproduction-source-inventory-v2",
            "resolution_format": "reproduction-resolution-v1",
            "resolution_bundle_sha256": "d" * 64,
        }
    )
    for source in inventory["sources"]:
        if source["exactness_status"] == "unavailable":
            source["resolution_outcome"] = "not_required"
        else:
            source["resolution_outcome"] = "resolved"
        source["evidence"] = [
            {
                "url": "https://example.test/evidence",
                "retrieved_at": "2025-01-01T00:00:00Z",
                "sha256": "e" * 64,
            }
        ]
        for item in source["objects"]:
            if item["size_bytes"] is None:
                item["size_upper_bound_bytes"] = 10_000
    return inventory


def _write_json(tmp_path: Path, name: str, payload: object) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload))
    return path


def _write_manifest(
    tmp_path: Path,
    payload: object,
    *,
    contract: str = "contract_key,value\nversion,1\n",
) -> Path:
    (tmp_path / "contract.csv").write_text(contract)
    if isinstance(payload, dict):
        payload = {**payload, "scientific_contract": "contract.csv"}
    return _write_json(tmp_path, "manifest.json", payload)


def _contract(profile_id: str) -> dict[str, str]:
    path = ROOT / "config" / "reproductions" / f"{profile_id}_contract.csv"
    with path.open(newline="") as handle:
        return {
            str(row["contract_key"]): str(row["value"])
            for row in csv.DictReader(handle)
        }


@pytest.mark.parametrize(
    ("profile_id", "expected_objects", "expected_unknown"),
    [("sun2025", 7, 1), ("andreadis2025", 5, 0)],
)
def test_exact_preflight_persists_idempotent_inventory_and_generation(
    conn, profile_id, expected_objects, expected_unknown
):
    fixture = FIXTURES / f"{profile_id}_preflight.json"
    first = run_preflight(profile_id, inventory_path=fixture, conn=conn)
    second = run_preflight(profile_id, inventory_path=fixture, conn=conn)

    assert first == second
    assert first.status == "ready"
    assert first.inventory_mode == "synthetic"
    assert first.object_count == expected_objects
    assert first.unknown_size_count == expected_unknown
    assert (
        conn.execute(
            f"select count(*) from {reproduction_ops_tbl(profile_id, 'preflight_runs')}"
        ).fetchone()[0]
        == 1
    )
    assert (
        conn.execute(
            f"select count(*) from {reproduction_ops_tbl(profile_id, 'source_objects')}"
        ).fetchone()[0]
        == expected_objects
    )
    assert (
        conn.execute(
            f"""
            select count(*)
            from {reproduction_ops_tbl(profile_id, "preflight_source_objects")}
            """
        ).fetchone()[0]
        == expected_objects
    )
    assert (
        conn.execute(
            f"""
        select status
        from {reproduction_ops_tbl(profile_id, "acquisition_generations")}
        """
        ).fetchone()[0]
        == "synthetic"
    )


def test_exact_mode_blocks_fallback_but_declared_nonexact_mode_accepts_it(
    conn, tmp_path
):
    payload = _inventory("sun2025")
    next(
        source
        for source in payload["sources"]
        if source["source_id"] == "era5_single_levels"
    )["exactness_status"] = "provider_reprocessed"
    path = _write_json(tmp_path, "fallback.json", payload)

    with pytest.raises(PreflightBlockedError, match="era5_single_levels") as exc:
        run_preflight("sun2025", inventory_path=path, conn=conn)
    assert exc.value.metrics.status == "blocked"
    assert (
        conn.execute(
            f"""
        select status
        from {reproduction_ops_tbl("sun2025", "preflight_runs")}
        where preflight_run_id = ?
        """,
            [exc.value.metrics.preflight_run_id],
        ).fetchone()[0]
        == "blocked"
    )

    allowed = run_preflight(
        "sun2025",
        inventory_path=path,
        exact_mode=False,
        conn=conn,
    )
    assert allowed.status == "ready"


def test_missing_inventory_and_unavailable_required_source_remain_blocked(
    conn, tmp_path
):
    empty = run_preflight("sun2025", fail_on_blocked=False, conn=conn)
    assert empty.status == "blocked"
    assert len(empty.blocking_sources) == empty.required_source_count

    payload = _inventory("sun2025")
    facility = next(
        source
        for source in payload["sources"]
        if source["source_id"] == "facility_cohort_14"
    )
    facility["exactness_status"] = "unavailable"
    facility["objects"] = []
    facility["reason"] = "supplement extraction pending"
    path = _write_json(tmp_path, "unavailable.json", payload)
    with pytest.raises(PreflightBlockedError, match="supplement extraction pending"):
        run_preflight(
            "sun2025",
            inventory_path=path,
            exact_mode=False,
            conn=conn,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_objects": 1}, "object budget exceeded"),
        ({"max_bytes": 1}, "storage budget exceeded"),
    ],
)
def test_preflight_budgets_are_hard_blockers(conn, kwargs, message):
    with pytest.raises(PreflightBlockedError, match=message):
        run_preflight(
            "andreadis2025",
            inventory_path=FIXTURES / "andreadis2025_preflight.json",
            conn=conn,
            **kwargs,
        )


def test_unknown_object_size_blocks_a_declared_storage_budget(conn):
    with pytest.raises(PreflightBlockedError, match="cannot be verified"):
        run_preflight(
            "sun2025",
            inventory_path=FIXTURES / "sun2025_preflight.json",
            max_bytes=1_000_000,
            conn=conn,
        )


@pytest.mark.parametrize("field", ["max_objects", "max_bytes"])
def test_preflight_rejects_nonpositive_budgets(conn, field):
    with pytest.raises(ValueError, match=field):
        run_preflight("sun2025", conn=conn, **{field: 0})


def test_inventory_rejects_signed_urls_and_secret_fields(conn, tmp_path):
    payload = _inventory("sun2025")
    payload["sources"][0]["objects"][0]["url"] = (
        "https://example.test/file?X-Amz-Signature=secret"
    )
    signed = _write_json(tmp_path, "signed.json", payload)
    with pytest.raises(ValueError, match="Signed or secret-bearing URL"):
        run_preflight("sun2025", inventory_path=signed, conn=conn)

    payload = _inventory("sun2025")
    payload["api_key"] = "must-not-persist"
    secret = _write_json(tmp_path, "secret.json", payload)
    with pytest.raises(ValueError, match="Secret-bearing field"):
        run_preflight("sun2025", inventory_path=secret, conn=conn)

    payload = _inventory("sun2025")
    payload["sources"][0]["objects"][0]["url"] = (
        "https://user:password@example.test/file"
    )
    credentials = _write_json(tmp_path, "credentials.json", payload)
    with pytest.raises(ValueError, match="Credential-bearing URL"):
        run_preflight("sun2025", inventory_path=credentials, conn=conn)

    payload = _inventory("sun2025")
    payload["sources"][0]["objects"][0]["url"] = "https://example.test/file?sig=secret"
    azure_signed = _write_json(tmp_path, "azure-signed.json", payload)
    with pytest.raises(ValueError, match="Signed or secret-bearing URL"):
        run_preflight("sun2025", inventory_path=azure_signed, conn=conn)

    payload = _inventory("sun2025")
    payload["sources"][0]["objects"][0]["download_href"] = (
        "https://example.test/file?X-Amz-Credential=secret"
    )
    indirect = _write_json(tmp_path, "indirect-signed.json", payload)
    with pytest.raises(ValueError, match="Signed or secret-bearing URL"):
        run_preflight("sun2025", inventory_path=indirect, conn=conn)


def test_inventory_must_match_a_pinned_static_checksum(conn, tmp_path):
    payload = _inventory("sun2025")
    code = next(
        source for source in payload["sources"] if source["source_id"] == "sun2025_code"
    )
    code["objects"][0]["checksum"] = "0" * 32
    path = _write_json(tmp_path, "checksum.json", payload)
    with pytest.raises(ValueError, match="pinned checksum"):
        run_preflight("sun2025", inventory_path=path, conn=conn)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p.update(inventory_mode="other"), "Inventory mode"),
        (lambda p: p.update(profile_id="other"), "Inventory profile_id"),
        (lambda p: p.update(sources={}), "Inventory sources must be a list"),
        (
            lambda p: p["sources"].append(deepcopy(p["sources"][0])),
            "duplicate source",
        ),
        (
            lambda p: p["sources"][0].update(source_id="unknown"),
            "unknown source",
        ),
        (
            lambda p: p["sources"][0].update(exactness_status="maybe"),
            "invalid exactness_status",
        ),
        (
            lambda p: p["sources"][0].update(objects={}),
            "objects .* must be a list",
        ),
        (
            lambda p: p["sources"][0]["objects"].append(
                deepcopy(p["sources"][0]["objects"][0])
            ),
            "repeats object",
        ),
        (
            lambda p: p["sources"][0]["objects"][0].pop("url"),
            "require object_id and url",
        ),
        (
            lambda p: p["sources"][0]["objects"][0].update(size_bytes=-1),
            "size_bytes must fit BIGINT",
        ),
        (
            lambda p: p["sources"][0]["objects"].__setitem__(0, "bad"),
            "must be an object",
        ),
        (
            lambda p: p["sources"].__setitem__(0, "bad"),
            "inventory source must be an object",
        ),
    ],
)
def test_inventory_shape_validation(conn, tmp_path, mutation, message):
    payload = _inventory("sun2025")
    mutation(payload)
    path = _write_json(tmp_path, "invalid.json", payload)
    with pytest.raises(ValueError, match=message):
        run_preflight("sun2025", inventory_path=path, conn=conn)


def test_manifest_and_scientific_contract_validation(tmp_path):
    original, _manifest_hash, _contract_hash = load_profile("sun2025")

    invalid_cases = [
        ([], "must contain a JSON object"),
        ({}, "profile_id"),
        ({**original, "profile_id": "other"}, "Manifest profile_id"),
        (
            {**original, "inventory_format": "legacy"},
            "Manifest inventory_format",
        ),
        (
            {**original, "resolution_format": "legacy"},
            "Manifest resolution_format",
        ),
        ({**original, "sources": {}}, "Manifest field 'sources' is required"),
        ({**original, "sources": {"bad": "value"}}, "sources must be a list"),
        ({**original, "sources": ["bad"]}, "source contract must be an object"),
        (
            {**original, "sources": [{**original["sources"][0], "url": None}]},
            "missing fields",
        ),
        (
            {
                **original,
                "sources": [original["sources"][0], original["sources"][0]],
            },
            "Duplicate source id",
        ),
        (
            {
                **original,
                "sources": [
                    {**original["sources"][0], "required_exactness": "unknown"}
                ],
            },
            "unsupported required_exactness",
        ),
        (
            {
                **original,
                "sources": [{**original["sources"][0], "allowed_fallbacks": ["exact"]}],
            },
            "invalid allowed_fallbacks",
        ),
        (
            {
                **original,
                "sources": [{**original["sources"][0], "required": "yes"}],
            },
            "required must be boolean",
        ),
    ]
    for index, (payload, message) in enumerate(invalid_cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        path = _write_manifest(case_dir, payload)
        with pytest.raises(ValueError, match=message):
            load_profile("sun2025", manifest_path=path)

    escape_dir = tmp_path / "escape"
    escape_dir.mkdir()
    (tmp_path / "outside.csv").write_text("contract_key,value\nversion,1\n")
    escaped = _write_json(
        escape_dir,
        "manifest.json",
        {**original, "scientific_contract": "../outside.csv"},
    )
    with pytest.raises(ValueError, match="must be beside"):
        load_profile("sun2025", manifest_path=escaped)

    resolution_escape_dir = tmp_path / "resolution-escape"
    resolution_escape_dir.mkdir()
    resolution_escaped = _write_manifest(
        resolution_escape_dir,
        {**original, "resolution_evidence": "../outside.json"},
    )
    with pytest.raises(ValueError, match="Resolution evidence must be beside"):
        load_profile("sun2025", manifest_path=resolution_escaped)

    missing_contract_dir = tmp_path / "missing-contract"
    missing_contract_dir.mkdir()
    missing_contract = _write_json(
        missing_contract_dir,
        "manifest.json",
        {**original, "scientific_contract": "missing.csv"},
    )
    with pytest.raises(ValueError, match="Cannot read scientific contract"):
        load_profile("sun2025", manifest_path=missing_contract)

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    empty = _write_manifest(empty_dir, original, contract="contract_key,value\n")
    with pytest.raises(ValueError, match="keyed policy rows"):
        load_profile("sun2025", manifest_path=empty)

    duplicate_dir = tmp_path / "duplicate"
    duplicate_dir.mkdir()
    duplicate = _write_manifest(
        duplicate_dir,
        original,
        contract="contract_key,value\nversion,1\nversion,2\n",
    )
    with pytest.raises(ValueError, match="duplicate policy keys"):
        load_profile("sun2025", manifest_path=duplicate)


def test_tracked_scientific_contracts_pin_paper_reported_values():
    sun = _contract("sun2025")
    assert sun["maximum_cloud_fraction"] == "0.15"
    assert sun["selected_wind_altitude"] == "300"
    assert sun["integration_ellipse_major_axis"] == "20"
    assert sun["integration_ellipse_minor_axis"] == "10"
    assert sun["reported_plant_hour_checkpoint"] == "15558"

    andreadis = _contract("andreadis2025")
    assert andreadis["minimum_prior_width"] == "80"
    assert andreadis["minimum_reach_length"] == "7"
    assert andreadis["reported_conditionally_observed_gauged_reaches"] == "827"
    assert andreadis["reported_highest_quality_gauged_reaches"] == "65"
    assert andreadis["reported_hydraulic_consistency_gauged_reaches"] == "54"
    assert andreadis["reported_global_highest_quality_reaches"] == "11389"
    assert andreadis["reported_ungauged_highest_quality_reaches"] == "11274"


def test_json_and_file_errors_are_actionable(conn, tmp_path):
    malformed = tmp_path / "bad.json"
    malformed.write_text("{")
    with pytest.raises(ValueError, match="Invalid JSON"):
        run_preflight("sun2025", inventory_path=malformed, conn=conn)
    with pytest.raises(ValueError, match="Cannot read reproduction input"):
        run_preflight("sun2025", inventory_path=tmp_path / "missing.json", conn=conn)
    with pytest.raises(ValueError, match="Unknown reproduction profile"):
        load_profile("unknown")


def test_production_inventory_creates_planned_generation_and_keeps_contract_history(
    conn, tmp_path
):
    fixture = FIXTURES / "sun2025_preflight.json"
    run_preflight("sun2025", inventory_path=fixture, conn=conn)
    manifest, _manifest_hash, _contract_hash = load_profile("sun2025")
    manifest_path = _write_manifest(
        tmp_path,
        manifest,
        contract="contract_key,value\nversion,2\n",
    )
    inventory = _production_inventory("sun2025")
    inventory_path = _write_json(tmp_path, "production.json", inventory)

    metrics = run_preflight(
        "sun2025",
        manifest_path=manifest_path,
        inventory_path=inventory_path,
        conn=conn,
    )

    assert metrics.inventory_mode == "production"
    assert (
        conn.execute(
            f"""
            select status
            from {reproduction_ops_tbl("sun2025", "acquisition_generations")}
            where preflight_run_id = ?
            """,
            [metrics.preflight_run_id],
        ).fetchone()[0]
        == "planned"
    )
    assert (
        conn.execute(
            f"""
            select count(*)
            from {reproduction_ops_tbl("sun2025", "source_contracts")}
            """
        ).fetchone()[0]
        == 14
    )


def test_production_inventory_requires_resolution_format_and_bounded_sizes(
    conn, tmp_path
):
    legacy = _inventory("sun2025")
    legacy["inventory_mode"] = "production"
    with pytest.raises(ValueError, match="inventory_format"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "legacy.json", legacy),
            conn=conn,
        )

    production = _production_inventory("sun2025")
    geos = next(
        source
        for source in production["sources"]
        if source["source_id"] == "geos_cf_2024"
    )
    geos["objects"][0].pop("size_upper_bound_bytes")
    with pytest.raises(PreflightBlockedError, match="storage plan is unbounded"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "unbounded.json", production),
            conn=conn,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda payload: payload.update(resolution_format="legacy"),
            "resolution_format",
        ),
        (
            lambda payload: payload.update(resolution_bundle_sha256="not-a-hash"),
            "resolution-bundle SHA-256",
        ),
    ],
)
def test_production_inventory_requires_canonical_resolution_identity(
    conn, tmp_path, mutation, message
):
    production = _production_inventory("sun2025")
    mutation(production)
    with pytest.raises(ValueError, match=message):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "invalid-resolution.json", production),
            conn=conn,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda source: source.update(resolution_outcome="maybe"),
            "invalid resolution outcome",
        ),
        (
            lambda source: source.update(evidence=[]),
            "requires technical evidence",
        ),
        (
            lambda source: source.update(evidence=["not-an-object"]),
            "must be an object",
        ),
        (
            lambda source: source["evidence"][0].update(url="not-a-url"),
            "malformed evidence",
        ),
        (
            lambda source: source["evidence"][0].update(retrieved_at="not-a-timestamp"),
            "malformed evidence",
        ),
        (
            lambda source: source["evidence"][0].update(
                retrieved_at="2025-01-01T00:00:00"
            ),
            "malformed evidence",
        ),
        (
            lambda source: source.update(resolution_outcome="transient_error"),
            "cannot attach objects",
        ),
    ],
)
def test_production_source_resolution_evidence_is_validated(
    conn, tmp_path, mutation, message
):
    production = _production_inventory("sun2025")
    mutation(production["sources"][0])
    with pytest.raises(ValueError, match=message):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "invalid-evidence.json", production),
            conn=conn,
        )


def test_production_inventory_requires_absolute_provider_urls(conn, tmp_path):
    malformed_evidence = _production_inventory("sun2025")
    malformed_evidence["sources"][0]["evidence"][0]["url"] = "https:evidence"
    with pytest.raises(ValueError, match="malformed evidence"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(
                tmp_path, "malformed-evidence-url.json", malformed_evidence
            ),
            conn=conn,
        )

    local_object = _production_inventory("sun2025")
    local_object["sources"][0]["objects"][0]["url"] = "file:///tmp/object"
    with pytest.raises(ValueError, match="absolute"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "local-object-url.json", local_object),
            conn=conn,
        )


@pytest.mark.parametrize(
    ("upper_bound", "message"),
    [
        (-1, "size_upper_bound_bytes must fit BIGINT"),
        (1, "upper bound is below exact size"),
    ],
)
def test_production_object_upper_bounds_are_validated(
    conn, tmp_path, upper_bound, message
):
    production = _production_inventory("sun2025")
    item = production["sources"][0]["objects"][0]
    item["size_upper_bound_bytes"] = upper_bound
    with pytest.raises(ValueError, match=message):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "invalid-bound.json", production),
            conn=conn,
        )


def test_planned_max_bytes_uses_upper_bounds_for_budgeting(conn, tmp_path):
    production = _production_inventory("sun2025")
    path = _write_json(tmp_path, "production.json", production)
    metrics = run_preflight("sun2025", inventory_path=path, conn=conn)
    assert metrics.planned_max_bytes == metrics.total_bytes + 10_000
    assert metrics.unknown_size_count == 1
    assert metrics.unbounded_size_count == 0

    with pytest.raises(PreflightBlockedError, match="planned maximum"):
        run_preflight(
            "sun2025",
            inventory_path=path,
            max_bytes=metrics.planned_max_bytes - 1,
            conn=conn,
        )


def test_production_resolution_outcomes_and_conditional_grdc_block(conn, tmp_path):
    production = _production_inventory("andreadis2025")
    l4 = next(
        source
        for source in production["sources"]
        if source["source_id"] == "swot_l4_sos_paper_snapshot"
    )
    l4["evidence_summary"] = {"contains_gauge_priors": True}
    ready = run_preflight(
        "andreadis2025",
        inventory_path=_write_json(tmp_path, "ready.json", production),
        conn=conn,
    )
    assert ready.status == "ready"

    l4["evidence_summary"] = {"contains_gauge_priors": False}
    with pytest.raises(PreflightBlockedError, match="grdc_gauge_fallback"):
        run_preflight(
            "andreadis2025",
            inventory_path=_write_json(tmp_path, "grdc.json", production),
            conn=conn,
        )

    l4["resolution_outcome"] = "definitively_unavailable"
    l4["exactness_status"] = "unavailable"
    l4["objects"] = []
    l4["reason"] = "paper-time generation was retired"
    with pytest.raises(
        PreflightBlockedError, match="paper-time generation was retired"
    ):
        run_preflight(
            "andreadis2025",
            inventory_path=_write_json(tmp_path, "unavailable.json", production),
            conn=conn,
        )


def test_production_required_outcomes_report_precise_blockers(conn, tmp_path):
    not_required = _production_inventory("sun2025")
    source = not_required["sources"][0]
    source.update(
        resolution_outcome="not_required",
        exactness_status="unavailable",
        objects=[],
    )
    with pytest.raises(PreflightBlockedError, match="cannot be classified"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "not-required.json", not_required),
            conn=conn,
        )

    empty_resolved = _production_inventory("sun2025")
    empty_resolved["sources"][0]["objects"] = []
    with pytest.raises(PreflightBlockedError, match="no provider objects"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "empty-resolved.json", empty_resolved),
            conn=conn,
        )


def test_resolved_grdc_satisfies_conditional_prior_requirement(conn, tmp_path):
    production = _production_inventory("andreadis2025")
    l4 = next(
        source
        for source in production["sources"]
        if source["source_id"] == "swot_l4_sos_paper_snapshot"
    )
    l4["evidence_summary"] = {"contains_gauge_priors": False}
    grdc = next(
        source
        for source in production["sources"]
        if source["source_id"] == "grdc_gauge_fallback"
    )
    grdc.update(
        {
            "resolution_outcome": "resolved",
            "exactness_status": "exact",
            "objects": [
                {
                    "object_id": "grdc-priors",
                    "url": "https://example.test/grdc-priors.csv",
                    "size_bytes": 100,
                }
            ],
        }
    )

    metrics = run_preflight(
        "andreadis2025",
        inventory_path=_write_json(tmp_path, "grdc-resolved.json", production),
        conn=conn,
    )
    assert metrics.status == "ready"


def test_planned_inventory_total_must_fit_warehouse_bigint(conn, tmp_path):
    production = _production_inventory("sun2025")
    production["sources"][0]["objects"][0]["size_bytes"] = preflight.MAX_SIGNED_BIGINT
    production["sources"][1]["objects"][0]["size_bytes"] = 1
    with pytest.raises(ValueError, match="Planned inventory bytes exceed"):
        run_preflight(
            "sun2025",
            inventory_path=_write_json(tmp_path, "overflow.json", production),
            conn=conn,
        )


def test_changed_provider_revision_appends_one_object_revision(conn, tmp_path):
    production = _production_inventory("sun2025")
    first_path = _write_json(tmp_path, "first.json", production)
    run_preflight("sun2025", inventory_path=first_path, conn=conn)
    initial_count = conn.execute(
        f"select count(*) from {reproduction_ops_tbl('sun2025', 'source_objects')}"
    ).fetchone()[0]

    production["sources"][0]["objects"][0]["provider_revision_id"] = "revision-2"
    second_path = _write_json(tmp_path, "second.json", production)
    run_preflight("sun2025", inventory_path=second_path, conn=conn)

    assert (
        conn.execute(
            f"select count(*) from {reproduction_ops_tbl('sun2025', 'source_objects')}"
        ).fetchone()[0]
        == initial_count + 1
    )


def test_partial_source_success_is_retained_without_generation(conn, tmp_path):
    production = _production_inventory("sun2025")
    blocker = production["sources"][0]
    blocker.update(
        {
            "resolution_outcome": "transient_error",
            "exactness_status": "unavailable",
            "objects": [],
            "reason": "CMR pagination failed after successful sibling resolution",
        }
    )
    path = _write_json(tmp_path, "partial.json", production)

    metrics = run_preflight(
        "sun2025", inventory_path=path, fail_on_blocked=False, conn=conn
    )

    assert metrics.status == "blocked"
    assert (
        conn.execute(
            f"select count(*) from {reproduction_ops_tbl('sun2025', 'source_objects')}"
        ).fetchone()[0]
        == metrics.object_count
        > 0
    )
    assert (
        conn.execute(
            f"""
            select count(*)
            from {reproduction_ops_tbl("sun2025", "acquisition_generations")}
            """
        ).fetchone()[0]
        == 0
    )


def test_persistence_rolls_back_atomically(conn, monkeypatch):
    real_helper = preflight.reproduction_ops_tbl
    calls = 0

    def fail_completeness(profile_id, name):
        nonlocal calls
        calls += 1
        if name == "source_completeness":
            return '"missing_schema"."missing_table"'
        return real_helper(profile_id, name)

    monkeypatch.setattr(preflight, "reproduction_ops_tbl", fail_completeness)
    with pytest.raises(duckdb.CatalogException):
        run_preflight(
            "sun2025",
            inventory_path=FIXTURES / "sun2025_preflight.json",
            conn=conn,
        )
    assert calls == 7
    assert (
        conn.execute(
            f"select count(*) from {real_helper('sun2025', 'source_contracts')}"
        ).fetchone()[0]
        == 0
    )
