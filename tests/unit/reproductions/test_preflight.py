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
            "size_bytes must be non-negative",
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
    inventory = _inventory("sun2025")
    inventory["inventory_mode"] = "production"
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
