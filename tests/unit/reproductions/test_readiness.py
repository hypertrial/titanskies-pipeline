from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests
import scripts.resolve_reproduction_sources as readiness_cli

import titanskies_pipeline.reproductions.readiness as readiness
from titanskies_pipeline.reproductions.preflight import PreflightMetrics, load_profile
from titanskies_pipeline.reproductions.readiness import (
    ResolutionMetrics,
    canonical_era5_requests,
    resolve_reproduction_sources,
)

ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "config" / "reproductions"
CATALOGS = ROOT / "tests" / "fixtures" / "reproductions" / "readiness_catalogs.json"


def _record() -> dict[str, str]:
    return {
        "url": "https://example.test/evidence",
        "retrieved_at": "2025-01-01T00:00:00Z",
        "sha256": "a" * 64,
    }


def _write_bundle(tmp_path: Path, profile: str, sources: list[dict]) -> Path:
    path = tmp_path / "evidence.json"
    path.write_text(
        json.dumps(
            {
                "format": "reproduction-resolution-v1",
                "profile_id": profile,
                "sources": sources,
            }
        )
    )
    return path


def _response(status: int, payload: dict, **headers: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(payload).encode()
    response.headers.update(headers)
    response.url = "https://example.test/catalog"
    return response


def _epa_catalog(*, years: tuple[int, ...] = (2023, 2024)) -> dict:
    return {
        "items": [
            {
                "filename": f"Emissions-Hourly-{year}-Q{quarter}.csv",
                "s3Path": (
                    f"emissions/hourly/quarter/Emissions-Hourly-{year}-Q{quarter}.csv"
                ),
                "bytes": 10,
                "lastUpdated": f"{year}-12-31T00:00:0{quarter}Z",
                "metadata": {
                    "dataType": "Emissions",
                    "dataSubType": "Hourly",
                    "grouping": "Quarterly",
                },
            }
            for year in years
            for quarter in range(1, 5)
        ]
    }


class FakeSession:
    def __init__(self, responses: list[requests.Response]):
        self.responses = responses
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


@pytest.mark.parametrize(
    ("preflight_status", "operator_count", "definitive_count", "expected_exit"),
    [
        ("ready", 0, 0, 0),
        ("blocked", 0, 1, 2),
        ("blocked", 1, 0, 3),
    ],
)
def test_readiness_cli_exit_contract(
    monkeypatch,
    tmp_path,
    preflight_status,
    operator_count,
    definitive_count,
    expected_exit,
):
    monkeypatch.setattr(readiness_cli, "reset_duckdb_connection_state", lambda: None)
    monkeypatch.setattr(
        readiness_cli,
        "resolve_reproduction_sources",
        lambda *_args, **_kwargs: ResolutionMetrics(
            "sun2025",
            "complete" if expected_exit != 3 else "transient",
            str(tmp_path / "inventory.json"),
            "a" * 64,
            "b" * 64,
            1,
            1,
            1 if expected_exit == 0 else 0,
            operator_count,
            0,
            definitive_count,
        ),
    )
    monkeypatch.setattr(
        readiness_cli,
        "run_preflight",
        lambda *_args, **_kwargs: PreflightMetrics(
            "run",
            "sun2025",
            preflight_status,
            "production",
            True,
            1,
            1,
            1,
            1,
            1,
            0,
            0,
            () if preflight_status == "ready" else ("source: blocker",),
            "c" * 64,
            "d" * 64,
            "e" * 64,
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "resolve_reproduction_sources.py",
            "sun2025",
            "--evidence-bundle",
            str(tmp_path / "evidence.json"),
            "--import-directory",
            str(tmp_path),
            "--output-inventory",
            str(tmp_path / "inventory.json"),
            "--duckdb-path",
            str(tmp_path / "nested" / "readiness.duckdb"),
        ],
    )

    assert readiness_cli.main() == expected_exit
    assert (tmp_path / "nested").is_dir()


def test_resolver_orders_provider_objects_and_is_byte_deterministic(tmp_path):
    objects = [
        {
            "object_id": "b",
            "provider_object_id": "provider-b",
            "provider_revision_id": "2",
            "url": "https://example.test/b",
            "size_bytes": 20,
        },
        {
            "object_id": "a",
            "provider_object_id": "provider-a",
            "provider_revision_id": "1",
            "url": "https://example.test/a",
            "size_bytes": None,
            "size_upper_bound_bytes": 15,
        },
    ]
    source = {
        "source_id": "facility_cohort_14",
        "outcome": "resolved",
        "evidence": [_record()],
        "objects": objects,
    }
    evidence = _write_bundle(tmp_path, "sun2025", [source])
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    first_metrics = resolve_reproduction_sources(
        "sun2025",
        evidence_path=evidence,
        import_dir=tmp_path,
        output_path=first,
    )
    source["objects"] = list(reversed(objects))
    evidence = _write_bundle(tmp_path, "sun2025", [source])
    second_metrics = resolve_reproduction_sources(
        "sun2025",
        evidence_path=evidence,
        import_dir=tmp_path,
        output_path=second,
    )

    assert first.read_bytes() == second.read_bytes()
    assert first_metrics.inventory_sha256 == second_metrics.inventory_sha256
    payload = json.loads(first.read_text())
    canonical_hash = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    ).hexdigest()
    assert first_metrics.inventory_sha256 == canonical_hash
    facility = next(
        item for item in payload["sources"] if item["source_id"] == "facility_cohort_14"
    )
    assert [item["object_id"] for item in facility["objects"]] == ["a", "b"]
    assert payload["inventory_format"] == "reproduction-source-inventory-v2"
    assert "retrieved_at" not in payload


def test_cmr_pagination_deduplicates_revisions_and_preserves_metadata(tmp_path):
    item = {
        "meta": {
            "concept-id": "G1-TEST",
            "revision-id": 2,
            "revision-date": "2024-01-02T00:00:00Z",
        },
        "umm": {
            "GranuleUR": "granule",
            "RelatedUrls": [
                {"Type": "GET DATA", "URL": "https://example.test/granule.zip"}
            ],
            "DataGranule": {
                "ArchiveAndDistributionInformation": [
                    {"Name": "granule.zip"},
                    {
                        "Name": "granule.zip",
                        "Size": 1.5,
                        "SizeUnit": "KB",
                        "Checksum": {
                            "Algorithm": "SHA-256",
                            "Value": "b" * 64,
                        },
                    },
                ]
            },
        },
    }
    session = FakeSession(
        [
            _response(200, {"items": [item] * 2000}),
            _response(200, {"items": [item]}),
        ]
    )
    evidence = _write_bundle(
        tmp_path,
        "sun2025",
        [
            {
                "source_id": "tempo_no2_l2_v03",
                "outcome": "resolved",
                "catalog_url": "https://example.test/cmr",
                "evidence": [_record()],
            }
        ],
    )
    output = tmp_path / "inventory.json"

    metrics = resolve_reproduction_sources(
        "sun2025",
        evidence_path=evidence,
        import_dir=tmp_path,
        output_path=output,
        session=session,
    )

    tempo = next(
        item
        for item in json.loads(output.read_text())["sources"]
        if item["source_id"] == "tempo_no2_l2_v03"
    )
    assert metrics.object_count == 1
    assert tempo["objects"][0]["provider_object_id"] == "G1-TEST"
    assert tempo["objects"][0]["provider_revision_id"] == "2"
    assert tempo["objects"][0]["size_upper_bound_bytes"] == 1536
    assert [call[2]["params"]["page_num"] for call in session.calls] == [1, 2]


def test_cmr_provider_completeness_headers_block_partial_resolution():
    manifest, *_ = load_profile("sun2025")
    source = next(
        item for item in manifest["sources"] if item["id"] == "tempo_no2_l2_v03"
    )
    item = {
        "meta": {
            "concept-id": "G1-TEST",
            "revision-id": 1,
            "revision-date": "2024-01-01T00:00:00Z",
        },
        "umm": {
            "GranuleUR": "granule",
            "RelatedUrls": [
                {"Type": "GET DATA", "URL": "https://example.test/granule.nc"}
            ],
            "DataGranule": {"SizeInBytes": 1},
        },
    }

    partial = _response(200, {"items": [item]}, **{"CMR-Hits": "2"})
    with pytest.raises(readiness._ProviderResolutionError, match="ended after 1 of 2"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([partial]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    timed_out = _response(200, {"items": [item]}, **{"CMR-Time-Out": "true"})
    with pytest.raises(readiness._ProviderResolutionError, match="timed-out partial"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([timed_out]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    malformed = _response(200, {"items": [item]}, **{"CMR-Hits": "invalid"})
    with pytest.raises(ValueError, match="malformed CMR-Hits"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([malformed]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    changed = FakeSession(
        [
            _response(200, {"items": [item] * 2000}, **{"CMR-Hits": "2"}),
            _response(200, {"items": [item]}, **{"CMR-Hits": "3"}),
        ]
    )
    with pytest.raises(readiness._ProviderResolutionError, match="hit count changed"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            changed,
            timeout=1,
            sleep=lambda _delay: None,
        )

    transient = readiness._resolve_source(
        source,
        {
            "outcome": "resolved",
            "catalog_url": "https://example.test/cmr",
            "evidence": [_record()],
        },
        Path("."),
        FakeSession([_response(200, {"items": [item]}, **{"CMR-Hits": "2"})]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert transient["resolution_outcome"] == "transient_error"
    assert "1 of 2" in transient["reason"]

    negative = _response(200, {"items": []}, **{"CMR-Hits": "-1"})
    with pytest.raises(ValueError, match="malformed CMR-Hits"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([negative]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    exceeded = _response(200, {"items": [item]}, **{"CMR-Hits": "0"})
    with pytest.raises(ValueError, match="exceeded its reported hit count"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([exceeded]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    exact = _response(200, {"items": [item]}, **{"CMR-Hits": "1"})
    assert (
        len(
            readiness._cmr_objects(
                source,
                {"catalog_url": "https://example.test/cmr"},
                FakeSession([exact]),
                timeout=1,
                sleep=lambda _delay: None,
            )
        )
        == 1
    )

    second_item = {
        **item,
        "meta": {**item["meta"], "concept-id": "G2-TEST"},
        "umm": {**item["umm"], "GranuleUR": "granule-2"},
    }
    complete_pages = FakeSession(
        [
            _response(200, {"items": [item] * 2000}, **{"CMR-Hits": "2"}),
            _response(200, {"items": [second_item]}, **{"CMR-Hits": "2"}),
        ]
    )
    assert (
        len(
            readiness._cmr_objects(
                source,
                {"catalog_url": "https://example.test/cmr"},
                complete_pages,
                timeout=1,
                sleep=lambda _delay: None,
            )
        )
        == 2
    )


def test_provider_retry_after_and_exhaustion_are_sanitized(tmp_path, monkeypatch):
    monkeypatch.setenv("PLUMEGRAPH_EPA_API_KEY", "test-only-secret")
    session = FakeSession(
        [
            _response(429, {}, **{"Retry-After": "0"}),
            _response(503, {}),
            _response(503, {}),
            _response(503, {}),
        ]
    )
    sleeps: list[float] = []
    evidence = _write_bundle(
        tmp_path,
        "sun2025",
        [
            {
                "source_id": "epa_camd_hourly",
                "outcome": "resolved",
                "catalog_url": "https://example.test/epa",
                "evidence": [_record()],
            }
        ],
    )
    output = tmp_path / "inventory.json"

    metrics = resolve_reproduction_sources(
        "sun2025",
        evidence_path=evidence,
        import_dir=tmp_path,
        output_path=output,
        session=session,
        sleep=sleeps.append,
    )

    epa = next(
        item
        for item in json.loads(output.read_text())["sources"]
        if item["source_id"] == "epa_camd_hourly"
    )
    assert metrics.transient_error_count == 1
    assert epa["resolution_outcome"] == "transient_error"
    assert epa["reason"].endswith("HTTP 503")
    assert sleeps == [0.0, 2.0, 4.0]


def test_import_hash_identity_and_path_confinement(tmp_path):
    import_dir = tmp_path / "imports"
    import_dir.mkdir()
    cohort = CONFIG / "sun2025_facilities.csv"
    imported = import_dir / cohort.name
    imported.write_bytes(cohort.read_bytes())
    checksum = hashlib.sha256(imported.read_bytes()).hexdigest()
    evidence_source = {
        "source_id": "facility_cohort_14",
        "outcome": "resolved",
        "import_filename": cohort.name,
        "import_sha256": checksum,
        "canonical_url": "https://example.test/cohort.csv",
        "source_revision": "v1",
        "evidence": [_record()],
    }
    evidence = _write_bundle(tmp_path, "sun2025", [evidence_source])

    metrics = resolve_reproduction_sources(
        "sun2025",
        evidence_path=evidence,
        import_dir=import_dir,
        output_path=tmp_path / "inventory.json",
    )
    assert metrics.resolved_source_count == 1

    evidence_source["import_filename"] = "../outside.csv"
    evidence = _write_bundle(tmp_path, "sun2025", [evidence_source])
    with pytest.raises(ValueError, match="cannot escape"):
        resolve_reproduction_sources(
            "sun2025",
            evidence_path=evidence,
            import_dir=import_dir,
            output_path=tmp_path / "bad.json",
        )

    evidence_source["import_filename"] = cohort.name
    evidence_source["import_sha256"] = "0" * 64
    evidence = _write_bundle(tmp_path, "sun2025", [evidence_source])
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        resolve_reproduction_sources(
            "sun2025",
            evidence_path=evidence,
            import_dir=import_dir,
            output_path=tmp_path / "mismatch.json",
        )


def test_evidence_rejects_secrets_malformed_hashes_and_unknown_sources(tmp_path):
    cases = [
        (
            {
                "source_id": "facility_cohort_14",
                "outcome": "resolved",
                "token": "secret",
                "evidence": [_record()],
            },
            "Secret-bearing",
        ),
        (
            {
                "source_id": "facility_cohort_14",
                "outcome": "resolved",
                "evidence": [{**_record(), "sha256": "bad"}],
            },
            "invalid SHA-256",
        ),
        (
            {
                "source_id": "unknown",
                "outcome": "resolved",
                "evidence": [_record()],
                "objects": [],
            },
            "unknown sources",
        ),
    ]
    for index, (source, match) in enumerate(cases):
        case = tmp_path / str(index)
        case.mkdir()
        evidence = _write_bundle(case, "sun2025", [source])
        with pytest.raises(ValueError, match=match):
            resolve_reproduction_sources(
                "sun2025",
                evidence_path=evidence,
                import_dir=case,
                output_path=case / "inventory.json",
            )


def test_git_cutoff_and_full_commit_identity_are_enforced(tmp_path):
    repository = {
        "repository": "example/project",
        "commit_sha": "c" * 40,
        "license": "MIT",
        "size_upper_bound_bytes": 1000,
        "metadata_url": "https://example.test/commit",
    }
    evidence = _write_bundle(
        tmp_path,
        "andreadis2025",
        [
            {
                "source_id": "confluence_code",
                "outcome": "resolved",
                "repositories": [repository],
                "evidence": [_record()],
            }
        ],
    )
    late = _response(
        200,
        {
            "sha": "c" * 40,
            "commit": {"committer": {"date": "2024-10-25T00:00:00Z"}},
        },
    )
    with pytest.raises(ValueError, match="violates the paper cutoff"):
        resolve_reproduction_sources(
            "andreadis2025",
            evidence_path=evidence,
            import_dir=tmp_path,
            output_path=tmp_path / "inventory.json",
            session=FakeSession([late]),
        )


def test_geos_v1_and_epa_year_coverage_are_exact_contracts(tmp_path):
    geos_source = {
        "source_id": "geos_cf_2024",
        "outcome": "resolved",
        "evidence": [_record()],
        "objects": [
            {
                "object_id": "geos.nc",
                "url": "https://example.test/geos.nc",
                "size_upper_bound_bytes": 100,
                "dataset_family": "GEOS-CF v2",
                "schema_fingerprint": "schema",
            }
        ],
    }
    evidence = _write_bundle(tmp_path, "sun2025", [geos_source])
    with pytest.raises(ValueError, match="v1 family"):
        resolve_reproduction_sources(
            "sun2025",
            evidence_path=evidence,
            import_dir=tmp_path,
            output_path=tmp_path / "geos.json",
        )

    epa_source = {
        "source_id": "epa_camd_hourly",
        "outcome": "resolved",
        "evidence": [_record()],
        "objects": [
            {
                "object_id": f"Emissions-Hourly-2024-Q{quarter}.csv",
                "provider_object_id": f"epa-2024-q{quarter}",
                "provider_revision_id": f"revision-{quarter}",
                "url": f"https://example.test/camd-2024-q{quarter}.csv",
                "size_bytes": 100,
                "year": 2024,
                "quarter": quarter,
            }
            for quarter in range(1, 5)
        ],
    }
    evidence = _write_bundle(tmp_path, "sun2025", [epa_source])
    with pytest.raises(ValueError, match="requested quarters"):
        resolve_reproduction_sources(
            "sun2025",
            evidence_path=evidence,
            import_dir=tmp_path,
            output_path=tmp_path / "epa.json",
        )


def test_l4_object_revision_must_not_exceed_paper_cutoff(tmp_path):
    evidence = _write_bundle(
        tmp_path,
        "andreadis2025",
        [
            {
                "source_id": "swot_l4_sos_paper_snapshot",
                "outcome": "resolved",
                "evidence_summary": {"contains_gauge_priors": True},
                "evidence": [_record()],
                "objects": [
                    {
                        "object_id": "sos.nc",
                        "url": "https://example.test/sos.nc",
                        "size_bytes": 100,
                        "source_revision": "2024-10-25T00:00:00Z",
                    }
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="violates the paper cutoff"):
        resolve_reproduction_sources(
            "andreadis2025",
            evidence_path=evidence,
            import_dir=tmp_path,
            output_path=tmp_path / "l4.json",
        )


def test_canonical_era5_requests_cover_each_facility_month_deterministically(
    tmp_path,
):
    manifest, _manifest_hash, _contract_hash = load_profile("sun2025")
    source = next(
        item for item in manifest["sources"] if item["id"] == "era5_single_levels"
    )
    cohort = CONFIG / "sun2025_facilities.csv"
    rows = list(csv.DictReader(cohort.read_text().splitlines()))
    reversed_path = tmp_path / "reversed.csv"
    with reversed_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0])
        writer.writeheader()
        writer.writerows(reversed(rows))

    normal = canonical_era5_requests(source, cohort)
    reversed_requests = canonical_era5_requests(source, reversed_path)

    assert normal == reversed_requests
    assert len(normal) == 14 * 17
    assert normal[0]["window_start"] == "2023-08-02T00:00:00Z"
    assert normal[-1]["window_end_exclusive"] == "2025-01-01T00:00:00Z"
    assert len(normal[0]["hours"]) == 24


def test_tracked_sun_cohort_has_reviewed_camd_crosswalks_and_locators():
    rows = list(
        csv.DictReader((CONFIG / "sun2025_facilities.csv").read_text().splitlines())
    )
    assert len(rows) == 14
    assert {row["camd_facility_id"] for row in rows} == {
        "1356",
        "1364",
        "1379",
        "2103",
        "2167",
        "2168",
        "3470",
        "6076",
        "6077",
        "6146",
        "6204",
        "6257",
        "6705",
        "8042",
    }
    assert all(row["source_locator"] for row in rows)
    assert all(row["crosswalk_source"].startswith("https://") for row in rows)
    assert all(float(row["analysis_half_width_km"]) == 15 for row in rows)


def test_synthetic_replay_catalog_covers_all_resolver_surfaces():
    catalogs = json.loads(CATALOGS.read_text())
    assert set(catalogs) == {
        "cmr",
        "zenodo",
        "epa",
        "geos_cf",
        "cds_import",
        "l4",
        "git",
    }
    serialized = json.dumps(catalogs)
    assert "example.test" in serialized
    assert "earthdata.nasa.gov" not in serialized


def test_json_url_timestamp_and_evidence_validation_errors(tmp_path):
    with pytest.raises(ValueError, match="Cannot read"):
        readiness._read_json(tmp_path / "missing.json", label="test")
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{")
    with pytest.raises(ValueError, match="Invalid JSON"):
        readiness._read_json(malformed, label="test")
    scalar = tmp_path / "scalar.json"
    scalar.write_text("[]")
    with pytest.raises(ValueError, match="must contain a JSON object"):
        readiness._read_json(scalar, label="test")

    for url, message in [
        ("relative", "absolute HTTP"),
        ("ftp://example.test/file", "absolute HTTP"),
        ("https://user:password@example.test/file", "credentials"),
        ("https://example.test/file?token=secret", "Signed provider"),
    ]:
        with pytest.raises(ValueError, match=message):
            readiness._unsigned_url(url)
    assert (
        readiness._unsigned_url("https://example.test/file?a=hello%20world#fragment")
        == "https://example.test/file?a=hello+world"
    )
    with pytest.raises(ValueError, match="Credential-bearing URL"):
        readiness._assert_secret_safe(["https://user:pass@example.test/file"])
    with pytest.raises(ValueError, match="Signed or secret-bearing URL"):
        readiness._assert_secret_safe(["https://example.test/file?api_key=secret"])
    readiness._assert_secret_safe({"token": "", "authorization": False})

    with pytest.raises(ValueError, match="ISO-8601"):
        readiness._parse_utc("bad", label="time")
    with pytest.raises(ValueError, match="timezone"):
        readiness._parse_utc("2025-01-01T00:00:00", label="time")

    invalid_bundles = [
        ({"profile_id": "sun2025", "sources": []}, "Evidence format"),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "other",
                "sources": [],
            },
            "Evidence profile_id",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": {},
            },
            "sources must be a list",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": ["bad"],
            },
            "source must be an object",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": [{"source_id": "", "outcome": "bad"}],
            },
            "source_id and valid outcome",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": [
                    {
                        "source_id": "one",
                        "outcome": "resolved",
                        "evidence": [_record()],
                    },
                    {
                        "source_id": "one",
                        "outcome": "resolved",
                        "evidence": [_record()],
                    },
                ],
            },
            "repeats source",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": [
                    {"source_id": "one", "outcome": "resolved", "evidence": []}
                ],
            },
            "requires evidence records",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": [
                    {
                        "source_id": "one",
                        "outcome": "resolved",
                        "evidence": ["bad"],
                    }
                ],
            },
            "must be an object",
        ),
        (
            {
                "format": "reproduction-resolution-v1",
                "profile_id": "sun2025",
                "sources": [
                    {
                        "source_id": "one",
                        "outcome": "resolved",
                        "evidence": [{**_record(), "url": ""}],
                    }
                ],
            },
            "requires URL",
        ),
    ]
    for bundle, message in invalid_bundles:
        with pytest.raises(ValueError, match=message):
            readiness._validate_evidence("sun2025", bundle)


def test_cohort_validation_rejects_each_contract_violation():
    cohort = (CONFIG / "sun2025_facilities.csv").read_bytes()
    rows = list(csv.DictReader(cohort.decode().splitlines()))

    def encoded(changed_rows, fieldnames=None):
        import io

        handle = io.StringIO()
        writer = csv.DictWriter(handle, fieldnames=fieldnames or list(changed_rows[0]))
        writer.writeheader()
        writer.writerows(changed_rows)
        return handle.getvalue().encode()

    with pytest.raises(ValueError, match="UTF-8"):
        readiness._validate_sun_cohort(b"\xff")
    with pytest.raises(ValueError, match="missing required columns"):
        readiness._validate_sun_cohort(b"name\nfacility\n")
    with pytest.raises(ValueError, match="exactly 14"):
        readiness._validate_sun_cohort(encoded(rows[:-1]))

    duplicate = [dict(row) for row in rows]
    duplicate[-1]["camd_facility_id"] = duplicate[0]["camd_facility_id"]
    with pytest.raises(ValueError, match="unique CAMD"):
        readiness._validate_sun_cohort(encoded(duplicate))
    bad_id = [dict(row) for row in rows]
    bad_id[0]["camd_facility_id"] = "facility"
    with pytest.raises(ValueError, match="unique CAMD"):
        readiness._validate_sun_cohort(encoded(bad_id))
    bad_coordinates = [dict(row) for row in rows]
    bad_coordinates[0]["latitude"] = "91"
    with pytest.raises(ValueError, match="invalid coordinates"):
        readiness._validate_sun_cohort(encoded(bad_coordinates))
    bad_aoi = [dict(row) for row in rows]
    bad_aoi[0]["analysis_half_width_km"] = "0"
    with pytest.raises(ValueError, match="must be positive"):
        readiness._validate_sun_cohort(encoded(bad_aoi))
    missing_locator = [dict(row) for row in rows]
    missing_locator[0]["source_locator"] = ""
    with pytest.raises(ValueError, match="labels, locators"):
        readiness._validate_sun_cohort(encoded(missing_locator))


def test_request_timeout_retry_dates_and_nonretryable_http(monkeypatch):
    class RaisingSession:
        def __init__(self, exception):
            self.exception = exception
            self.calls = 0

        def request(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls < 4:
                raise self.exception
            return _response(200, {"ok": True})

    sleeps: list[float] = []
    response = readiness._request(
        RaisingSession(requests.Timeout()),
        "GET",
        "https://example.test",
        timeout=1,
        sleep=sleeps.append,
    )
    assert response.status_code == 200
    assert sleeps == [1.0, 2.0, 4.0]

    class AlwaysRaisingSession:
        def request(self, *_args, **_kwargs):
            raise requests.ConnectionError()

    with pytest.raises(requests.ConnectionError):
        readiness._request(
            AlwaysRaisingSession(),
            "GET",
            "https://example.test",
            timeout=1,
            sleep=lambda _delay: None,
        )

    bad_request = FakeSession([_response(400, {"error": "bad"})])
    with pytest.raises(requests.HTTPError):
        readiness._request(
            bad_request,
            "GET",
            "https://example.test",
            timeout=1,
            sleep=lambda _delay: None,
        )

    monkeypatch.setattr(
        readiness,
        "parsedate_to_datetime",
        lambda _value: datetime.now(timezone.utc),
    )
    sleeps = []
    readiness._request(
        FakeSession(
            [
                _response(429, {}, **{"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}),
                _response(200, {}),
            ]
        ),
        "GET",
        "https://example.test",
        timeout=1,
        sleep=sleeps.append,
    )
    assert sleeps == [0.0]

    monkeypatch.setattr(
        readiness,
        "parsedate_to_datetime",
        lambda _value: (_ for _ in ()).throw(ValueError("bad")),
    )
    sleeps = []
    readiness._request(
        FakeSession(
            [
                _response(429, {}, **{"Retry-After": "invalid"}),
                _response(200, {}),
            ]
        ),
        "GET",
        "https://example.test",
        timeout=1,
        sleep=sleeps.append,
    )
    assert sleeps == [1.0]


def test_object_and_import_validation_branches(tmp_path):
    for value, message in [
        ({"url": "https://example.test", "size_bytes": 1}, "require ID"),
        (
            {
                "object_id": "a",
                "url": "https://example.test",
                "size_bytes": -1,
            },
            "fit BIGINT",
        ),
        (
            {
                "object_id": "a",
                "url": "https://example.test",
                "size_upper_bound_bytes": 2**63,
            },
            "fit BIGINT",
        ),
        (
            {
                "object_id": "a",
                "url": "https://example.test",
                "size_bytes": None,
            },
            "requires a size",
        ),
        (
            {
                "object_id": "a",
                "url": "https://example.test",
                "size_bytes": 2,
                "size_upper_bound_bytes": 1,
            },
            "invalid upper bound",
        ),
    ]:
        with pytest.raises(ValueError, match=message):
            readiness._normalize_object("source", value)

    evidence = {"evidence": [_record()]}
    with pytest.raises(FileNotFoundError, match="not configured"):
        readiness._objects_from_import("source", evidence, tmp_path)

    payload_path = tmp_path / "import.json"
    payload_path.write_text(json.dumps({"source_id": "wrong", "objects": []}))
    import_evidence = {
        **evidence,
        "import_filename": "import.json",
        "import_sha256": hashlib.sha256(payload_path.read_bytes()).hexdigest(),
    }
    with pytest.raises(ValueError, match="wrong source identity"):
        readiness._objects_from_import("source", import_evidence, tmp_path)

    payload_path.write_text(
        json.dumps(
            {
                "source_id": "source",
                "canonical_requests": ["wrong"],
                "objects": [],
            }
        )
    )
    import_evidence["import_sha256"] = hashlib.sha256(
        payload_path.read_bytes()
    ).hexdigest()
    import_evidence["canonical_requests"] = []
    with pytest.raises(ValueError, match="canonical requests"):
        readiness._objects_from_import("source", import_evidence, tmp_path)

    payload_path.write_text(
        json.dumps(
            {
                "source_id": "source",
                "canonical_requests": [],
                "objects": {},
            }
        )
    )
    import_evidence["import_sha256"] = hashlib.sha256(
        payload_path.read_bytes()
    ).hexdigest()
    with pytest.raises(ValueError, match="objects list"):
        readiness._objects_from_import("source", import_evidence, tmp_path)

    payload_path.write_text(
        json.dumps(
            {
                "source_id": "source",
                "canonical_requests": [],
                "objects": [
                    {
                        "object_id": "imported",
                        "url": "https://example.test/imported",
                        "size_bytes": 1,
                    }
                ],
            }
        )
    )
    import_evidence["import_sha256"] = hashlib.sha256(
        payload_path.read_bytes()
    ).hexdigest()
    assert (
        readiness._objects_from_import("source", import_evidence, tmp_path)[0][
            "object_id"
        ]
        == "imported"
    )

    csv_path = tmp_path / "generic.csv"
    csv_path.write_text("value\none\n")
    generic_evidence = {
        "import_filename": "generic.csv",
        "import_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "canonical_url": "https://example.test/generic.csv",
        "source_revision": "v1",
        "evidence": [_record()],
    }
    assert (
        readiness._objects_from_import("generic", generic_evidence, tmp_path)[0][
            "object_id"
        ]
        == "generic.csv"
    )

    outside = tmp_path.parent / "outside.json"
    outside.write_text("{}")
    link = tmp_path / "outside-link.json"
    link.symlink_to(outside)
    with pytest.raises(ValueError, match="escapes"):
        readiness._validated_import_path(tmp_path, link.name)


def test_static_epa_opendap_and_git_provider_contracts(tmp_path):
    sun, *_ = load_profile("sun2025")
    andreadis, *_ = load_profile("andreadis2025")
    code = next(item for item in sun["sources"] if item["id"] == "sun2025_code")
    zenodo_payload = {
        "revision": 4,
        "updated": "2025-01-01T00:00:00Z",
        "files": [
            {
                "id": "file",
                "key": code["request"]["archive_filename"],
                "size": 10,
                "checksum": f"md5:{code['checksum']}",
                "links": {"self": "https://example.test/code.zip"},
            }
        ],
    }
    objects = readiness._static_archive_objects(
        code,
        {"zenodo_record_id": "1"},
        FakeSession([_response(200, zenodo_payload)]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert objects[0]["provider_revision_id"] == "4"

    for payload, message in [
        ({"files": {}}, "Malformed Zenodo"),
        ({"files": []}, "exactly one file"),
    ]:
        with pytest.raises(ValueError, match=message):
            readiness._static_archive_objects(
                code,
                {"zenodo_record_id": "1"},
                FakeSession([_response(200, payload)]),
                timeout=1,
                sleep=lambda _delay: None,
            )

    head_source = {
        "id": "archive",
        "url": "https://example.test/archive.zip",
        "request": {"archive_filename": "archive.zip"},
        "checksum_algorithm": "md5",
        "checksum": "a" * 32,
    }
    head = _response(200, {})
    head.headers.update({"Content-Length": "12", "ETag": "etag"})
    objects = readiness._static_archive_objects(
        head_source,
        {"provider_object_id": "provider", "provider_revision_id": "revision"},
        FakeSession([head]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert objects[0]["size_bytes"] == 12
    assert objects[0]["etag"] == "etag"

    epa = next(item for item in sun["sources"] if item["id"] == "epa_camd_hourly")
    epa_payload = _epa_catalog()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("PLUMEGRAPH_EPA_API_KEY", "secret")
    try:
        epa_objects = readiness._epa_objects(
            epa,
            {"catalog_url": "https://example.test/epa"},
            FakeSession([_response(200, epa_payload)]),
            timeout=1,
            sleep=lambda _delay: None,
        )
        assert len(epa_objects) == 8
        assert epa_objects[0]["provider_object_id"].startswith(
            "emissions/hourly/quarter/"
        )
        assert epa_objects[0]["provider_revision_id"].startswith("2023-")
        assert epa_objects[0]["url"].startswith(
            "https://api.epa.gov/easey/bulk-files/emissions/hourly/quarter/"
        )
    finally:
        monkeypatch.undo()
    for payload, message in [
        ({"items": {}}, "Malformed provider"),
        (
            _epa_catalog(years=(2024,)),
            "missing requested quarters",
        ),
        (
            {
                "items": [
                    {
                        **item,
                        "lastUpdated": "",
                    }
                    for item in _epa_catalog()["items"]
                ]
            },
            "missing identity or revision",
        ),
    ]:
        with pytest.raises(ValueError, match=message):
            readiness._epa_objects(
                epa,
                {"catalog_url": "https://example.test/epa"},
                FakeSession([_response(200, payload)]),
                timeout=1,
                sleep=lambda _delay: None,
            )

    with pytest.raises(ValueError, match="Malformed provider"):
        readiness._epa_objects(
            epa,
            {"catalog_url": "https://example.test/epa"},
            FakeSession([_response(200, {"items": ["not-an-object"]})]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    duplicate_quarter = _epa_catalog()
    duplicate_quarter["items"].append(dict(duplicate_quarter["items"][0]))
    with pytest.raises(ValueError, match="repeats or includes unexpected"):
        readiness._epa_objects(
            epa,
            {"catalog_url": "https://example.test/epa"},
            FakeSession([_response(200, duplicate_quarter)]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    inconsistent = _epa_catalog()
    inconsistent["items"][0]["metadata"]["grouping"] = "State"
    with pytest.raises(ValueError, match="inconsistent grouping"):
        readiness._epa_objects(
            epa,
            {"catalog_url": "https://example.test/epa"},
            FakeSession([_response(200, inconsistent)]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    with_irrelevant = _epa_catalog()
    with_irrelevant["items"][:0] = [
        {
            "filename": "Facility-Attributes.csv",
            "s3Path": "facility/Facility-Attributes.csv",
            "bytes": 1,
            "lastUpdated": "2025-01-01T00:00:00Z",
            "metadata": {},
        },
        {
            "filename": "Emissions-Hourly-2022-Q1.csv",
            "s3Path": "emissions/hourly/quarter/Emissions-Hourly-2022-Q1.csv",
            "bytes": 1,
            "lastUpdated": "2025-01-01T00:00:00Z",
            "metadata": {},
        },
    ]
    assert (
        len(
            readiness._epa_objects(
                epa,
                {
                    "catalog_url": "https://example.test/epa",
                    "download_base_url": "https://example.test/bulk/",
                },
                FakeSession([_response(200, with_irrelevant)]),
                timeout=1,
                sleep=lambda _delay: None,
            )
        )
        == 8
    )

    geos = next(item for item in sun["sources"] if item["id"] == "geos_cf_2024")
    geos_payload = {
        "dataset_family": "GEOS-CF v1",
        "variables": ["nitrogen_dioxide"],
        "year": 2024,
        "dimensions": {"time": 1},
        "dtypes": {"nitrogen_dioxide": "float32"},
        "revision": "v1-paper",
        "objects": [
            {
                "object_id": "geos",
                "url": "https://example.test/geos",
                "size_upper_bound_bytes": 10,
            }
        ],
    }
    objects = readiness._opendap_objects(
        geos,
        {"catalog_url": "https://example.test/geos"},
        FakeSession([_response(200, geos_payload)]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert objects[0]["schema_fingerprint"]
    assert objects[0]["provider_revision_id"] == "v1-paper"
    geos_cases = [
        ({**geos_payload, "dataset_family": "GEOS-CF v2"}, "pinned v1"),
        ({**geos_payload, "variables": []}, "required variables"),
        ({**geos_payload, "year": 2023}, "required year"),
        ({**geos_payload, "dimensions": []}, "schema/revision"),
        ({**geos_payload, "objects": {}}, "Malformed provider"),
    ]
    for payload, message in geos_cases:
        with pytest.raises(ValueError, match=message):
            readiness._opendap_objects(
                geos,
                {"catalog_url": "https://example.test/geos"},
                FakeSession([_response(200, payload)]),
                timeout=1,
                sleep=lambda _delay: None,
            )

    git = next(item for item in andreadis["sources"] if item["id"] == "confluence_code")
    repository = {
        "repository": "example/project",
        "commit_sha": "c" * 40,
        "license": "MIT",
        "size_upper_bound_bytes": 10,
        "metadata_url": "https://example.test/commit",
    }
    response = _response(
        200,
        {
            "sha": "c" * 40,
            "commit": {"committer": {"date": "2024-10-23T00:00:00Z"}},
        },
    )
    assert (
        readiness._git_objects(
            git,
            {"repositories": [repository]},
            FakeSession([response]),
            timeout=1,
            sleep=lambda _delay: None,
        )[0]["license"]
        == "MIT"
    )

    with pytest.raises(ValueError, match="requires repositories"):
        readiness._git_objects(
            git,
            {"repositories": []},
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )
    for repository_update, payload, message in [
        ({"commit_sha": "short"}, None, "full commit SHA"),
        ({}, {"sha": "d" * 40}, "wrong commit"),
        (
            {"license": ""},
            {
                "sha": "c" * 40,
                "commit": {"committer": {"date": "2024-10-23T00:00:00Z"}},
            },
            "requires a licence",
        ),
    ]:
        changed = {**repository, **repository_update}
        with pytest.raises(ValueError, match=message):
            readiness._git_objects(
                git,
                {"repositories": [changed]},
                FakeSession([_response(200, payload)]) if payload else FakeSession([]),
                timeout=1,
                sleep=lambda _delay: None,
            )


def test_cmr_malformed_identity_version_time_url_and_size_branches():
    sun, *_ = load_profile("sun2025")
    source = next(item for item in sun["sources"] if item["id"] == "tempo_no2_l2_v03")

    def cmr(item, evidence=None):
        return readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr", **(evidence or {})},
            FakeSession([_response(200, {"items": [item]})]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    with pytest.raises(ValueError, match="Malformed CMR"):
        readiness._cmr_objects(
            source,
            {"catalog_url": "https://example.test/cmr"},
            FakeSession([_response(200, {"items": {}})]),
            timeout=1,
            sleep=lambda _delay: None,
        )
    with pytest.raises(ValueError, match="lacks identity"):
        cmr({"meta": {}, "umm": {}})

    def item(
        *,
        version="V03",
        beginning="2024-01-01T00:00:00Z",
        ending="2024-01-01T01:00:00Z",
        urls=True,
        data_granule=None,
    ):
        return {
            "meta": {
                "concept-id": "G1-TEST",
                "revision-id": 1,
                "revision-date": "2024-01-01T00:00:00Z",
                "format": "umm",
            },
            "umm": {
                "GranuleUR": "granule",
                "CollectionReference": {
                    "ShortName": "TEMPO_NO2_L2",
                    "Version": version,
                },
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": beginning,
                        "EndingDateTime": ending,
                    }
                },
                "RelatedUrls": (
                    [
                        {
                            "Type": "GET DATA",
                            "URL": "https://example.test/granule.nc",
                        }
                    ]
                    if urls
                    else []
                ),
                "DataGranule": data_granule or {},
            },
        }

    with pytest.raises(ValueError, match="collection version"):
        cmr(item(version="V04"))
    with pytest.raises(ValueError, match="outside the request window"):
        cmr(
            item(
                beginning="2025-01-01T00:00:00Z",
                ending="2025-01-01T01:00:00Z",
            )
        )
    with pytest.raises(ValueError, match="lacks a canonical URL"):
        cmr(item(urls=False))
    assert cmr(item(data_granule={"SizeMB": 1}))[0]["size_upper_bound_bytes"] == 1024**2
    with pytest.raises(ValueError, match="unsupported size unit"):
        cmr(
            item(
                data_granule={
                    "ArchiveAndDistributionInformation": [
                        {
                            "Name": "granule.nc",
                            "Size": 1,
                            "SizeUnit": "BLOCKS",
                        }
                    ]
                }
            )
        )
    bounded = cmr(item(), {"size_upper_bound_bytes": 999})[0]
    assert bounded["size_upper_bound_bytes"] == 999
    checksummed = cmr(
        item(
            data_granule={
                "Checksum": {"Algorithm": "MD5", "Value": "a" * 32},
                "SizeInBytes": 5,
            }
        )
    )[0]
    assert checksummed["checksum"] == "a" * 32
    exact_archive = cmr(
        item(
            data_granule={
                "ArchiveAndDistributionInformation": [
                    {
                        "Name": "granule.nc",
                        "Size": 0.1,
                        "SizeUnit": "MB",
                        "SizeInBytes": 7,
                    }
                ]
            }
        )
    )[0]
    assert exact_archive["size_bytes"] == 7


def test_catalog_and_dispatch_branches(tmp_path, monkeypatch):
    sun, *_ = load_profile("sun2025")
    epa = next(item for item in sun["sources"] if item["id"] == "epa_camd_hourly")
    geos = next(item for item in sun["sources"] if item["id"] == "geos_cf_2024")
    code = next(item for item in sun["sources"] if item["id"] == "sun2025_code")

    with pytest.raises(ValueError, match="Malformed provider catalog"):
        readiness._catalog_payload(
            epa,
            {"catalog_url": "https://example.test/catalog"},
            FakeSession([_response(200, [])]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    inferred_years = _epa_catalog()
    monkeypatch.setenv("PLUMEGRAPH_EPA_API_KEY", "secret")
    assert (
        len(
            readiness._epa_objects(
                epa,
                {"catalog_url": "https://example.test/epa"},
                FakeSession([_response(200, inferred_years)]),
                timeout=1,
                sleep=lambda _delay: None,
            )
        )
        == 8
    )

    direct_epa = [
        {
            "object_id": f"Emissions-Hourly-{year}-Q{quarter}.csv",
            "provider_object_id": f"epa-{year}-q{quarter}",
            "provider_revision_id": f"revision-{year}-{quarter}",
            "url": f"https://example.test/{year}/{quarter}",
            "size_bytes": 1,
            "year": year,
            "quarter": quarter,
        }
        for year in (2023, 2024)
        for quarter in range(1, 5)
    ]
    assert (
        len(
            readiness._resolve_objects(
                epa,
                {"objects": direct_epa},
                tmp_path,
                FakeSession([]),
                timeout=1,
                sleep=lambda _delay: None,
            )
        )
        == 8
    )
    with pytest.raises(ValueError, match="must be a list"):
        readiness._resolve_objects(
            geos,
            {"objects": {}},
            tmp_path,
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )
    assert readiness._resolve_objects(
        geos,
        {
            "objects": [
                {
                    "object_id": "geos",
                    "url": "https://example.test/geos",
                    "size_upper_bound_bytes": 1,
                    "dataset_family": "GEOS-CF v1",
                    "schema_fingerprint": "schema",
                }
            ]
        },
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )

    zenodo_payload = {
        "revision": 1,
        "files": [
            {
                "id": "file",
                "key": code["request"]["archive_filename"],
                "size": 1,
                "checksum": f"md5:{code['checksum']}",
                "links": {"self": "https://example.test/code.zip"},
            }
        ],
    }
    assert readiness._resolve_objects(
        code,
        {"zenodo_record_id": "1"},
        tmp_path,
        FakeSession([_response(200, zenodo_payload)]),
        timeout=1,
        sleep=lambda _delay: None,
    )

    geos_payload = {
        "dataset_family": "GEOS-CF v1",
        "variables": ["nitrogen_dioxide"],
        "year": 2024,
        "dimensions": {"time": 1},
        "dtypes": {"nitrogen_dioxide": "float32"},
        "revision": "v1",
        "objects": [
            {
                "object_id": "geos",
                "url": "https://example.test/geos",
                "size_upper_bound_bytes": 1,
            }
        ],
    }
    assert readiness._resolve_objects(
        geos,
        {"catalog_url": "https://example.test/geos"},
        tmp_path,
        FakeSession([_response(200, geos_payload)]),
        timeout=1,
        sleep=lambda _delay: None,
    )


def test_resolve_source_transient_conflict_and_cutoff_branches(tmp_path):
    sun, *_ = load_profile("sun2025")
    andreadis, *_ = load_profile("andreadis2025")
    epa = next(item for item in sun["sources"] if item["id"] == "epa_camd_hourly")
    base_evidence = {"outcome": "resolved", "evidence": [_record()]}
    result = readiness._resolve_source(
        epa,
        base_evidence,
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "operator_input_required"

    facility = next(
        item for item in sun["sources"] if item["id"] == "facility_cohort_14"
    )
    result = readiness._resolve_source(
        facility,
        base_evidence,
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "operator_input_required"

    tempo = next(item for item in sun["sources"] if item["id"] == "tempo_no2_l2_v03")
    era5 = next(item for item in sun["sources"] if item["id"] == "era5_single_levels")
    result = readiness._resolve_source(
        era5,
        base_evidence,
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "operator_input_required"
    no_cohort_source = {
        **era5,
        "request": {
            key: value
            for key, value in era5["request"].items()
            if key != "facility_cohort"
        },
    }
    result = readiness._resolve_source(
        no_cohort_source,
        base_evidence,
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "operator_input_required"
    (tmp_path / "sun2025_facilities.csv").write_bytes(
        (CONFIG / "sun2025_facilities.csv").read_bytes()
    )
    result = readiness._resolve_source(
        era5,
        base_evidence,
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "operator_input_required"

    result = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        FakeSession([_response(200, {"items": []})]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "transient_error"

    result = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        FakeSession([_response(503, {})] * 4),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "transient_error"
    result = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        FakeSession(
            [
                _response(200, {"items": []}),
            ]
        ),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["reason"] == "provider returned no exact objects"

    nonretryable = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        FakeSession([_response(400, {})]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert nonretryable["resolution_outcome"] == "transient_error"
    assert nonretryable["reason"].endswith("HTTP 400")
    unauthorized = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        FakeSession([_response(403, {})]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert unauthorized["resolution_outcome"] == "operator_input_required"
    invalid_json = requests.Response()
    invalid_json.status_code = 200
    invalid_json._content = b"{"
    invalid_json.url = "https://example.test/cmr"
    with pytest.raises(requests.exceptions.InvalidJSONError):
        readiness._resolve_source(
            tempo,
            {**base_evidence, "catalog_url": "https://example.test/cmr"},
            tmp_path,
            FakeSession([invalid_json]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    class ConnectionFailure:
        def request(self, *_args, **_kwargs):
            raise requests.ConnectionError()

    result = readiness._resolve_source(
        tempo,
        {**base_evidence, "catalog_url": "https://example.test/cmr"},
        tmp_path,
        ConnectionFailure(),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["resolution_outcome"] == "transient_error"

    duplicate = {
        "object_id": "same",
        "provider_revision_id": "revision",
        "url": "https://example.test/object",
        "size_bytes": 1,
    }
    with pytest.raises(ValueError, match="conflicting identities"):
        readiness._resolve_source(
            facility,
            {**base_evidence, "objects": [duplicate, duplicate]},
            tmp_path,
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    conflicting_provider_revision = [
        {
            "object_id": object_id,
            "provider_object_id": "provider-object",
            "provider_revision_id": "revision-1",
            "url": f"https://example.test/{object_id}",
            "size_bytes": 1,
        }
        for object_id in ("first", "second")
    ]
    with pytest.raises(ValueError, match="conflicting provider revisions"):
        readiness._resolve_source(
            facility,
            {**base_evidence, "objects": conflicting_provider_revision},
            tmp_path,
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    with pytest.raises(ValueError, match="immutable revision identities"):
        readiness._resolve_source(
            facility,
            {
                **base_evidence,
                "objects": [
                    {
                        "object_id": "cohort",
                        "url": "https://example.test/cohort",
                        "size_bytes": 1,
                    }
                ],
            },
            tmp_path,
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    l4 = next(
        item
        for item in andreadis["sources"]
        if item["id"] == "swot_l4_sos_paper_snapshot"
    )
    with pytest.raises(ValueError, match="authoritative revision time"):
        readiness._resolve_source(
            l4,
            {
                **base_evidence,
                "objects": [
                    {
                        "object_id": "l4",
                        "url": "https://example.test/l4",
                        "size_bytes": 1,
                    }
                ],
            },
            tmp_path,
            FakeSession([]),
            timeout=1,
            sleep=lambda _delay: None,
        )

    result = readiness._resolve_source(
        l4,
        {
            **base_evidence,
            "objects": [
                {
                    "object_id": "l4",
                    "url": "https://example.test/l4",
                    "size_bytes": 1,
                    "source_revision": "2024-10-24T00:00:00Z",
                }
            ],
        },
        tmp_path,
        FakeSession([]),
        timeout=1,
        sleep=lambda _delay: None,
    )
    assert result["objects"]


def test_resolver_top_level_validation_and_complete_status(tmp_path):
    with pytest.raises(ValueError, match="timeout_seconds"):
        resolve_reproduction_sources(
            "sun2025",
            evidence_path=CONFIG / "sun2025_resolution.json",
            import_dir=tmp_path,
            output_path=tmp_path / "inventory.json",
            timeout_seconds=0,
        )

    manifest, *_ = load_profile("sun2025")
    minimal = {
        **manifest,
        "scientific_contract": "contract.csv",
        "resolution_evidence": "evidence.json",
        "sources": [
            next(
                item
                for item in manifest["sources"]
                if item["id"] == "facility_cohort_14"
            )
        ],
    }
    (tmp_path / "contract.csv").write_text("contract_key,value\nversion,1\n")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(minimal))
    source = {
        "source_id": "facility_cohort_14",
        "outcome": "resolved",
        "evidence": [_record()],
        "objects": [
            {
                "object_id": "cohort",
                "provider_revision_id": "revision-1",
                "url": "https://example.test/cohort",
                "size_bytes": 1,
            }
        ],
    }
    evidence_path = _write_bundle(tmp_path, "sun2025", [source])
    metrics = resolve_reproduction_sources(
        "sun2025",
        manifest_path=manifest_path,
        evidence_path=evidence_path,
        import_dir=tmp_path,
        output_path=tmp_path / "complete.json",
    )
    assert metrics.status == "complete"
