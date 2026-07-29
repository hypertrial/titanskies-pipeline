from __future__ import annotations

import json
from datetime import date, datetime, timezone

import duckdb
import pyarrow.parquet as pq
import pytest

from titanskies_pipeline.plumegraph import sources
from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)

UTC = timezone.utc


@pytest.fixture
def conn():
    connection = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(connection)
    yield connection
    connection.close()


def _one_facility_manifest(path, **document_updates):
    facility = {
        "facility_id": "1",
        "facility_name": "One",
        "latitude": 35,
        "longitude": -100,
        "timezone": "America/Chicago",
        "utc_standard_offset_minutes": -360,
        "annual_nox_tons": 1,
        "is_cohort": True,
        "review_status": "approved",
        "inclusion_reason": "reviewed",
    }
    document = {
        "schema_version": "plumegraph-cohort-v1",
        "cohort_version": "v1",
        "review_status": "approved",
        "approved_by": "scientist",
        "facilities": [facility],
        **document_updates,
    }
    path.write_text(json.dumps(document))
    return path


def test_month_windows_are_half_open_and_cross_year():
    start = datetime(2024, 12, 15, tzinfo=UTC)
    end = datetime(2025, 2, 2, tzinfo=UTC)
    assert sources._month_windows(start, end) == [
        (start, datetime(2025, 1, 1, tzinfo=UTC)),
        (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)),
        (datetime(2025, 2, 1, tzinfo=UTC), end),
    ]
    with pytest.raises(ValueError, match="ordered timezone-aware"):
        sources._month_windows(datetime(2024, 1, 1), end)
    with pytest.raises(ValueError, match="ordered timezone-aware"):
        sources._month_windows(end, start)
    with pytest.raises(ValueError, match="timezone"):
        sources._db_time(datetime(2024, 1, 1))
    assert sources._utc_now().tzinfo == UTC


def test_cohort_manifest_validation(tmp_path):
    path = _one_facility_manifest(tmp_path / "cohort.json")
    version, facilities, _ = sources.load_cohort_manifest(
        path,
        expected_cohort_count=1,
    )
    assert version == "v1"
    assert facilities[0].annual_nox_tons == 1

    path.write_text("[]")
    with pytest.raises(ValueError, match="JSON object"):
        sources.load_cohort_manifest(path, expected_cohort_count=1)
    _one_facility_manifest(path, schema_version="bad")
    with pytest.raises(ValueError, match="Unsupported"):
        sources.load_cohort_manifest(path, expected_cohort_count=1)
    _one_facility_manifest(path, cohort_version="")
    with pytest.raises(ValueError, match="version and facilities"):
        sources.load_cohort_manifest(path, expected_cohort_count=1)
    _one_facility_manifest(path, review_status="draft", approved_by=None)
    with pytest.raises(ValueError, match="explicitly approved"):
        sources.load_cohort_manifest(path, expected_cohort_count=1)
    _one_facility_manifest(path, facilities=["bad"])
    with pytest.raises(ValueError, match="entries must be objects"):
        sources.load_cohort_manifest(
            path,
            expected_cohort_count=1,
            require_approved=False,
        )

    _one_facility_manifest(path)
    document = json.loads(path.read_text())
    document["facilities"][0]["annual_nox_tons"] = ""
    document["facilities"].append(dict(document["facilities"][0]))
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="invalid facility"):
        sources.load_cohort_manifest(path, expected_cohort_count=2)
    document["facilities"] = [document["facilities"][0]]
    path.write_text(json.dumps(document))
    _, facilities, _ = sources.load_cohort_manifest(path, expected_cohort_count=1)
    assert facilities[0].annual_nox_tons is None
    with pytest.raises(ValueError, match="count must be positive"):
        sources.load_cohort_manifest(path, expected_cohort_count=0)
    with pytest.raises(ValueError, match="exactly 2"):
        sources.load_cohort_manifest(path, expected_cohort_count=2)


def test_regions_merge_overlaps_and_keep_separate_groups():
    facilities = [
        sources.Facility("a", "A", 35, -100, "UTC", 0, 1, True, "ok", "x"),
        sources.Facility("b", "B", 35, -99.99, "UTC", 0, 1, True, "ok", "x"),
        sources.Facility("c", "C", 45, -80, "UTC", 0, 1, True, "ok", "x"),
        sources.Facility("d", "D", 0, 0, "UTC", 0, 1, False, "ok", "x"),
    ]
    regions = sources.build_analysis_regions(
        facilities,
        cohort_version="v1",
        aoi_radius_km=10,
    )
    assert [region["facility_ids"] for region in regions] == [["a", "b"], ["c"]]
    assert all(region["geometry_wkb"] for region in regions)
    with pytest.raises(ValueError, match="radius"):
        sources.build_analysis_regions(
            facilities,
            cohort_version="v1",
            aoi_radius_km=0,
        )
    with pytest.raises(ValueError, match="require cohort"):
        sources.build_analysis_regions(
            [facilities[-1]],
            cohort_version="v1",
            aoi_radius_km=1,
        )


def test_persist_cohort_is_frozen_and_plans_deterministic_requests(
    tmp_path,
    conn,
):
    path = _one_facility_manifest(tmp_path / "cohort.json")
    metrics = sources.persist_cohort(path, expected_cohort_count=1, conn=conn)
    assert metrics["analysis_regions"] == 1
    assert (
        sources.persist_cohort(
            path,
            expected_cohort_count=1,
            conn=conn,
        )
        == metrics
    )
    start = datetime(2024, 1, 15, tzinfo=UTC)
    end = datetime(2024, 3, 2, tzinfo=UTC)
    first = sources.plan_source_requests(
        window_start=start,
        window_end=end,
        conn=conn,
    )
    assert first.requests_planned == 7
    second = sources.plan_source_requests(
        window_start=start,
        window_end=end,
        conn=conn,
    )
    assert second.requests_planned == 0
    assert second.requests_existing == 7
    requests = sources.pending_source_requests(conn=conn)
    assert {request.connector for request in requests} == {"harmony", "hrrr", "camd"}
    assert len(sources.pending_source_requests(connector="camd", conn=conn)) == 1
    harmony = next(request for request in requests if request.connector == "harmony")
    assert harmony.request["concept_id"] == "C3685896872-LARC_CLOUD"
    assert sources.plan_source_requests(backfill=True, conn=conn).requests_planned > 0

    changed = json.loads(path.read_text())
    changed["facilities"][0]["facility_name"] = "Changed"
    path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="cannot change"):
        sources.persist_cohort(path, expected_cohort_count=1, conn=conn)


def test_plan_requires_cohort_and_backfill_uses_full_2024(conn):
    with pytest.raises(RuntimeError, match="registry is empty"):
        sources.plan_source_requests(
            window_start=datetime(2024, 1, 1, tzinfo=UTC),
            window_end=datetime(2024, 2, 1, tzinfo=UTC),
            conn=conn,
        )


def _request() -> sources.SourceRequest:
    return sources.SourceRequest(
        "request",
        "camd",
        "v1",
        "region",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 2, 1, tzinfo=UTC),
        "contract",
        {"year": 2024},
    )


def _insert_request(conn, request):
    conn.execute(
        """
        insert into plumegraph_events_ops.source_requests
        (request_id, connector, source_version, analysis_region_id,
         window_start, window_end, request_json, request_contract_version,
         status, attempts, planned_at, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, 'planned', 0, current_timestamp,
                current_timestamp)
        """,
        [
            request.request_id,
            request.connector,
            request.source_version,
            request.analysis_region_id,
            request.window_start.replace(tzinfo=None),
            request.window_end.replace(tzinfo=None),
            json.dumps(request.request),
            request.request_contract_version,
        ],
    )


def test_snapshot_write_is_atomic_idempotent_and_validated(
    tmp_path,
    conn,
    monkeypatch,
):
    request = _request()
    _insert_request(conn, request)
    collected = datetime(2024, 2, 2, tzinfo=UTC)
    snapshot = sources.write_source_snapshot(
        b"body",
        request=request,
        source_identity="example.test",
        extension=".json",
        schema_fields=["b", "a"],
        row_count=1,
        source_lineage={"job_id": "job-1", "result_name": "result.nc"},
        collected_at=collected,
        raw_data_dir=tmp_path,
        conn=conn,
    )
    repeated = sources.write_source_snapshot(
        b"body",
        request=request,
        source_identity="example.test",
        extension="json",
        schema_fields=["a", "b"],
        row_count=1,
        source_lineage={"result_name": "result.nc", "job_id": "job-1"},
        collected_at=collected,
        raw_data_dir=tmp_path,
        conn=conn,
    )
    assert snapshot == repeated
    assert json.loads(snapshot.source_lineage_json)["job_id"] == "job-1"
    assert (tmp_path / snapshot.artifact_uri).read_bytes() == b"body"
    with pytest.raises(ValueError, match="credentials"):
        sources.write_source_snapshot(
            b"unsafe-lineage",
            request=request,
            source_identity="example.test/unsafe",
            extension="json",
            schema_fields=[],
            row_count=0,
            source_lineage={"result": "https://example.test/file?token=secret"},
            raw_data_dir=tmp_path,
            conn=conn,
        )
    with pytest.raises(ValueError, match="alphanumeric"):
        sources._snapshot_destination(tmp_path, "x", "y", "z", "../json")
    with pytest.raises(ValueError, match="escapes"):
        sources._snapshot_destination(tmp_path, "/escape", "y", "z", "json")

    destination = tmp_path / snapshot.artifact_uri
    destination.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        sources.write_source_snapshot(
            b"body",
            request=request,
            source_identity="example.test",
            extension="json",
            schema_fields=[],
            row_count=0,
            raw_data_dir=tmp_path,
            conn=conn,
        )
    destination.unlink()
    real_hash = sources.sha256_file
    monkeypatch.setattr(sources, "sha256_file", lambda _path: "wrong")
    with pytest.raises(ValueError, match="changed while writing"):
        sources.write_source_snapshot(
            b"new",
            request=request,
            source_identity="example.test/new",
            extension="json",
            schema_fields=[],
            row_count=0,
            raw_data_dir=tmp_path,
            conn=conn,
        )
    monkeypatch.setattr(sources, "sha256_file", real_hash)


def test_normalization_preserves_missing_negative_and_local_standard_time():
    snapshot = sources.SourceSnapshot(
        "snapshot",
        "request",
        "harmony",
        "source",
        None,
        "path",
        "a" * 64,
        None,
        "b" * 64,
        1,
        datetime(2024, 1, 2, tzinfo=UTC),
    )
    gps = (
        datetime(2024, 1, 1, tzinfo=UTC) - datetime(1980, 1, 6, tzinfo=UTC)
    ).total_seconds() + 18
    pixel = sources.normalize_tempo_pixel(
        {
            "time_gps_seconds": gps,
            "mirror_step": 1,
            "xtrack": 2,
            "no2_vertical_column": -1,
            "no2_uncertainty": "nan",
            "quality_flag": "",
            "cloud_fraction": "",
        },
        analysis_region_id="region",
        granule_id="granule",
        upstream_revision="r1",
        snapshot=snapshot,
    )
    assert pixel["no2_vertical_column"] == -1
    assert pixel["no2_uncertainty"] is None
    assert pixel["quality_flag"] is None
    assert pixel["collection_version"] == "V04"
    assert pixel["no2_unit"] == "molecules/cm2"

    emission = sources.normalize_camd_hour(
        {
            "facility_id": "1",
            "unit_id": "A",
            "operating_date": "2024-07-15",
            "operating_hour": 12,
            "nox_mass_lbs": "",
            "operating_time_hours": "nan",
            "source_quality": "",
        },
        timezone_name="America/Chicago",
        utc_standard_offset_minutes=-360,
        snapshot=snapshot,
    )
    assert emission["nox_mass_lbs"] is None
    assert emission["operating_time_hours"] is None
    assert emission["source_quality"] is None
    assert emission["observation_start_utc"] == datetime(2024, 7, 15, 18, tzinfo=UTC)
    assert sources._nullable_float(None) is None
    assert sources._nullable_int("") is None


def test_persist_records_is_idempotent_retryable_and_pending_rows_are_decoded(
    conn,
):
    request = _request()
    _insert_request(conn, request)
    row = {
        "meteorology_revision_id": "met",
        "analysis_region_id": "region",
        "valid_time": datetime(2024, 1, 1, tzinfo=UTC),
        "latitude": 1.0,
        "longitude": 2.0,
        "source_snapshot_id": "snapshot",
        "collected_at": datetime(2024, 1, 2, tzinfo=UTC),
    }
    first = sources.persist_normalized_records(meteorology=[row], conn=conn)
    second = sources.persist_normalized_records(meteorology=[row], conn=conn)
    assert first.meteorology_rows_inserted == 1
    assert second.meteorology_rows_inserted == 0
    decoded = sources.pending_source_requests(conn=conn)[0]
    assert decoded.request_id == request.request_id
    assert decoded.window_start.tzinfo == UTC
    conn.execute(
        """
        update plumegraph_events_ops.source_requests
        set status = 'running', started_at = current_timestamp
        """
    )
    assert sources.pending_source_requests(conn=conn) == []
    conn.execute(
        """
        update plumegraph_events_ops.source_requests
        set started_at = current_timestamp - interval '2 hours'
        """
    )
    assert len(sources.pending_source_requests(conn=conn)) == 1
    with pytest.raises(Exception):
        sources.persist_normalized_records(
            meteorology=[{"meteorology_revision_id": "broken"}],
            conn=conn,
        )
    assert (
        conn.execute(
            "select count(*) from plumegraph_events_raw.meteorology_observations"
        ).fetchone()[0]
        == 1
    )


def test_normalized_artifact_and_database_result_commit_together(tmp_path, conn):
    request = _request()
    _insert_request(conn, request)
    snapshot = sources.write_source_snapshot(
        b"meteorology",
        request=request,
        source_identity="https://example.test/hrrr",
        extension="json",
        schema_fields=["valid_time"],
        row_count=1,
        collected_at=datetime(2024, 2, 2, tzinfo=UTC),
        raw_data_dir=tmp_path,
        register=False,
        conn=conn,
    )
    row = {
        "meteorology_revision_id": "met",
        "analysis_region_id": "region",
        "valid_time": datetime(2024, 1, 1, tzinfo=UTC),
        "latitude": 1.0,
        "longitude": 2.0,
        "source_snapshot_id": snapshot.snapshot_id,
        "collected_at": snapshot.collected_at,
    }
    metrics = sources.persist_normalized_records(
        meteorology=[row],
        snapshot=snapshot,
        successful_request=request,
        raw_data_dir=tmp_path,
        conn=conn,
    )
    assert metrics.snapshots == 1
    artifact_uri, checksum = conn.execute(
        """
        select artifact_uri, content_sha256
        from plumegraph_events_ops.normalized_artifacts
        """
    ).fetchone()
    artifact = tmp_path / artifact_uri
    assert pq.read_table(artifact).num_rows == 1
    assert sources.sha256_file(artifact) == checksum
    assert (
        conn.execute(
            """
        select status
        from plumegraph_events_ops.source_requests
        where request_id = 'request'
        """
        ).fetchone()[0]
        == "success"
    )
    second_row = {
        **row,
        "meteorology_revision_id": "met-2",
        "valid_time": datetime(2024, 1, 2, tzinfo=UTC),
    }
    sources.persist_normalized_records(
        meteorology=[row, second_row],
        snapshot=snapshot,
        successful_request=request,
        raw_data_dir=tmp_path,
        conn=conn,
    )
    assert (
        conn.execute(
            """
        select count(*)
        from plumegraph_events_ops.normalized_artifacts
        where source_snapshot_id = ?
        """,
            [snapshot.snapshot_id],
        ).fetchone()[0]
        == 2
    )
    with pytest.raises(ValueError, match="no partition timestamp"):
        sources.persist_normalized_records(
            meteorology=[{"meteorology_revision_id": "undated"}],
            snapshot=snapshot,
            raw_data_dir=tmp_path,
            conn=conn,
        )
    repeated_artifact = sources.write_normalized_artifact(
        [row],
        snapshot=snapshot,
        analysis_region_id=request.analysis_region_id,
        partition_date=request.window_start.date(),
        raw_data_dir=tmp_path,
    )
    assert repeated_artifact.content_sha256 == checksum
    artifact.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="normalized artifact checksum"):
        sources.write_normalized_artifact(
            [row],
            snapshot=snapshot,
            analysis_region_id=request.analysis_region_id,
            partition_date=request.window_start.date(),
            raw_data_dir=tmp_path,
        )

    failed_request = sources.SourceRequest(
        "failed-request",
        "hrrr",
        "v1",
        "region",
        request.window_start,
        request.window_end,
        "contract",
        {},
    )
    _insert_request(conn, failed_request)
    failed_snapshot = sources.write_source_snapshot(
        b"failed",
        request=failed_request,
        source_identity="https://example.test/failure",
        extension="json",
        schema_fields=[],
        row_count=1,
        collected_at=datetime(2024, 2, 2, tzinfo=UTC),
        raw_data_dir=tmp_path,
        register=False,
        conn=conn,
    )
    with pytest.raises(Exception):
        sources.persist_normalized_records(
            meteorology=[{"meteorology_revision_id": "broken"}],
            snapshot=failed_snapshot,
            successful_request=failed_request,
            raw_data_dir=tmp_path,
            conn=conn,
        )
    assert (
        conn.execute(
            """
        select count(*)
        from plumegraph_events_ops.source_snapshots
        where snapshot_id = ?
        """,
            [failed_snapshot.snapshot_id],
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            """
        select status
        from plumegraph_events_ops.source_requests
        where request_id = 'failed-request'
        """
        ).fetchone()[0]
        == "planned"
    )

    with pytest.raises(ValueError, match="exactly one record type"):
        sources.persist_normalized_records(
            pixels=[{"pixel_revision_id": "p"}],
            meteorology=[row],
            snapshot=snapshot,
            conn=conn,
        )
    unsafe = sources.SourceSnapshot(**{**snapshot.__dict__, "connector": "../escape"})
    with pytest.raises(ValueError, match="unsafe component"):
        sources.write_normalized_artifact(
            [],
            snapshot=unsafe,
            analysis_region_id="region",
            partition_date=date(2024, 1, 1),
            raw_data_dir=tmp_path,
        )
    symlink_root = tmp_path / "symlink-root"
    outside = tmp_path / "outside"
    symlink_root.mkdir()
    outside.mkdir()
    (symlink_root / "normalized").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escapes"):
        sources.write_normalized_artifact(
            [],
            snapshot=snapshot,
            analysis_region_id="region",
            partition_date=date(2024, 1, 1),
            raw_data_dir=symlink_root,
        )


def test_secret_sanitization_handles_values_and_query_markers():
    message = "secret token=abc&next=1 API_KEY=value apikey=other signature=signed"
    sanitized = sources.sanitize_source_error(message, secrets=("secret", ""))
    assert "secret" not in sanitized
    assert "abc" not in sanitized
    assert "value" not in sanitized
    assert "other" not in sanitized
    assert "signed" not in sanitized
    assert "[REDACTED]" in sanitized
    assert len(sources.sanitize_source_error("x" * 3000)) == 2000
