from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

import titanskies_pipeline.riverpulse.collection as collection
from titanskies_pipeline.riverpulse.collection import (
    RiverPulseBatchError,
    pending_requests,
    persist_fetch_result,
    plan_source_requests,
    sync_pending_requests,
)
from titanskies_pipeline.riverpulse.hydrocron import (
    FetchResult,
    HydrocronFetchError,
    HydrocronRequest,
)
from titanskies_pipeline.riverpulse.network import (
    persist_network_artifacts,
    publish_network_generation,
    synthetic_network_rows,
)

TESTS_ROOT = Path(__file__).resolve().parents[2]
CASSETTE = TESTS_ROOT / "fixtures" / "cassettes" / "riverpulse_hydrocron.csv"
UTC = timezone.utc


def _register_network(conn, tmp_path):
    reaches, edges, anchors = synthetic_network_rows()
    artifacts = publish_network_generation(
        output_dir=tmp_path / "network",
        reaches=reaches,
        edges=edges,
        artifact_mode="synthetic",
        source_manifest_sha256=hashlib.sha256(b"synthetic").hexdigest(),
        resolved_anchors=anchors,
    )
    persist_network_artifacts(artifacts, conn=conn)
    return artifacts


def _plan_one(conn):
    plan_source_requests(
        window_start=datetime(2024, 1, 1, tzinfo=UTC),
        window_end=datetime(2025, 1, 1, tzinfo=UTC),
        reach_ids=["RP1001"],
        conn=conn,
    )
    return pending_requests(conn=conn)[0]


def test_discovery_requires_network_and_plans_half_open_year_windows(duck, tmp_path):
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="network registry is empty"):
            plan_source_requests(conn=conn)
        _register_network(conn, tmp_path)
        metrics = plan_source_requests(
            window_start=datetime(2023, 8, 1, tzinfo=UTC),
            window_end=datetime(2025, 2, 1, tzinfo=UTC),
            reach_ids=["RP1001"],
            conn=conn,
        )
        assert metrics.reaches == 1
        assert metrics.windows == 3
        assert metrics.requests_planned == 3
        duplicate = plan_source_requests(
            window_start=datetime(2023, 8, 1, tzinfo=UTC),
            window_end=datetime(2025, 2, 1, tzinfo=UTC),
            reach_ids=["RP1001"],
            conn=conn,
        )
        assert duplicate.requests_planned == 0
        assert duplicate.requests_existing == 3
        assert len(pending_requests(max_requests=2, conn=conn)) == 2
        with pytest.raises(ValueError, match="not registered"):
            plan_source_requests(
                window_start=datetime(2024, 1, 1, tzinfo=UTC),
                window_end=datetime(2025, 1, 1, tzinfo=UTC),
                reach_ids=["UNKNOWN"],
                conn=conn,
            )
        with pytest.raises(ValueError, match="at least one reach"):
            plan_source_requests(
                window_start=datetime(2024, 1, 1, tzinfo=UTC),
                window_end=datetime(2025, 1, 1, tzinfo=UTC),
                reach_ids=[],
                conn=conn,
            )
        deduplicated = plan_source_requests(
            window_start=datetime(2025, 1, 1, tzinfo=UTC),
            window_end=datetime(2026, 1, 1, tzinfo=UTC),
            reach_ids=["RP1001", "RP1001"],
            conn=conn,
        )
        assert deduplicated.reaches == 1
        assert deduplicated.requests_planned == 1
        stale_id = pending_requests(conn=conn)[0].request_id
        conn.execute(
            """
            update riverpulse_events_ops.source_requests
            set status = 'running',
                started_at = current_timestamp - interval '2 hours'
            where request_id = ?
            """,
            [stale_id],
        )
        assert stale_id in {
            request.request_id for request in pending_requests(conn=conn)
        }
        conn.execute(
            """
            update riverpulse_events_ops.source_requests
            set started_at = current_timestamp
            where request_id = ?
            """,
            [stale_id],
        )
        assert stale_id not in {
            request.request_id for request in pending_requests(conn=conn)
        }


def test_identical_overlap_and_correction_revision_semantics(duck, tmp_path):
    raw_dir = tmp_path / "raw"
    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        request = _plan_one(conn)
        result = FetchResult(200, CASSETTE.read_bytes(), 1, False)
        assert persist_fetch_result(
            request, result, raw_data_dir=raw_dir, conn=conn
        ) == (3, 3, 42, 3)
        assert persist_fetch_result(
            request, result, raw_data_dir=raw_dir, conn=conn
        ) == (3, 0, 0, 0)
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_raw.observation_revisions"
            ).fetchone()[0]
            == 3
        )

        plan_source_requests(
            window_start=datetime(2024, 5, 1, tzinfo=UTC),
            window_end=datetime(2024, 12, 1, tzinfo=UTC),
            reach_ids=["RP1001"],
            conn=conn,
        )
        overlap = pending_requests(conn=conn)[0]
        assert overlap.request_id != request.request_id
        assert persist_fetch_result(
            overlap, result, raw_data_dir=raw_dir, conn=conn
        ) == (3, 0, 0, 3)
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_raw.observation_snapshot_links"
            ).fetchone()[0]
            == 6
        )

        corrected = FetchResult(
            200,
            CASSETTE.read_bytes().replace(b"8.47", b"8.48", 1),
            1,
            False,
        )
        assert persist_fetch_result(
            overlap, corrected, raw_data_dir=raw_dir, conn=conn
        ) == (3, 1, 14, 3)
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_raw.observation_revisions"
            ).fetchone()[0]
            == 4
        )
        assert (
            conn.execute(
                "select count(distinct response_sha256) "
                "from riverpulse_events_raw.observation_revisions"
            ).fetchone()[0]
            == 2
        )


def test_no_data_is_successful_zero_row_snapshot(duck, tmp_path):
    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        request = _plan_one(conn)
        result = FetchResult(400, b'{"message":"No data found"}', 1, True)
        assert persist_fetch_result(
            request, result, raw_data_dir=tmp_path / "raw", conn=conn
        ) == (0, 0, 0, 0)
        row = conn.execute(
            """
            select status, http_status, row_count
            from riverpulse_events_ops.source_requests
            """
        ).fetchone()
        assert row == ("success", 400, 0)
        snapshot = conn.execute(
            """
            select artifact_uri, row_count
            from riverpulse_events_ops.source_snapshots
            """
        ).fetchone()
        assert snapshot[0].endswith(".txt")
        assert snapshot[1] == 0


def test_partial_batch_commits_success_and_leaves_failure_retryable(
    duck, tmp_path, monkeypatch
):
    monkeypatch.setenv("RIVERPULSE_REQUEST_INTERVAL_SECONDS", "0.2")
    sleeps = []
    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        plan_source_requests(
            window_start=datetime(2024, 1, 1, tzinfo=UTC),
            window_end=datetime(2025, 1, 1, tzinfo=UTC),
            reach_ids=["RP1001", "RP2001"],
            conn=conn,
        )

        def fetcher(request):
            if request.reach_id == "RP2001":
                raise HydrocronFetchError(
                    "actionable failure", attempts=4, status_code=503
                )
            return FetchResult(200, CASSETTE.read_bytes(), 1, False)

        with pytest.raises(RiverPulseBatchError) as raised:
            sync_pending_requests(
                conn=conn,
                fetcher=fetcher,
                sleep=sleeps.append,
                raw_data_dir=tmp_path / "raw",
            )
        assert raised.value.metrics.requests_succeeded == 1
        assert raised.value.metrics.requests_failed == 1
        assert sleeps == [1.0]
        statuses = dict(
            conn.execute(
                "select reach_id, status from riverpulse_events_ops.source_requests"
            ).fetchall()
        )
        assert statuses == {"RP1001": "success", "RP2001": "failed"}
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_raw.observation_revisions"
            ).fetchone()[0]
            == 3
        )

        replacement = CASSETTE.read_bytes().replace(b"RP1001", b"RP2001")
        retry = sync_pending_requests(
            conn=conn,
            fetcher=lambda _request: FetchResult(200, replacement, 1, False),
            sleep=lambda _: None,
            raw_data_dir=tmp_path / "raw",
        )
        assert retry.requests_succeeded == 1
        assert retry.requests_failed == 0


def test_malformed_response_and_secrets_are_recorded_safely(
    duck, tmp_path, monkeypatch
):
    monkeypatch.setenv("RIVERPULSE_HYDROCRON_API_KEY", "api-secret")
    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        _plan_one(conn)

        def fail(_request):
            raise HydrocronFetchError(
                "api-secret https://example.test/path?token=api-secret",
                attempts=1,
                status_code=400,
            )

        with pytest.raises(RiverPulseBatchError):
            sync_pending_requests(
                conn=conn,
                fetcher=fail,
                sleep=lambda _: None,
                raw_data_dir=tmp_path / "raw",
            )
        error = conn.execute(
            "select error_message from riverpulse_events_ops.source_requests"
        ).fetchone()[0]
        assert "api-secret" not in error
        assert "token=" not in error


def test_snapshot_corruption_and_reach_mismatch_are_rejected(duck, tmp_path):
    raw_dir = tmp_path / "raw"
    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        request = _plan_one(conn)
        result = FetchResult(200, CASSETTE.read_bytes(), 1, False)
        persist_fetch_result(request, result, raw_data_dir=raw_dir, conn=conn)
        relative = conn.execute(
            "select artifact_uri from riverpulse_events_ops.source_snapshots"
        ).fetchone()[0]
        (raw_dir / relative).write_bytes(b"corrupt")
        with pytest.raises(ValueError, match="checksum mismatch"):
            persist_fetch_result(request, result, raw_data_dir=raw_dir, conn=conn)

        wrong_request = HydrocronRequest.create(
            reach_id="RP2001",
            window_start=request.window_start,
            window_end=request.window_end,
            field_contract_version=request.field_contract_version,
        )
        with pytest.raises(ValueError, match="different reach"):
            persist_fetch_result(
                wrong_request,
                result,
                raw_data_dir=raw_dir,
                conn=conn,
            )


def test_snapshot_path_and_atomic_write_defenses(monkeypatch, tmp_path):
    request = HydrocronRequest.create(
        reach_id="../../escape",
        window_start=datetime(2024, 1, 1, tzinfo=UTC),
        window_end=datetime(2025, 1, 1, tzinfo=UTC),
        field_contract_version="riverpulse-v1",
    )
    with pytest.raises(ValueError, match="escapes"):
        collection._snapshot_path(
            tmp_path / "raw",
            request,
            "a" * 64,
            no_data=False,
        )

    destination = tmp_path / "raw" / "snapshot.csv"
    monkeypatch.setattr(collection, "sha256_file", lambda _path: "wrong")
    with pytest.raises(ValueError, match="changed while writing"):
        collection._atomic_snapshot(b"body", destination)
    assert not destination.exists()


def test_snapshot_transaction_rolls_back_on_database_failure(duck, tmp_path):
    class FailingConnection:
        def __init__(self, inner):
            self.inner = inner
            self.rolled_back = False

        def execute(self, sql, parameters=None):
            if "source_snapshots" in sql and "INSERT OR IGNORE" in sql:
                raise RuntimeError("injected snapshot failure")
            if sql.strip() == "ROLLBACK":
                self.rolled_back = True
            return (
                self.inner.execute(sql)
                if parameters is None
                else self.inner.execute(sql, parameters)
            )

    with duck.get_connection() as conn:
        _register_network(conn, tmp_path)
        request = _plan_one(conn)
        failing = FailingConnection(conn)
        with pytest.raises(RuntimeError, match="injected snapshot failure"):
            persist_fetch_result(
                request,
                FetchResult(200, CASSETTE.read_bytes(), 1, False),
                raw_data_dir=tmp_path / "raw",
                conn=failing,
            )
        assert failing.rolled_back
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_ops.source_snapshots"
            ).fetchone()[0]
            == 0
        )
