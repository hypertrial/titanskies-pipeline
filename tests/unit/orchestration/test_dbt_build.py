from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

from tests.unit.orchestration.orchestration_test_support import (
    _DormantThread,
    _FakeClock,
    _FakeQueue,
    _ImmediateThread,
    _patch_guardrail_clock,
)

from titanskies_pipeline.orchestration import config as orch_config
from titanskies_pipeline.orchestration import dbt_build as dbt_build_mod
from titanskies_pipeline.resources.progress_guardrails import NoProgressTimeoutError


@pytest.fixture(autouse=True)
def _isolate_warehouse(monkeypatch, tmp_path):
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        dbt_build_mod, "active_duckdb_path", lambda: tmp_path / "warehouse.duckdb"
    )


def test_stream_dbt_build_appends_full_refresh_flag():
    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(full_refresh=True),
        )
    )
    assert captured_args == [["build", "--full-refresh"]]
    assert events == ["event"]


def test_stream_dbt_build_appends_dbt_select_and_exclude_flags():
    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                full_refresh=True,
                dbt_select="+tag:tempo",
                dbt_exclude="tag:other",
            ),
        )
    )
    assert captured_args == [
        [
            "build",
            "--full-refresh",
            "--select",
            "+tag:tempo",
            "--exclude",
            "tag:other",
        ]
    ]


def test_stream_dbt_build_fetches_row_counts_and_column_metadata():
    calls: list[object] = []

    class FakeDbtEventStream:
        def fetch_row_counts(self):
            calls.append("row_counts")
            return self

        def fetch_column_metadata(self, *, with_column_lineage=True):
            calls.append(("column_metadata", with_column_lineage))
            return self

        def __iter__(self):
            yield "event"

    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.adapter.cleanup_connections = lambda: calls.append("cleanup")
            m.stream = lambda: FakeDbtEventStream()
            m.process = MagicMock(returncode=0)
            return m

    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(fetch_dbt_metadata=True),
        )
    )
    assert events == ["event"]
    assert calls == ["row_counts", ("column_metadata", False), "cleanup"]


def test_stream_dbt_build_handles_missing_opt_in_dbt_metadata_hooks():
    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(fetch_dbt_metadata=True),
        )
    )
    assert events == ["event"]


def test_cleanup_dbt_adapter_handles_adapter_shapes():
    calls: list[str] = []

    dbt_build_mod._cleanup_dbt_adapter(MagicMock(adapter=None))

    adapter = MagicMock()
    adapter.cleanup_connections.side_effect = lambda: calls.append("connections")
    adapter.connections.cleanup_all.side_effect = lambda: calls.append("all")
    adapter.connections.close_all_connections.side_effect = lambda: calls.append(
        "close_all"
    )
    dbt_build_mod._cleanup_dbt_adapter(MagicMock(adapter=adapter))
    assert calls == ["connections", "all", "close_all"]


def test_stream_dbt_build_syncs_duckdb_path_env(monkeypatch, tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setattr(dbt_build_mod, "active_duckdb_path", lambda: db_path)
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)

    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.stream = lambda: iter([])
            m.process = MagicMock(returncode=0)
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(),
        )
    )
    assert os.environ["DUCKDB_PATH"] == str(db_path)


def test_stream_dbt_build_merges_heartbeat_diagnostics(monkeypatch):
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )
    heartbeat_calls: list[bool] = []

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=0))
            m.stream = lambda: iter([])
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
            heartbeat_diagnostics_fn=lambda: (
                heartbeat_calls.append(True) or {"heartbeat": "ok"}
            ),
        )
    )
    assert heartbeat_calls == [True]


def test_stream_dbt_build_ignores_non_dict_heartbeat(monkeypatch):
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=0))
            m.stream = lambda: iter([])
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="titanskies_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
            heartbeat_diagnostics_fn=lambda: None,
        )
    )


def test_stream_dbt_build_raises_producer_error(monkeypatch):
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(dbt_build_mod, "active_duckdb_path", lambda: "/tmp/test.duckdb")

    class BrokenInvocation:
        process = MagicMock(returncode=0)

        def stream(self):
            raise RuntimeError("stream broke")
            yield  # pragma: no cover

    dbt = MagicMock()
    dbt.cli.return_value = BrokenInvocation()

    with pytest.raises(RuntimeError, match="stream broke"):
        list(
            dbt_build_mod.stream_dbt_build(
                asset_name="titanskies_dbt",
                context=MagicMock(),
                dbt=dbt,
                config=orch_config.DbtBuildConfig(progress_poll_seconds=1),
            )
        )


def test_stream_dbt_build_raises_on_nonzero_returncode(monkeypatch):
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(dbt_build_mod, "active_duckdb_path", lambda: "/tmp/test.duckdb")
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=2))
            m.stream = lambda: iter(["event"])
            return m

    with pytest.raises(RuntimeError, match="exit code 2"):
        list(
            dbt_build_mod.stream_dbt_build(
                asset_name="titanskies_dbt",
                context=MagicMock(),
                dbt=MockDbt(),
                config=orch_config.DbtBuildConfig(progress_poll_seconds=1),
            )
        )


def test_stream_dbt_build_hard_timeout_terminates_process(monkeypatch):
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _DormantThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )
    process_mock = MagicMock(returncode=None)

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=process_mock)
            m.stream = lambda: iter(())
            return m

    with pytest.raises(NoProgressTimeoutError):
        list(
            dbt_build_mod.stream_dbt_build(
                asset_name="titanskies_dbt",
                context=MagicMock(),
                dbt=MockDbt(),
                config=orch_config.DbtBuildConfig(
                    no_progress_soft_timeout_seconds=None,
                    no_progress_hard_timeout_seconds=1,
                    progress_log_interval_seconds=1,
                    progress_poll_seconds=1,
                ),
            )
        )
    assert process_mock.terminate.called
