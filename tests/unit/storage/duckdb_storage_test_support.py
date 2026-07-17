"""Shared fixtures for storage/duckdb unit tests."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from titanskies_pipeline.config._reload_settings import reload_all_settings_modules
from titanskies_pipeline.storage.duckdb.connection import (
    get_connection,
    reset_duckdb_connection_state,
)
from titanskies_pipeline.storage.duckdb.schemas.tempo import (
    create_all_tempo_test_tables,
)


def isolate_duckdb_test_env(monkeypatch, db_path: str | Path) -> None:
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)


@pytest.fixture
def duck(monkeypatch, tmp_path):
    isolate_duckdb_test_env(monkeypatch, tmp_path / "unit.duckdb")
    import titanskies_pipeline.storage.duckdb.connection as connection

    reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()
    with get_connection() as conn:
        create_all_tempo_test_tables(conn)
    yield connection
    reset_duckdb_connection_state()
