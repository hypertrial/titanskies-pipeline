"""Shared fixtures for unit tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import requests

_NETWORK_DISABLED_MSG = (
    "Real outbound HTTP is disabled in unit tests. "
    "Inject a mock client or patch the transport at the module under test."
)


@pytest.fixture(autouse=True)
def block_real_http(monkeypatch):
    def _blocked_request(self, method, url, *args, **kwargs):
        del self, method, args, kwargs
        raise RuntimeError(f"{_NETWORK_DISABLED_MSG} ({url})")

    monkeypatch.setattr(requests.sessions.Session, "request", _blocked_request)


@pytest.fixture
def isolated_env(monkeypatch, tmp_path):
    real_exists = Path.exists

    def _no_repo_dotenv(self: Path) -> bool:
        if self.name == ".env":
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", _no_repo_dotenv)
    for key in (
        "DUCKDB_NAME",
        "DUCKDB_PATH",
        "DBT_PROFILES_DIR",
        "TEMPO_NO2_CMR_CONCEPT_ID",
        "TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS",
        "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED",
        "TEMPO_GEOGRAPHY_MANIFEST_PATH",
    ):
        monkeypatch.delenv(key, raising=False)
    db = tmp_path / "test.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db))
    return db


@pytest.fixture
def reload_settings(isolated_env):
    from titanskies_pipeline.config._reload_settings import reload_all_settings_modules

    yield reload_all_settings_modules()


@pytest.fixture
def reset_connection_globals():
    import titanskies_pipeline.storage.duckdb.connection as connection

    connection.reset_duckdb_connection_state()
    yield
    connection.reset_duckdb_connection_state()


@pytest.fixture
def no_sleep():
    with patch("time.sleep", lambda *_a, **_k: None):
        yield


from tests.unit.storage.duckdb_storage_test_support import duck  # noqa: E402, F401
