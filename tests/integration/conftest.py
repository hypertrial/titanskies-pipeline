"""Shared fixtures for integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.unit.storage.duckdb_storage_test_support import duck  # noqa: F401


@pytest.fixture
def dbt_profiles_dir(tmp_path: Path) -> Path:
    profiles_dir = tmp_path / ".dbt"
    profiles_dir.mkdir()
    return profiles_dir


def write_dbt_profile(profiles_dir: Path, db_path: Path, *, threads: int = 2) -> None:
    (profiles_dir / "profiles.yml").write_text(
        f"""
titanskies:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: {threads}
  target: dev
""".strip()
        + "\n"
    )
