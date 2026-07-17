#!/usr/bin/env python3
"""Rewrite DuckDB file to reclaim dead space."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import bootstrap

bootstrap()

from titanskies_pipeline.config.settings import DUCKDB_PATH  # noqa: E402
from titanskies_pipeline.storage.duckdb.connection import (  # noqa: E402
    active_duckdb_path,
    reset_duckdb_connection_state,
)

import duckdb  # noqa: E402


def main() -> None:
    path = active_duckdb_path()
    temp_path = path.with_suffix(path.suffix + ".compact")
    reset_duckdb_connection_state()
    duckdb.connect(str(temp_path)).execute(
        f"ATTACH '{path}' AS source; COPY FROM DATABASE source TO '{temp_path}'; DETACH source;"
    )
    temp_path.replace(path)
    print(f"Compacted {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
