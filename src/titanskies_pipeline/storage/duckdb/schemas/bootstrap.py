"""Bootstrap every first-party TitanSkies warehouse lane."""

from __future__ import annotations

import duckdb

from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)
from titanskies_pipeline.storage.duckdb.schemas.reproductions import (
    bootstrap_reproduction_tables,
)
from titanskies_pipeline.storage.duckdb.schemas.riverpulse import (
    bootstrap_riverpulse_tables,
)
from titanskies_pipeline.storage.duckdb.schemas.tempo import (
    bootstrap_all_tempo_tables,
)


def bootstrap_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    bootstrap_all_tempo_tables(conn)
    bootstrap_riverpulse_tables(conn)
    bootstrap_plumegraph_tables(conn)
    bootstrap_reproduction_tables(conn)


__all__ = ["bootstrap_all_tables"]
