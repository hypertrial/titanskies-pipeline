#!/usr/bin/env python3
"""Seed disposable DuckDB for dbt source freshness CI."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import bootstrap

bootstrap()

from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD  # noqa: E402
from titanskies_pipeline.storage.duckdb.connection import (  # noqa: E402
    get_persistent_connection,
    init_duck_db,
    reset_duckdb_connection_state,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import (  # noqa: E402
    tempo_raw_tbl,
)


def main() -> None:
    reset_duckdb_connection_state()
    init_duck_db()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = get_persistent_connection()
    for scope in (SCOPE_NO2, SCOPE_NO2_STD):
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {tempo_raw_tbl("region_hour_aggregates", scope=scope)}
            (observation_hour, canonical_region_id, country_code, region_type,
             no2_mean, no2_median, no2_p90, valid_pixel_count, total_pixel_count,
             valid_area_km2, total_area_km2, coverage_fraction, quality_flag_accepted,
             source_granule_count, all_granules_validated, revision, geometry_version,
             ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                now,
                "US-CA-037",
                "US",
                "county",
                2.5e15,
                2.4e15,
                3.0e15,
                10,
                12,
                8.0,
                10.0,
                0.8,
                True,
                1,
                True,
                1,
                "test-v1",
                now,
            ],
        )
    conn.close()


if __name__ == "__main__":
    main()
