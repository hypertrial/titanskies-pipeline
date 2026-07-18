#!/usr/bin/env python3
"""Build and summarize the credential-free TitanSkies demo warehouse."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

from titanskies_pipeline.storage.duckdb import connection
from titanskies_pipeline.storage.duckdb.demo_seed import seed_demo_warehouse

ROOT = Path(__file__).resolve().parents[1]
DEMO_PATH = ROOT / ".cache" / "demo.duckdb"


def main() -> None:
    DEMO_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEMO_PATH.unlink(missing_ok=True)
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(DEMO_PATH)
    env["DUCKDB_NAME"] = str(DEMO_PATH)
    os.environ.update({"DUCKDB_PATH": str(DEMO_PATH), "DUCKDB_NAME": str(DEMO_PATH)})
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    seed_demo_warehouse(conn)
    conn.close()

    common = [
        "--project-dir",
        str(ROOT / "dbt"),
        "--profiles-dir",
        str(ROOT / "dbt" / "profiles"),
    ]
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "seed", *common], check=True, env=env
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "run",
            "--select",
            "tag:tempo,tag:no2",
            *common,
        ],
        check=True,
        env=env,
    )

    conn = duckdb.connect(str(DEMO_PATH), read_only=True)
    relations = (
        "tempo_no2_raw.region_hour_aggregates",
        "tempo_no2_raw.grid_latest",
        "tempo_no2_marts.tempo_no2_region_hourly",
        "tempo_no2_marts.tempo_no2_country_hourly",
        "tempo_no2_marts.tempo_no2_grid_latest",
    )
    counts = [
        (relation, conn.execute(f"select count(*) from {relation}").fetchone()[0])
        for relation in relations
    ]
    admin = conn.execute(
        """
        select canonical_region_id, observation_hour, no2_mean, coverage_fraction
        from tempo_no2_marts.tempo_no2_region_hourly
        order by observation_hour desc, canonical_region_id
        limit 3
        """
    ).fetchall()
    grid = conn.execute(
        """
        select grid_row, grid_col, latitude, longitude, no2, is_analysis_ready
        from tempo_no2_marts.tempo_no2_grid_latest
        order by grid_row, grid_col
        limit 3
        """
    ).fetchall()
    csv_path = DEMO_PATH.with_name("demo-region-hourly.csv")
    parquet_path = DEMO_PATH.with_name("demo-grid-latest.parquet")
    conn.execute(
        f"""
        COPY (
            SELECT * FROM tempo_no2_marts.tempo_no2_region_hourly
            WHERE is_analysis_ready
        ) TO '{csv_path}' (HEADER, DELIMITER ',')
        """
    )
    conn.execute(
        f"""
        COPY tempo_no2_marts.tempo_no2_grid_latest
        TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    if csv_path.stat().st_size == 0 or parquet_path.stat().st_size == 0:
        raise RuntimeError("Demo export validation failed")
    conn.close()
    print(f"warehouse={DEMO_PATH}")
    for relation, count in counts:
        print(f"relation={relation} rows={count}")
    print(f"admin_query={admin}")
    print(f"grid_query={grid}")
    print(f"csv_export={csv_path}")
    print(f"parquet_export={parquet_path}")


if __name__ == "__main__":
    main()
