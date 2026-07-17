"""End-to-end acceptance test for the credential-free demo and exports."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_demo_populates_admin_grid_and_exports(tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/build_demo.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "tempo_no2_marts.tempo_no2_region_hourly" in result.stdout
    assert "tempo_no2_marts.tempo_no2_grid_latest" in result.stdout

    warehouse = REPO_ROOT / ".cache" / "demo.duckdb"
    csv_path = tmp_path / "admin.csv"
    parquet_path = tmp_path / "grid.parquet"
    conn = duckdb.connect(str(warehouse), read_only=False)
    admin_count, grid_count, issue_count = conn.execute(
        """
        select
            (select count(*) from tempo_no2_marts.tempo_no2_region_hourly),
            (select count(*) from tempo_no2_marts.tempo_no2_grid_latest),
            (select count(*) from tempo_no2_observability.tempo_no2_data_quality
             where issue_type in ('low_coverage', 'zero_valid'))
        """
    ).fetchone()
    conn.execute(
        f"""
        copy (
            select * from tempo_no2_marts.tempo_no2_region_hourly
            where is_analysis_ready
        ) to '{csv_path}' (header, delimiter ',')
        """
    )
    conn.execute(
        f"""
        copy (
            select * from tempo_no2_marts.tempo_no2_grid_latest
        ) to '{parquet_path}' (format parquet, compression zstd)
        """
    )
    conn.close()

    assert admin_count > 0
    assert grid_count > 0
    assert issue_count > 0
    assert csv_path.stat().st_size > 0
    assert parquet_path.stat().st_size > 0
    assert (
        duckdb.connect()
        .execute(f"select count(*) from read_parquet('{parquet_path}')")
        .fetchone()[0]
        == grid_count
    )
