"""Golden-row regression coverage for shipped public marts."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile

import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.storage.duckdb.demo_seed import seed_demo_warehouse

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_ROOT = REPO_ROOT / "dbt"


def _assert_exact(actual, expected) -> None:
    assert actual == expected


def _run_dbt(
    args: list[str],
    *,
    profiles_dir: Path,
    env: dict[str, str],
    project_dir: Path = DBT_ROOT,
) -> None:
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_golden_region_hourly_and_data_quality(tmp_path, dbt_profiles_dir):
    db_path = tmp_path / "golden.duckdb"
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    os.environ["DUCKDB_PATH"] = str(db_path)
    os.environ["DUCKDB_NAME"] = str(db_path)
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    seed_demo_warehouse(
        conn,
        include_quality_issues=False,
        anchor_time=datetime.now().replace(minute=0, second=0, microsecond=0),
    )
    conn.close()

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    _run_dbt(["seed"], profiles_dir=dbt_profiles_dir, env=env)
    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)

    conn = duckdb.connect(str(db_path))
    hourly = conn.execute(
        """
        SELECT
            canonical_region_id,
            country_code,
            region_type,
            no2_mean,
            no2_median,
            no2_p90,
            valid_pixel_count,
            total_pixel_count,
            coverage_fraction,
            quality_flag_accepted,
            all_granules_validated,
            source_granule_count,
            is_analysis_ready
        FROM tempo_no2_marts.tempo_no2_region_hourly
        WHERE canonical_region_id = 'US-CA-037'
        ORDER BY observation_hour DESC
        LIMIT 1
        """
    ).fetchall()
    _assert_exact(
        hourly,
        [
            (
                "US-CA-037",
                "US",
                "county",
                1.55e15,
                1.488e15,
                1.829e15,
                16,
                20,
                0.82,
                True,
                True,
                2,
                True,
            )
        ],
    )

    latest = conn.execute(
        """
        SELECT canonical_region_id, country_code, region_type,
               latest_no2_mean, latest_coverage_fraction
        FROM tempo_no2_marts.tempo_no2_region_latest
        WHERE canonical_region_id = 'US-CA-037'
        """
    ).fetchall()
    _assert_exact(latest, [("US-CA-037", "US", "county", 1.55e15, 0.82)])

    country = conn.execute(
        """
        SELECT
            country_code,
            no2_mean,
            no2_median,
            no2_p90,
            coverage_fraction,
            valid_pixel_count,
            analysis_ready_region_count,
            region_count
        FROM tempo_no2_marts.tempo_no2_country_hourly
        WHERE country_code = 'US'
        ORDER BY observation_hour DESC
        LIMIT 1
        """
    ).fetchall()
    _assert_exact(
        country,
        [("US", 1.35e15, 1.296e15, 1.593e15, 0.82, 16, 1, 1)],
    )

    anomaly = conn.execute(
        """
        SELECT baseline_sample_count, baseline_median, baseline_mad, robust_z_score
        FROM tempo_no2_marts.tempo_no2_region_anomalies
        WHERE canonical_region_id = 'US-CA-037'
        ORDER BY observation_hour DESC
        LIMIT 1
        """
    ).fetchone()
    assert anomaly[:3] == (7, 1.35e15, 1.0e14)
    assert anomaly[3] == pytest.approx(1.3489815189531904)

    grid = conn.execute(
        """
        SELECT count(*), count(*) filter (where is_analysis_ready)
        FROM tempo_no2_marts.tempo_no2_grid_latest
        """
    ).fetchone()
    assert grid == (3, 2)

    dq_errors = conn.execute(
        """
        SELECT count(*)
        FROM tempo_no2_observability.tempo_no2_data_quality
        WHERE severity = 'error'
        """
    ).fetchone()[0]
    assert dq_errors == 0
    conn.close()


def test_golden_comparison_rejects_changed_no2_value():
    with pytest.raises(AssertionError):
        _assert_exact([(2.5e15, 0.8)], [(2.4e15, 0.8)])


def test_incremental_append_late_replacement_and_contract_invalidation_match_full_refresh(
    tmp_path, dbt_profiles_dir
):
    db_path = tmp_path / "incremental.duckdb"
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    os.environ["DUCKDB_PATH"] = str(db_path)
    os.environ["DUCKDB_NAME"] = str(db_path)
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    seed_demo_warehouse(
        conn,
        include_quality_issues=False,
        anchor_time=datetime(2026, 7, 12, 20),
    )
    conn.close()
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    _run_dbt(["seed"], profiles_dir=dbt_profiles_dir, env=env)
    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)

    conn = duckdb.connect(str(db_path))
    initial = conn.execute(
        """
        select canonical_region_id, observation_hour, no2_mean, revision,
               contract_version
        from tempo_no2_intermediate.int_tempo_no2_region_hourly
        order by all
        """
    ).fetchall()
    conn.close()
    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)
    conn = duckdb.connect(str(db_path))
    assert (
        conn.execute(
            """
            select canonical_region_id, observation_hour, no2_mean, revision,
                   contract_version
            from tempo_no2_intermediate.int_tempo_no2_region_hourly
            order by all
            """
        ).fetchall()
        == initial
    )

    conn.execute(
        """
        insert into tempo_no2_raw.region_hour_aggregates
        select
            observation_hour + interval '1 day', canonical_region_id,
            country_code, region_type, no2_mean + 1e13, no2_median,
            no2_p90, valid_pixel_count, total_pixel_count, valid_area_km2,
            total_area_km2, coverage_fraction, quality_flag_accepted,
            source_granule_count, all_granules_validated,
            nextval('tempo_no2_hour_revision'), geometry_version, current_timestamp
        from tempo_no2_raw.region_hour_aggregates
        where canonical_region_id = 'US-CA-037'
        order by observation_hour desc
        limit 1
        """
    )
    conn.close()
    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)
    conn = duckdb.connect(str(db_path))
    appended_hour = conn.execute(
        """
        select max(observation_hour)
        from tempo_no2_intermediate.int_tempo_no2_region_hourly
        where canonical_region_id = 'US-CA-037'
        """
    ).fetchone()[0]
    assert appended_hour == datetime(2026, 7, 13, 20)

    changed_hour = datetime(2026, 7, 8, 20)
    conn.execute(
        """
        update tempo_no2_raw.region_hour_aggregates
        set no2_mean = no2_mean + 4e14,
            no2_median = no2_median + 4e14,
            no2_p90 = no2_p90 + 4e14,
            revision = nextval('tempo_no2_hour_revision'),
            ingested_at = current_timestamp
        where canonical_region_id = 'US-CA-037' and observation_hour = ?
        """,
        [changed_hour],
    )
    previous_future = conn.execute(
        """
        select source_revision, baseline_median
        from tempo_no2_intermediate.int_tempo_no2_region_anomalies
        where canonical_region_id = 'US-CA-037'
          and observation_hour = timestamp '2026-07-12 20:00:00'
        """
    ).fetchone()
    conn.close()
    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)
    conn = duckdb.connect(str(db_path))
    assert (
        conn.execute(
            """
        select no2_mean
        from tempo_no2_intermediate.int_tempo_no2_region_hourly
        where canonical_region_id = 'US-CA-037' and observation_hour = ?
        """,
            [changed_hour],
        ).fetchone()[0]
        > 1.5e15
    )
    current_future = conn.execute(
        """
            select source_revision, baseline_median
            from tempo_no2_intermediate.int_tempo_no2_region_anomalies
            where canonical_region_id = 'US-CA-037'
              and observation_hour = timestamp '2026-07-12 20:00:00'
            """
    ).fetchone()
    assert current_future[0] == previous_future[0]
    assert current_future[1] != previous_future[1]

    conn.close()
    changed_project = tmp_path / "dbt-contract-change"
    shutil.copytree(DBT_ROOT, changed_project)
    contract_seed = changed_project / "seeds" / "tempo_no2_contract.csv"
    contract_seed.write_text(contract_seed.read_text().replace("0.4.0", "0.4-test"))
    _run_dbt(
        ["build"],
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=changed_project,
    )
    conn = duckdb.connect(str(db_path))
    assert conn.execute(
        """
        select count(distinct contract_version), min(contract_version)
        from tempo_no2_intermediate.int_tempo_no2_region_hourly
        """
    ).fetchone() == (1, "0.4-test")
    conn.execute(
        """
        create table incremental_hourly_snapshot as
        select * from tempo_no2_intermediate.int_tempo_no2_region_hourly
        """
    )
    conn.execute(
        """
        create table incremental_anomaly_snapshot as
        select * from tempo_no2_intermediate.int_tempo_no2_region_anomalies
        """
    )
    conn.close()

    _run_dbt(
        ["build", "--full-refresh"],
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=changed_project,
    )
    conn = duckdb.connect(str(db_path))
    for snapshot, relation in (
        (
            "incremental_hourly_snapshot",
            "tempo_no2_intermediate.int_tempo_no2_region_hourly",
        ),
        (
            "incremental_anomaly_snapshot",
            "tempo_no2_intermediate.int_tempo_no2_region_anomalies",
        ),
    ):
        difference = conn.execute(
            f"""
            select count(*) from (
                (select * from {snapshot} except select * from {relation})
                union all
                (select * from {relation} except select * from {snapshot})
            )
            """
        ).fetchone()[0]
        assert difference == 0
    conn.close()
