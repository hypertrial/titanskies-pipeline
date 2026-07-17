"""Production-cardinality dbt incremental benchmark."""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile
from tests.integration.duckdb.test_golden_marts import _run_dbt

import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.storage.duckdb.schemas.tempo import (
    create_all_tempo_test_tables,
)


def _incremental_model_execution_seconds() -> float:
    """Read dbt's measured incremental-model time, excluding fixed DDL overhead."""
    run_results = json.loads(
        (Path(__file__).parents[3] / "dbt" / "target" / "run_results.json").read_text()
    )
    incremental_models = {
        "model.titanskies.int_tempo_no2_region_anomalies",
        "model.titanskies.int_tempo_no2_region_hourly",
    }
    return sum(
        result["execution_time"]
        for result in run_results["results"]
        if result["unique_id"] in incremental_models
    )


@pytest.mark.performance
def test_one_hour_incremental_is_exact_and_at_most_thirty_percent_of_full_refresh(
    tmp_path: Path, dbt_profiles_dir: Path
) -> None:
    region_count = 10_000
    db_path = tmp_path / "production-cardinality.duckdb"
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    os.environ["DUCKDB_PATH"] = str(db_path)
    os.environ["DUCKDB_NAME"] = str(db_path)
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    create_all_tempo_test_tables(conn)
    conn.execute(
        f"""
        insert into tempo_no2_ops.region_registry
        select
            'US', 'county', lpad(cast(i as varchar), 5, '0'),
            'US-PC-' || lpad(cast(i as varchar), 5, '0'),
            'Performance County ' || i, 'US-PC', 'UTC', 'performance-v1',
            repeat('a', 64), current_timestamp
        from range({region_count}) as regions(i)
        """
    )
    for day in range(29):
        revision = conn.execute("select nextval('tempo_no2_hour_revision')").fetchone()[
            0
        ]
        conn.execute(
            f"""
            insert into tempo_no2_raw.region_hour_aggregates
            select
                timestamp '2026-06-01 12:00:00' + interval '{day} days',
                canonical_region_id, 'US', 'county', 1e15 + {day} * 1e12,
                1e15 + {day} * 1e12, 1e15 + {day} * 1e12,
                1, 1, 1.0, 1.0, 1.0, true, 1, true, {revision},
                'performance-v1', current_timestamp
            from tempo_no2_ops.region_registry
            """
        )
    conn.close()

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    _run_dbt(["seed"], profiles_dir=dbt_profiles_dir, env=env)
    _run_dbt(["build", "--full-refresh"], profiles_dir=dbt_profiles_dir, env=env)

    conn = duckdb.connect(str(db_path))
    revision = conn.execute("select nextval('tempo_no2_hour_revision')").fetchone()[0]
    conn.execute(
        f"""
        insert into tempo_no2_raw.region_hour_aggregates
        select
            timestamp '2026-06-30 12:00:00', canonical_region_id, 'US',
            'county', 1.03e15, 1.03e15, 1.03e15, 1, 1, 1.0, 1.0, 1.0,
            true, 1, true, {revision}, 'performance-v1', current_timestamp
        from tempo_no2_ops.region_registry
        """
    )
    conn.close()

    _run_dbt(["build"], profiles_dir=dbt_profiles_dir, env=env)
    incremental_seconds = _incremental_model_execution_seconds()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        create table performance_incremental_snapshot as
        select * from tempo_no2_intermediate.int_tempo_no2_region_anomalies
        """
    )
    conn.close()

    _run_dbt(["build", "--full-refresh"], profiles_dir=dbt_profiles_dir, env=env)
    full_refresh_seconds = _incremental_model_execution_seconds()
    conn = duckdb.connect(str(db_path))
    difference = conn.execute(
        """
        select count(*) from (
            (select * from performance_incremental_snapshot
             except select * from tempo_no2_intermediate.int_tempo_no2_region_anomalies)
            union all
            (select * from tempo_no2_intermediate.int_tempo_no2_region_anomalies
             except select * from performance_incremental_snapshot)
        )
        """
    ).fetchone()[0]
    conn.close()

    assert difference == 0
    assert incremental_seconds <= full_refresh_seconds * 0.30, (
        f"incremental={incremental_seconds:.3f}s, full={full_refresh_seconds:.3f}s"
    )
