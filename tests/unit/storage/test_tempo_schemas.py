import duckdb
import pytest

import titanskies_pipeline.storage.duckdb.schemas.tempo as tempo_schemas
from titanskies_pipeline.storage.duckdb.demo_seed import seed_demo_warehouse
from titanskies_pipeline.storage.duckdb.schemas.tempo import (
    WAREHOUSE_SCHEMA_VERSION,
    bootstrap_all_tempo_tables,
    create_all_tempo_test_tables,
    seed_test_tempo_pipeline_run_event,
)


def test_seed_test_tempo_pipeline_run_event(duck):
    with duck.get_connection() as conn:
        create_all_tempo_test_tables(conn)
        seed_test_tempo_pipeline_run_event(conn)
        row = conn.execute(
            "SELECT run_id, job_name FROM tempo_no2_ops.pipeline_run_events LIMIT 1"
        ).fetchone()
    assert row[0] == "test-run"
    assert row[1] == "tempo_no2_full_pipeline"


def test_demo_seed_includes_mixed_coverage_and_zero_valid_rows(duck):
    with duck.get_connection() as conn:
        seed_demo_warehouse(conn, include_quality_issues=True)
        rows = conn.execute(
            """
            select canonical_region_id, coverage_fraction, no2_mean,
                   quality_flag_accepted
            from tempo_no2_raw.region_hour_aggregates
            where coverage_fraction < 0.5
            order by coverage_fraction, canonical_region_id
            """
        ).fetchall()
    assert rows[0] == ("MX-CMX-002", 0.0, None, False)
    assert rows[1][0:2] == ("CA-ON-3520005", 0.1)


def test_populated_pre_v04_warehouse_requires_rebuild(tmp_path):
    conn = duckdb.connect(str(tmp_path / "v01.duckdb"))
    conn.execute("create schema tempo_no2_raw")
    conn.execute(
        "create table tempo_no2_raw.region_granule_aggregates (granule_id varchar)"
    )
    conn.execute("insert into tempo_no2_raw.region_granule_aggregates values ('old')")
    with pytest.raises(RuntimeError, match="schema 0.5.0 requires a clean rebuild"):
        bootstrap_all_tempo_tables(conn)
    conn.close()


def test_empty_v01_bootstrap_table_is_replaced(tmp_path):
    conn = duckdb.connect(str(tmp_path / "empty-v01.duckdb"))
    conn.execute("create schema tempo_no2_raw")
    conn.execute("create schema tempo_no2_ops")
    conn.execute(
        "create table tempo_no2_raw.region_granule_aggregates (granule_id varchar)"
    )
    bootstrap_all_tempo_tables(conn)
    columns = {
        row[1]
        for row in conn.execute(
            "pragma table_info('tempo_no2_raw.region_hour_aggregates')"
        ).fetchall()
    }
    conn.close()
    assert {"source_granule_count", "revision"} <= columns


def test_v01_ops_data_also_marks_warehouse_as_populated(tmp_path):
    conn = duckdb.connect(str(tmp_path / "ops-v01.duckdb"))
    conn.execute("create schema tempo_no2_raw")
    conn.execute("create schema tempo_no2_ops")
    conn.execute(
        "create table tempo_no2_raw.region_granule_aggregates (granule_id varchar)"
    )
    conn.execute("create table tempo_no2_ops.region_registry (id varchar)")
    conn.execute("insert into tempo_no2_ops.region_registry values ('US')")
    with pytest.raises(RuntimeError, match="schema 0.5.0 requires a clean rebuild"):
        bootstrap_all_tempo_tables(conn)
    conn.close()


def test_explicit_older_schema_metadata_requires_rebuild(tmp_path):
    conn = duckdb.connect(str(tmp_path / "metadata-v02.duckdb"))
    conn.execute("create schema tempo_no2_ops")
    conn.execute(
        """
        create table tempo_no2_ops.warehouse_metadata (
            metadata_key varchar primary key,
            metadata_value varchar,
            updated_at timestamp
        )
        """
    )
    conn.execute(
        "insert into tempo_no2_ops.warehouse_metadata values "
        "('schema_version', '0.2', current_timestamp)"
    )
    with pytest.raises(RuntimeError, match="contains schema 0.2"):
        bootstrap_all_tempo_tables(conn)
    conn.close()


def test_v05_shared_metadata_and_riverpulse_tables_are_bootstrapped(duck):
    with duck.get_connection() as conn:
        version = conn.execute(
            """
            select metadata_value
            from titanskies_ops.warehouse_metadata
            where metadata_key = 'schema_version'
            """
        ).fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                """
                select table_name
                from information_schema.tables
                where table_schema = 'riverpulse_events_raw'
                """
            ).fetchall()
        }
    assert WAREHOUSE_SCHEMA_VERSION == "0.5.0"
    assert version == "0.5.0"
    assert {
        "reaches",
        "reach_edges",
        "observation_revisions",
        "discharge_revisions",
        "observation_snapshot_links",
    } <= tables


def test_shared_older_schema_metadata_requires_clean_rebuild(tmp_path):
    conn = duckdb.connect(str(tmp_path / "shared-v04.duckdb"))
    conn.execute("create schema titanskies_ops")
    conn.execute(
        """
        create table titanskies_ops.warehouse_metadata (
            metadata_key varchar primary key,
            metadata_value varchar,
            updated_at timestamp
        )
        """
    )
    conn.execute(
        "insert into titanskies_ops.warehouse_metadata values "
        "('schema_version', '0.4', current_timestamp)"
    )
    with pytest.raises(RuntimeError, match="contains schema 0.4"):
        bootstrap_all_tempo_tables(conn)
    conn.close()


@pytest.mark.parametrize("schema", ["titanskies_ops", "tempo_no2_ops"])
def test_empty_schema_metadata_table_can_be_bootstrapped(tmp_path, schema):
    conn = duckdb.connect(str(tmp_path / f"empty-{schema}.duckdb"))
    conn.execute(f"create schema {schema}")
    conn.execute(
        f"""
        create table {schema}.warehouse_metadata (
            metadata_key varchar primary key,
            metadata_value varchar,
            updated_at timestamp
        )
        """
    )
    bootstrap_all_tempo_tables(conn)
    assert (
        conn.execute(
            """
        select metadata_value
        from titanskies_ops.warehouse_metadata
        where metadata_key = 'schema_version'
        """
        ).fetchone()[0]
        == "0.5.0"
    )
    conn.close()


def test_schema_stamp_check_rejects_mismatch_defensively(tmp_path):
    conn = duckdb.connect(str(tmp_path / "direct-mismatch.duckdb"))
    conn.execute("create schema titanskies_ops")
    conn.execute(
        """
        create table titanskies_ops.warehouse_metadata (
            metadata_key varchar primary key,
            metadata_value varchar,
            updated_at timestamp
        )
        """
    )
    conn.execute(
        "insert into titanskies_ops.warehouse_metadata values "
        "('schema_version', 'unexpected', current_timestamp)"
    )
    with pytest.raises(RuntimeError, match="contains schema unexpected"):
        tempo_schemas._check_and_stamp_schema_version(conn)
    conn.close()
