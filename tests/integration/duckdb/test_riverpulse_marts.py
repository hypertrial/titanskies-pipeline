"""RiverPulse incremental/current-revision dbt integration contract."""

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from tests.integration.conftest import write_dbt_profile
from tests.integration.duckdb.test_golden_marts import DBT_ROOT, _run_dbt

import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.riverpulse.collection import (
    pending_requests,
    persist_fetch_result,
    plan_source_requests,
)
from titanskies_pipeline.riverpulse.hydrocron import FetchResult
from titanskies_pipeline.riverpulse.network import (
    persist_network_artifacts,
    publish_network_generation,
    synthetic_network_rows,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
CASSETTE = REPO_ROOT / "tests" / "fixtures" / "cassettes" / "riverpulse_hydrocron.csv"
UTC = timezone.utc


def _riverpulse_build_args(*extra: str) -> list[str]:
    return [
        "build",
        *extra,
        "--select",
        "tag:riverpulse,tag:events",
    ]


def test_incremental_correction_contract_invalidation_matches_full_refresh(
    tmp_path, dbt_profiles_dir, monkeypatch
):
    db_path = tmp_path / "riverpulse.duckdb"
    raw_dir = tmp_path / "raw"
    write_dbt_profile(dbt_profiles_dir, db_path)
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("RIVERPULSE_RAW_DATA_DIR", str(raw_dir))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    reaches, edges, anchors = synthetic_network_rows()
    artifacts = publish_network_generation(
        output_dir=tmp_path / "network",
        reaches=reaches,
        edges=edges,
        artifact_mode="synthetic",
        source_manifest_sha256=hashlib.sha256(b"synthetic").hexdigest(),
        resolved_anchors=anchors,
    )
    persist_network_artifacts(artifacts, conn=conn)
    plan_source_requests(
        window_start=datetime(2024, 1, 1, tzinfo=UTC),
        window_end=datetime(2025, 1, 1, tzinfo=UTC),
        reach_ids=["RP1001"],
        conn=conn,
    )
    request = pending_requests(conn=conn)[0]
    persist_fetch_result(
        request,
        FetchResult(200, CASSETTE.read_bytes(), 1, False),
        collected_at=datetime(2024, 7, 8, tzinfo=UTC),
        raw_data_dir=raw_dir,
        conn=conn,
    )
    conn.close()

    env = os.environ.copy()
    _run_dbt(
        _riverpulse_build_args(),
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    conn = duckdb.connect(str(db_path))
    initial = conn.execute(
        """
        select observation_id, observation_revision_id, wse, contract_version
        from riverpulse_events_intermediate.int_riverpulse_events_current_observations
        order by observation_id
        """
    ).fetchall()
    conn.close()
    assert len(initial) == 2

    conn = connection.get_persistent_connection()
    persist_fetch_result(
        request,
        FetchResult(
            200,
            CASSETTE.read_bytes().replace(b"8.47", b"8.49", 1),
            1,
            False,
        ),
        collected_at=datetime(2024, 7, 9, tzinfo=UTC),
        raw_data_dir=raw_dir,
        conn=conn,
    )
    conn.close()
    _run_dbt(
        _riverpulse_build_args(),
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    conn = duckdb.connect(str(db_path))
    corrected = conn.execute(
        """
        select crid, wse
        from riverpulse_events_marts.riverpulse_events_observations
        where observation_time = timestamp '2024-06-15 10:30:00'
        """
    ).fetchone()
    assert corrected == ("PIC1", 8.49)
    conn.close()

    copied_dbt = tmp_path / "dbt-contract-change"
    shutil.copytree(
        DBT_ROOT,
        copied_dbt,
        ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"),
    )
    contract = copied_dbt / "seeds" / "riverpulse_events_contract.csv"
    contract.write_text(contract.read_text().replace("0.5.0", "0.5-test"))
    _run_dbt(
        _riverpulse_build_args(),
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=copied_dbt,
    )
    conn = duckdb.connect(str(db_path))
    versions = conn.execute(
        """
        select distinct contract_version
        from riverpulse_events_intermediate.int_riverpulse_events_current_observations
        """
    ).fetchall()
    assert versions == [("0.5-test",)]
    conn.execute(
        """
        create table riverpulse_incremental_snapshot as
        select * from riverpulse_events_marts.riverpulse_events_observations
        """
    )
    conn.close()

    _run_dbt(
        _riverpulse_build_args("--full-refresh"),
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=copied_dbt,
    )
    conn = duckdb.connect(str(db_path))
    difference = conn.execute(
        """
        select count(*) from (
            (select * from riverpulse_incremental_snapshot
             except select * from riverpulse_events_marts.riverpulse_events_observations)
            union all
            (select * from riverpulse_events_marts.riverpulse_events_observations
             except select * from riverpulse_incremental_snapshot)
        )
        """
    ).fetchone()[0]
    provenance_gaps = conn.execute(
        """
        select count(*)
        from riverpulse_events_marts.riverpulse_events_observations
        where source_snapshot_count < 1 or response_sha256 is null
        """
    ).fetchone()[0]
    decoded_flags = conn.execute(
        """
        select
            has_classification_quality_suspect,
            unconstrained_discharge_quality_bits,
            constrained_discharge_quality_bits
        from riverpulse_events_marts.riverpulse_events_observations
        where observation_time = timestamp '2024-07-06 10:18:00'
        """
    ).fetchone()
    conn.close()
    assert difference == 0
    assert provenance_gaps == 0
    assert decoded_flags == (True, 1, 1)
