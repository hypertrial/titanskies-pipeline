#!/usr/bin/env python3
"""Build and summarize the credential-free RiverPulse vertical-slice demo."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = ROOT / ".cache" / "riverpulse-demo"
DEMO_PATH = CACHE_ROOT / "riverpulse-demo.duckdb"


def _reset_demo() -> None:
    if CACHE_ROOT.parent != ROOT / ".cache":
        raise RuntimeError("Refusing to reset an unexpected demo path")
    shutil.rmtree(CACHE_ROOT, ignore_errors=True)
    CACHE_ROOT.mkdir(parents=True)


def main() -> None:
    _reset_demo()
    network_dir = CACHE_ROOT / "network"
    raw_dir = CACHE_ROOT / "raw"
    os.environ.update(
        {
            "DUCKDB_PATH": str(DEMO_PATH),
            "DUCKDB_NAME": str(DEMO_PATH),
            "RIVERPULSE_NETWORK_MANIFEST_PATH": str(
                network_dir / "riverpulse_network_artifacts.json"
            ),
            "RIVERPULSE_RAW_DATA_DIR": str(raw_dir),
            "RIVERPULSE_REQUEST_INTERVAL_SECONDS": "0",
            "RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED": "false",
        }
    )

    from build_riverpulse_network import build_network

    artifacts = build_network(
        output_dir=network_dir,
        synthetic=True,
        source_cache=CACHE_ROOT / "source-cache",
        offline=True,
    )

    from titanskies_pipeline.riverpulse.collection import (
        pending_requests,
        persist_fetch_result,
        plan_source_requests,
    )
    from titanskies_pipeline.riverpulse.hydrocron import FetchResult
    from titanskies_pipeline.riverpulse.network import persist_network_artifacts
    from titanskies_pipeline.storage.duckdb import connection

    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        persist_network_artifacts(artifacts, conn=conn)
        plan_source_requests(
            window_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            window_end=datetime(2025, 1, 1, tzinfo=timezone.utc),
            reach_ids=["RP1001"],
            conn=conn,
        )
        request = pending_requests(conn=conn)[0]
        cassette = (
            ROOT / "tests" / "fixtures" / "cassettes" / "riverpulse_hydrocron.csv"
        ).read_bytes()
        result = FetchResult(200, cassette, 1, False)
        first_ingest = persist_fetch_result(
            request,
            result,
            collected_at=datetime(2024, 7, 8, tzinfo=timezone.utc),
            raw_data_dir=raw_dir,
            conn=conn,
        )
        idempotent_rerun = persist_fetch_result(
            request,
            result,
            collected_at=datetime(2024, 7, 8, tzinfo=timezone.utc),
            raw_data_dir=raw_dir,
            conn=conn,
        )
    finally:
        conn.close()

    common = [
        "--project-dir",
        str(ROOT / "dbt"),
        "--profiles-dir",
        str(ROOT / "dbt" / "profiles"),
    ]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--select",
            "tag:riverpulse,tag:events",
            *common,
        ],
        check=True,
        env=os.environ.copy(),
    )

    conn = duckdb.connect(str(DEMO_PATH), read_only=True)
    try:
        relations = (
            "riverpulse_events_raw.reaches",
            "riverpulse_events_raw.observation_revisions",
            "riverpulse_events_raw.discharge_revisions",
            "riverpulse_events_marts.riverpulse_events_reaches",
            "riverpulse_events_marts.riverpulse_events_observations",
            "riverpulse_events_marts.riverpulse_events_observation_revisions",
            "riverpulse_events_marts.riverpulse_events_discharges",
        )
        counts = [
            (relation, conn.execute(f"select count(*) from {relation}").fetchone()[0])
            for relation in relations
        ]
        current_vs_revision = conn.execute(
            """
            select
                observations.observation_id,
                observations.crid as current_crid,
                observations.wse as current_wse,
                count(revisions.observation_revision_id) as revision_count
            from riverpulse_events_marts.riverpulse_events_observations as observations
            inner join riverpulse_events_marts.riverpulse_events_observation_revisions
                as revisions using (observation_id)
            group by all
            order by observations.observation_id
            """
        ).fetchall()
    finally:
        conn.close()

    print(f"warehouse={DEMO_PATH}")
    print(
        f"network_build={artifacts.build_id} reaches={artifacts.reach_count} "
        f"anchors={artifacts.resolved_anchors}"
    )
    print(f"first_ingest={first_ingest}")
    print(f"idempotent_rerun={idempotent_rerun}")
    for relation, count in counts:
        print(f"relation={relation} rows={count}")
    print(f"current_vs_revision={current_vs_revision}")


if __name__ == "__main__":
    main()
