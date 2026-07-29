#!/usr/bin/env python3
"""Run an opt-in one-reach Hydrocron smoke in a disposable warehouse."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = ROOT / ".cache" / "riverpulse-live-smoke"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--reach-id")
    parser.add_argument("--days", type=int, default=30, choices=range(1, 91))
    args = parser.parse_args()
    if CACHE_ROOT.parent != ROOT / ".cache":
        raise RuntimeError("Refusing to reset an unexpected smoke path")
    shutil.rmtree(CACHE_ROOT, ignore_errors=True)
    CACHE_ROOT.mkdir(parents=True)
    database = CACHE_ROOT / "riverpulse-live.duckdb"
    raw_dir = CACHE_ROOT / "raw"
    os.environ.update(
        {
            "DUCKDB_PATH": str(database),
            "DUCKDB_NAME": str(database),
            "RIVERPULSE_NETWORK_MANIFEST_PATH": str(args.manifest.resolve()),
            "RIVERPULSE_RAW_DATA_DIR": str(raw_dir),
            "RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED": "false",
        }
    )

    from titanskies_pipeline.riverpulse.collection import (
        plan_source_requests,
        sync_pending_requests,
    )
    from titanskies_pipeline.riverpulse.network import (
        load_network_artifacts,
        persist_network_artifacts,
    )
    from titanskies_pipeline.storage.duckdb import connection

    artifacts = load_network_artifacts(args.manifest.resolve())
    reach_id = args.reach_id or next(iter(artifacts.resolved_anchors.values()))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        persist_network_artifacts(artifacts, conn=conn)
        end = datetime.now(timezone.utc)
        discovery = plan_source_requests(
            window_start=end - timedelta(days=args.days),
            window_end=end,
            reach_ids=[reach_id],
            conn=conn,
        )
        ingest = sync_pending_requests(conn=conn, raw_data_dir=raw_dir)
    finally:
        conn.close()

    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--select",
            "tag:riverpulse,tag:events",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt" / "profiles"),
        ],
        check=True,
        env=os.environ.copy(),
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "warehouse": str(database),
                "network_build": artifacts.build_id,
                "reach_id": reach_id,
                "discovery": discovery.__dict__,
                "ingest": ingest.__dict__,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
