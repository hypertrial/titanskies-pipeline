#!/usr/bin/env python3
"""Run one approved-facility PlumeGraph day in a disposable warehouse."""

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
CACHE_ROOT = ROOT / ".cache" / "plumegraph-live-smoke"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--facility-id")
    parser.add_argument("--date", default="2024-07-15")
    args = parser.parse_args()
    if CACHE_ROOT.parent != ROOT / ".cache":
        raise RuntimeError("Refusing to reset an unexpected smoke path")
    shutil.rmtree(CACHE_ROOT, ignore_errors=True)
    CACHE_ROOT.mkdir(parents=True)
    source_manifest = json.loads(args.manifest.read_text())
    facilities = source_manifest.get("facilities") or []
    target = next(
        (
            item
            for item in facilities
            if args.facility_id is None
            or str(item.get("facility_id")) == args.facility_id
        ),
        None,
    )
    if target is None:
        raise ValueError("Requested live-smoke facility is not in the cohort")
    smoke_facilities = []
    for item in facilities:
        candidate = dict(item)
        candidate["is_cohort"] = (
            str(candidate["facility_id"]) == str(target["facility_id"])
        )
        smoke_facilities.append(candidate)
    smoke_manifest = {
        **source_manifest,
        "cohort_version": (
            f"{source_manifest['cohort_version']}-live-smoke-"
            f"{target['facility_id']}"
        ),
        "facilities": smoke_facilities,
    }
    smoke_path = CACHE_ROOT / "cohort.json"
    smoke_path.write_text(json.dumps(smoke_manifest, indent=2, sort_keys=True) + "\n")
    database = CACHE_ROOT / "plumegraph-live.duckdb"
    os.environ.update(
        {
            "DUCKDB_PATH": str(database),
            "DUCKDB_NAME": str(database),
            "PLUMEGRAPH_RAW_DATA_DIR": str(CACHE_ROOT / "raw"),
            "PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED": "false",
        }
    )
    from titanskies_pipeline.plumegraph.analysis import run_pending_analysis
    from titanskies_pipeline.plumegraph.connectors import sync_source_connector
    from titanskies_pipeline.plumegraph.sources import (
        persist_cohort,
        plan_source_requests,
    )
    from titanskies_pipeline.storage.duckdb import connection

    start = datetime.fromisoformat(args.date).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        cohort = persist_cohort(
            smoke_path,
            require_approved=True,
            expected_cohort_count=1,
            conn=conn,
        )
        discovery = plan_source_requests(
            window_start=start,
            window_end=end,
            conn=conn,
        )
        ingest = {
            connector: sync_source_connector(
                connector,
                raw_data_dir=CACHE_ROOT / "raw",
                conn=conn,
            ).__dict__
            for connector in ("harmony", "hrrr", "camd")
        }
        analysis = run_pending_analysis(
            partition_dates=[start.date()],
            conn=conn,
        )
    finally:
        conn.close()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--select",
            "tag:plumegraph,tag:events",
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
                "facility_id": target["facility_id"],
                "date": args.date,
                "cohort": cohort,
                "discovery": discovery.__dict__,
                "ingest": ingest,
                "analysis": analysis.__dict__,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
