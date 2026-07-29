#!/usr/bin/env python3
"""Build the credential-free PlumeGraph evidence-ledger demo."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = ROOT / ".cache" / "plumegraph-demo"
DATABASE = CACHE_ROOT / "plumegraph-demo.duckdb"


def _reset_demo() -> None:
    if CACHE_ROOT.parent != ROOT / ".cache":
        raise RuntimeError("Refusing to reset an unexpected demo path")
    shutil.rmtree(CACHE_ROOT, ignore_errors=True)
    CACHE_ROOT.mkdir(parents=True)


def _dbt_build() -> None:
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


def main() -> None:
    _reset_demo()
    os.environ.update(
        {
            "DUCKDB_PATH": str(DATABASE),
            "DUCKDB_NAME": str(DATABASE),
            "PLUMEGRAPH_RAW_DATA_DIR": str(CACHE_ROOT / "raw"),
            "PLUMEGRAPH_RELEASE_DIR": str(CACHE_ROOT / "releases"),
            "PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED": "false",
        }
    )
    from titanskies_pipeline.plumegraph.analysis import run_pending_analysis
    from titanskies_pipeline.plumegraph.release import build_release, verify_release
    from titanskies_pipeline.plumegraph.synthetic import (
        seed_synthetic_sources,
        write_synthetic_benchmark,
        write_synthetic_cohort,
    )
    from titanskies_pipeline.plumegraph.validation import (
        load_benchmark,
        run_validation,
    )
    from titanskies_pipeline.storage.duckdb import connection

    cohort = write_synthetic_cohort(CACHE_ROOT / "cohort.json")
    benchmark = write_synthetic_benchmark(CACHE_ROOT / "benchmark.json")
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        sources = seed_synthetic_sources(
            cohort_path=cohort,
            raw_data_dir=CACHE_ROOT / "raw",
            conn=conn,
        )
        first_analysis = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
        idempotent_analysis = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
    finally:
        conn.close()
    _dbt_build()
    conn = connection.get_persistent_connection()
    try:
        load_benchmark(benchmark, allow_incomplete=True, conn=conn)
        validation = run_validation(
            "synthetic-benchmark-v1",
            allow_incomplete=True,
            conn=conn,
        )
    finally:
        conn.close()
    _dbt_build()
    conn = connection.get_persistent_connection()
    try:
        release = build_release(
            release_version="synthetic-v1",
            output_dir=CACHE_ROOT / "releases",
            validation_run_id=validation.validation_run_id,
            conn=conn,
        )
        manifest = verify_release(release.release_path)
    finally:
        conn.close()
    read_only = duckdb.connect(str(DATABASE), read_only=True)
    try:
        relations = (
            "plumegraph_events_marts.plumegraph_events_facilities",
            "plumegraph_events_marts.plumegraph_events_episodes",
            "plumegraph_events_marts.plumegraph_events_episode_revisions",
            "plumegraph_events_marts.plumegraph_events_candidate_sources",
            "plumegraph_events_marts.plumegraph_events_emission_estimates",
            "plumegraph_events_marts.plumegraph_events_evidence_pixels",
            "plumegraph_events_marts.plumegraph_events_provenance",
        )
        counts = {
            relation: read_only.execute(
                f"SELECT count(*) FROM {relation}"
            ).fetchone()[0]
            for relation in relations
        }
        example = read_only.execute(
            """
            SELECT
                episodes.plume_id,
                episodes.episode_revision_id,
                episodes.attribution_class,
                count(distinct revisions.episode_revision_id) as revision_count,
                count(distinct provenance.source_snapshot_id) as source_snapshots
            FROM plumegraph_events_marts.plumegraph_events_episodes as episodes
            INNER JOIN
                plumegraph_events_marts.plumegraph_events_episode_revisions
                    as revisions using (plume_id)
            INNER JOIN plumegraph_events_marts.plumegraph_events_provenance
                as provenance
                on
                    episodes.episode_revision_id
                    = provenance.episode_revision_id
            GROUP BY ALL
            ORDER BY episodes.plume_id
            LIMIT 1
            """
        ).fetchone()
    finally:
        read_only.close()
    print(f"warehouse={DATABASE}")
    print(f"sources={sources}")
    print(f"first_analysis={first_analysis}")
    print(f"idempotent_analysis={idempotent_analysis}")
    print(f"validation={validation}")
    print(f"release={release}")
    print(f"release_files={len(manifest['files'])}")
    for relation, count in counts.items():
        print(f"relation={relation} rows={count}")
    print(f"current_revision_provenance={example}")


if __name__ == "__main__":
    main()
