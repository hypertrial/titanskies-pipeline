#!/usr/bin/env python3
"""Run opt-in TEMPO discovery or a disposable two-granule live smoke."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
CACHE_ROOT = REPO_ROOT / ".cache" / "live-readiness"
RESULT_PATH = CACHE_ROOT / "result.json"
LIVE_SMOKE_LOOKBACK_HOURS = 24


def _sanitize_text(value: str) -> str:
    sanitized = value
    for name in ("EARTHDATA_USERNAME", "EARTHDATA_PASSWORD"):
        secret = os.getenv(name)
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    sanitized = re.sub(
        r"(https?://)(?:[^/@\s]+)@",
        r"\1[REDACTED]@",
        sanitized,
    )
    return re.sub(r"(https?://[^?\s]+)\?\S+", r"\1?[REDACTED]", sanitized)


def sanitize_log_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(_sanitize_text(source.read_text(errors="replace")))


def _write_result(result: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


def _run_discovery() -> dict[str, object]:
    from titanskies_pipeline.ingestion.tempo.cmr import discover_granules

    granules = discover_granules(lookback_hours=2)
    return {
        "status": "ok",
        "phase": "discovery",
        "granules_found": len(granules),
        "granule_ids": [granule.granule_id for granule in granules[:5]],
    }


def _require_credentials() -> None:
    missing = [
        name
        for name in ("EARTHDATA_USERNAME", "EARTHDATA_PASSWORD")
        if not os.getenv(name)
    ]
    if missing:
        raise RuntimeError(f"missing Earthdata credentials: {', '.join(missing)}")


def _reset_disposable_paths() -> None:
    for name in (
        "live.duckdb",
        "live.duckdb.wal",
        "live.duckdb-wal",
        "live.duckdb-shm",
    ):
        (CACHE_ROOT / name).unlink(missing_ok=True)
    for name in ("raw", "dbt-target"):
        path = CACHE_ROOT / name
        if path.parent != CACHE_ROOT:
            raise RuntimeError(f"refusing to reset unexpected path: {path}")
        shutil.rmtree(path, ignore_errors=True)


def _configure_live_environment() -> None:
    os.environ.update(
        {
            "DUCKDB_PATH": str(CACHE_ROOT / "live.duckdb"),
            "DUCKDB_NAME": str(CACHE_ROOT / "live.duckdb"),
            "DBT_TARGET_PATH": str(CACHE_ROOT / "dbt-target"),
            "DBT_LOG_PATH": str(CACHE_ROOT / "dbt-target"),
            "TEMPO_NO2_RAW_DATA_DIR": str(CACHE_ROOT / "raw"),
            "TEMPO_GEOGRAPHY_MANIFEST_PATH": str(
                CACHE_ROOT / "geo" / "tempo_geography_artifacts.json"
            ),
            "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED": "false",
        }
    )


def _run_live_smoke() -> dict[str, object]:
    _require_credentials()
    _reset_disposable_paths()
    _configure_live_environment()

    from scripts.build_region_artifacts import build_artifacts

    build_artifacts(
        CACHE_ROOT / "geo",
        use_synthetic=False,
        source_cache=CACHE_ROOT / "geo-sources",
    )

    from dagster import materialize
    from dagster_dbt import DbtCliResource

    from titanskies_pipeline.config.settings import DBT_PROFILES_DIR
    from titanskies_pipeline.orchestration.assets_tempo_no2 import (
        tempo_no2_ops_region_registry,
        tempo_no2_raw_granule_inventory,
        tempo_no2_raw_region_hour_aggregates,
        titanskies_dbt,
    )
    from titanskies_pipeline.orchestration.dbt_project import DBT_PROJECT
    from titanskies_pipeline.storage.duckdb.connection import (
        get_persistent_connection,
        reset_duckdb_connection_state,
    )

    reset_duckdb_connection_state()
    bootstrap = materialize([tempo_no2_ops_region_registry])
    if not bootstrap.success:
        raise RuntimeError("Production geography registration failed")
    result = materialize(
        [
            tempo_no2_raw_granule_inventory,
            tempo_no2_raw_region_hour_aggregates,
            titanskies_dbt,
        ],
        resources={
            "dbt": DbtCliResource(
                project_dir=str(DBT_PROJECT.project_dir),
                profiles_dir=str(DBT_PROFILES_DIR),
            )
        },
        run_config={
            "ops": {
                "tempo__no2__raw__granule_inventory": {
                    "config": {"lookback_hours": LIVE_SMOKE_LOOKBACK_HOURS}
                },
                "tempo__no2__raw__region_hour_aggregates": {
                    "config": {"max_granules": 2}
                },
            }
        },
    )
    if not result.success:
        raise RuntimeError("Dagster materialization failed")

    conn = get_persistent_connection()
    try:
        processed = int(
            conn.execute(
                "SELECT count(*) FROM tempo_no2_ops.granule_inventory "
                "WHERE processing_status = 'processed'"
            ).fetchone()[0]
        )
        mart_rows = int(
            conn.execute(
                "SELECT count(*) FROM tempo_no2_marts.tempo_no2_region_hourly"
            ).fetchone()[0]
        )
        grid_rows = int(
            conn.execute(
                "SELECT count(*) FROM tempo_no2_marts.tempo_no2_grid_latest"
            ).fetchone()[0]
        )
        dq_counts = dict(
            conn.execute(
                "SELECT severity, count(*) "
                "FROM tempo_no2_observability.tempo_no2_data_quality GROUP BY 1"
            ).fetchall()
        )
    finally:
        conn.close()
    if processed < 1 or mart_rows < 1 or grid_rows < 1:
        raise RuntimeError(
            "live warehouse validation failed: "
            f"processed={processed} mart_rows={mart_rows} grid_rows={grid_rows}"
        )
    return {
        "status": "ok",
        "phase": "live-smoke",
        "processed_granules": processed,
        "region_hourly_rows": mart_rows,
        "grid_latest_rows": grid_rows,
        "dq_counts": dq_counts,
    }


def _run_geography() -> dict[str, object]:
    from scripts.build_region_artifacts import DEFAULT_MANIFEST, build_artifacts

    from titanskies_pipeline.geography.build import load_source_manifest

    started = time.perf_counter()
    metrics = build_artifacts(
        CACHE_ROOT / "geo",
        use_synthetic=False,
        source_cache=CACHE_ROOT / "geo-sources",
    )
    manifest = load_source_manifest(DEFAULT_MANIFEST)
    return {
        "status": "ok",
        "phase": "geography",
        "source_versions": [source["version"] for source in manifest["sources"]],
        "geometry_version": metrics["geometry_version"],
        "grid_version": metrics["grid_version"],
        "registry_checksum": metrics["registry_checksum"],
        "weights_checksum": metrics["weights_checksum"],
        "region_count": metrics["region_count"],
        "weight_count": metrics["weight_count"],
        "duration_seconds": round(time.perf_counter() - started, 3),
        "registry_size_bytes": Path(metrics["registry_path"]).stat().st_size,
        "weights_size_bytes": Path(metrics["weights_path"]).stat().st_size,
    }


def _failure_phase(mode: str, exc: Exception) -> str:
    if mode == "discovery":
        return mode
    message = str(exc).lower()
    for phase, terms in (
        ("authentication", ("credential", "login", "unauthorized", "forbidden")),
        ("download", ("download", "earthaccess returned no files")),
        ("netcdf", ("netcdf", "product group", "quality flag")),
        ("aggregation", ("aggregate", "region", "geography", "grid")),
        ("dbt", ("dbt", "mart", "data_quality")),
    ):
        if any(term in message for term in terms):
            return phase
    return mode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=("discovery", "geography", "live-smoke"), required=True
    )
    parser.add_argument("--result-path", type=Path, default=RESULT_PATH)
    args = parser.parse_args(argv)
    phase = args.mode
    try:
        if args.mode == "discovery":
            result = _run_discovery()
        elif args.mode == "geography":
            result = _run_geography()
        else:
            result = _run_live_smoke()
    except Exception as exc:
        _write_result(
            {
                "status": "failed",
                "phase": _failure_phase(phase, exc),
                "error": _sanitize_text(str(exc)),
            },
            args.result_path,
        )
        return 1
    _write_result(result, args.result_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
