"""Resolve exact paper-source metadata and persist the readiness preflight."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.reproductions.preflight import run_preflight
from titanskies_pipeline.reproductions.readiness import resolve_reproduction_sources
from titanskies_pipeline.storage.duckdb.connection import (
    reset_duckdb_connection_state,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve metadata-only exact paper sources. This command never enables "
            "fallback acquisition or downloads scientific payloads."
        )
    )
    parser.add_argument("profile", choices=(SOURCE_SUN2025, SOURCE_ANDREADIS2025))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--evidence-bundle", required=True, type=Path)
    parser.add_argument("--import-directory", required=True, type=Path)
    parser.add_argument("--output-inventory", required=True, type=Path)
    parser.add_argument("--duckdb-path", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--object-budget", type=int)
    parser.add_argument("--byte-budget", type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    os.environ["DUCKDB_PATH"] = str(args.duckdb_path.resolve())
    reset_duckdb_connection_state()
    resolution = resolve_reproduction_sources(
        args.profile,
        evidence_path=args.evidence_bundle,
        import_dir=args.import_directory,
        output_path=args.output_inventory,
        manifest_path=args.manifest,
        timeout_seconds=args.timeout_seconds,
    )
    preflight = run_preflight(
        args.profile,
        manifest_path=args.manifest,
        inventory_path=args.output_inventory,
        exact_mode=True,
        max_objects=args.object_budget,
        max_bytes=args.byte_budget,
        fail_on_blocked=False,
    )
    payload = {
        "resolution": asdict(resolution),
        "preflight": {
            **asdict(preflight),
            "blocking_sources": list(preflight.blocking_sources),
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if preflight.status == "ready":
        return 0
    if resolution.operator_input_required_count or resolution.transient_error_count:
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
