"""Validate a paper-reproduction inventory and persist its preflight ledger."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.reproductions.preflight import (
    PreflightBlockedError,
    run_preflight,
)
from titanskies_pipeline.storage.duckdb.connection import (
    reset_duckdb_connection_state,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a provider-discovery inventory without downloading source "
            "payloads."
        )
    )
    parser.add_argument(
        "profile",
        choices=(SOURCE_SUN2025, SOURCE_ANDREADIS2025),
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--duckdb-path", type=Path)
    parser.add_argument("--max-objects", type=int)
    parser.add_argument("--max-bytes", type=int)
    parser.add_argument(
        "--allow-fallbacks",
        action="store_true",
        help="Permit only the provider-reprocessed/method-equivalent fallbacks declared by the manifest.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.duckdb_path:
        os.environ["DUCKDB_PATH"] = str(args.duckdb_path.resolve())
        reset_duckdb_connection_state()
    try:
        metrics = run_preflight(
            args.profile,
            manifest_path=args.manifest,
            inventory_path=args.inventory,
            exact_mode=not args.allow_fallbacks,
            max_objects=args.max_objects,
            max_bytes=args.max_bytes,
        )
    except PreflightBlockedError as exc:
        payload = asdict(exc.metrics)
        payload["blocking_sources"] = list(exc.metrics.blocking_sources)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2
    payload = asdict(metrics)
    payload["blocking_sources"] = list(metrics.blocking_sources)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
