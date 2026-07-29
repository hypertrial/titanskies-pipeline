#!/usr/bin/env python3
"""Build the synthetic or pinned production RiverPulse SWORD network."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from titanskies_pipeline.riverpulse.network import (
    build_production_network,
    publish_network_generation,
    synthetic_network_rows,
)

ROOT = Path(__file__).resolve().parents[1]


def build_network(
    *,
    output_dir: Path,
    synthetic: bool,
    source_cache: Path,
    offline: bool,
):
    if not synthetic:
        return build_production_network(
            output_dir=output_dir,
            source_cache=source_cache,
            source_manifest_path=ROOT / "config" / "riverpulse_sources.json",
            pilots_path=ROOT / "config" / "riverpulse_pilots.json",
            offline=offline,
        )
    reaches, edges, anchors = synthetic_network_rows()
    return publish_network_generation(
        output_dir=output_dir,
        reaches=reaches,
        edges=edges,
        artifact_mode="synthetic",
        source_manifest_sha256=hashlib.sha256(
            b"riverpulse-synthetic-network-v1"
        ).hexdigest(),
        resolved_anchors=anchors,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir", type=Path, default=Path("artifacts/riverpulse")
    )
    parser.add_argument(
        "--source-cache", type=Path, default=Path(".cache/riverpulse/sword")
    )
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    artifacts = build_network(
        output_dir=args.output_dir.resolve(),
        synthetic=args.synthetic,
        source_cache=args.source_cache.resolve(),
        offline=args.offline,
    )
    print(
        f"manifest={artifacts.manifest_path} build={artifacts.build_id} "
        f"mode={artifacts.artifact_mode}"
    )
    print(
        f"reaches={artifacts.reach_count} edges={artifacts.edge_count} "
        f"anchors={artifacts.resolved_anchors}"
    )


if __name__ == "__main__":
    main()
