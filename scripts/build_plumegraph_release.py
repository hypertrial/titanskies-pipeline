#!/usr/bin/env python3
"""Build and verify an immutable PlumeGraph release from the current warehouse."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from titanskies_pipeline.plumegraph.release import build_release, verify_release


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-version", required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--validation-run-id")
    args = parser.parse_args()
    result = build_release(
        release_version=args.release_version,
        output_dir=args.output_dir,
        validation_run_id=args.validation_run_id,
    )
    manifest = verify_release(result.release_path)
    print(
        json.dumps(
            {
                "status": "ok",
                "release_id": result.release_id,
                "release_path": str(result.release_path),
                "manifest_sha256": result.manifest_sha256,
                "episode_count": result.episode_count,
                "file_count": result.file_count,
                "evidence_format": manifest["evidence_format"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
