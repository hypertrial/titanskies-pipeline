#!/usr/bin/env python3
"""Build deterministic synthetic or pinned production geography artifacts."""

from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from titanskies_pipeline.geography.build import (
    build_production_artifacts,
    publish_artifact_generation,
)

try:
    from scripts.generate_geo_fixtures import REGISTRY_ROWS, WEIGHT_ROWS
except ModuleNotFoundError:  # direct execution places scripts/ on sys.path
    from generate_geo_fixtures import REGISTRY_ROWS, WEIGHT_ROWS

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "config" / "geography_sources.json"


def _atomic_synthetic_table(table: pa.Table, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, temporary, compression="zstd", version="2.6")
        if pq.ParquetFile(temporary).metadata.num_rows != table.num_rows:
            raise ValueError(
                f"Synthetic artifact validation failed: {destination.name}"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def build_artifacts(
    output_dir: Path,
    *,
    use_synthetic: bool,
    source_cache: Path | None = None,
    offline: bool = False,
) -> dict[str, object]:
    output_dir = output_dir.resolve()
    if not use_synthetic:
        return build_production_artifacts(
            output_dir=output_dir,
            source_cache=(source_cache or ROOT / ".cache" / "geo_sources").resolve(),
            manifest_path=DEFAULT_MANIFEST,
            offline=offline,
        )

    source_manifest_sha256 = hashlib.sha256(b"synthetic-v1").hexdigest()
    metadata = {
        b"grid_version": b"synthetic-v1",
        b"geometry_version": b"test-v1",
        b"source_manifest_sha256": source_manifest_sha256.encode(),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    build_dir = Path(tempfile.mkdtemp(prefix=".build.", dir=output_dir))
    registry_path = build_dir / "tempo_region_registry.parquet"
    weights_path = build_dir / "tempo_grid_region_weights.parquet"
    registry = (
        pa.Table.from_pylist(REGISTRY_ROWS)
        .sort_by([("canonical_region_id", "ascending")])
        .replace_schema_metadata(metadata)
    )
    weights = (
        pa.Table.from_pylist(WEIGHT_ROWS)
        .sort_by(
            [
                ("canonical_region_id", "ascending"),
                ("grid_row", "ascending"),
                ("grid_col", "ascending"),
            ]
        )
        .replace_schema_metadata(metadata)
    )
    _atomic_synthetic_table(registry, registry_path)
    _atomic_synthetic_table(weights, weights_path)
    try:
        return publish_artifact_generation(
            output_dir=output_dir,
            registry_source=registry_path,
            weights_source=weights_path,
            artifact_mode="synthetic",
            geometry_version="test-v1",
            grid_version="synthetic-v1",
            source_manifest_sha256=source_manifest_sha256,
            region_count=registry.num_rows,
            weight_count=weights.num_rows,
        )
    finally:
        import shutil

        shutil.rmtree(build_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/geo"),
    )
    parser.add_argument(
        "--source-cache",
        type=Path,
        default=Path(".cache/geo_sources"),
        help="Cache for checksum-verified provider downloads.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Require every pinned provider source to exist in --source-cache.",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Write the bundled credential-free mini geography.",
    )
    args = parser.parse_args()
    metrics = build_artifacts(
        args.output_dir,
        use_synthetic=args.synthetic,
        source_cache=args.source_cache,
        offline=args.offline,
    )
    print(
        f"manifest={metrics['manifest_path']} build={metrics['build_id']} "
        f"mode={metrics['artifact_mode']}"
    )
    print(
        f"registry={metrics['registry_path']} regions={metrics['region_count']} "
        f"checksum={metrics['registry_checksum']}"
    )
    print(
        f"weights={metrics['weights_path']} rows={metrics['weight_count']} "
        f"checksum={metrics['weights_checksum']} grid={metrics['grid_version']}"
    )


if __name__ == "__main__":
    main()
