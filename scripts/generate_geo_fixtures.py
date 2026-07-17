#!/usr/bin/env python3
"""Generate synthetic geography fixtures for tests."""

from __future__ import annotations

from pathlib import Path
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
GEO_DIR = ROOT / "tests" / "fixtures" / "geo"
ARTIFACTS_DIR = ROOT / "artifacts" / "geo"

REGISTRY_ROWS = [
    {
        "country_code": "US",
        "region_type": "country",
        "source_region_id": "US",
        "canonical_region_id": "US",
        "region_name": "United States",
        "parent_region_id": None,
        "timezone": "America/Chicago",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "US",
        "region_type": "state",
        "source_region_id": "06",
        "canonical_region_id": "US-CA",
        "region_name": "California",
        "parent_region_id": "US",
        "timezone": "America/Los_Angeles",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "CA",
        "region_type": "country",
        "source_region_id": "CA",
        "canonical_region_id": "CA",
        "region_name": "Canada",
        "parent_region_id": None,
        "timezone": "America/Winnipeg",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "US",
        "region_type": "county",
        "source_region_id": "06037",
        "canonical_region_id": "US-CA-037",
        "region_name": "Los Angeles County",
        "parent_region_id": "US-CA",
        "timezone": "America/Los_Angeles",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "CA",
        "region_type": "province",
        "source_region_id": "35",
        "canonical_region_id": "CA-ON",
        "region_name": "Ontario",
        "parent_region_id": "CA",
        "timezone": "America/Toronto",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "CA",
        "region_type": "census_subdivision",
        "source_region_id": "3520",
        "canonical_region_id": "CA-ON-3520",
        "region_name": "Toronto",
        "parent_region_id": "CA-ON",
        "timezone": "America/Toronto",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "MX",
        "region_type": "country",
        "source_region_id": "MX",
        "canonical_region_id": "MX",
        "region_name": "Mexico",
        "parent_region_id": None,
        "timezone": "America/Mexico_City",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "MX",
        "region_type": "state",
        "source_region_id": "09",
        "canonical_region_id": "MX-CMX",
        "region_name": "Ciudad de Mexico",
        "parent_region_id": "MX",
        "timezone": "America/Mexico_City",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
    {
        "country_code": "MX",
        "region_type": "municipality",
        "source_region_id": "09002",
        "canonical_region_id": "MX-CMX-002",
        "region_name": "Azcapotzalco",
        "parent_region_id": "MX-CMX",
        "timezone": "America/Mexico_City",
        "geometry_version": "test-v1",
        "geometry_checksum": "registry-test-checksum",
    },
]

WEIGHT_ROWS = [
    {
        "grid_row": 0,
        "grid_col": 0,
        "canonical_region_id": "US-CA-037",
        "overlap_weight": 0.6,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 0,
        "grid_col": 0,
        "canonical_region_id": "US-CA",
        "overlap_weight": 0.6,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 0,
        "grid_col": 0,
        "canonical_region_id": "US",
        "overlap_weight": 0.6,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 0,
        "grid_col": 1,
        "canonical_region_id": "CA-ON-3520",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 0,
        "grid_col": 1,
        "canonical_region_id": "CA-ON",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 0,
        "grid_col": 1,
        "canonical_region_id": "CA",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 1,
        "grid_col": 0,
        "canonical_region_id": "MX-CMX-002",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 1,
        "grid_col": 0,
        "canonical_region_id": "MX-CMX",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
    {
        "grid_row": 1,
        "grid_col": 0,
        "canonical_region_id": "MX",
        "overlap_weight": 1.0,
        "geometry_version": "test-v1",
    },
]


def _with_metadata(table: pa.Table) -> pa.Table:
    return table.replace_schema_metadata(
        {
            b"grid_version": b"synthetic-v1",
            b"geometry_version": b"test-v1",
            b"source_manifest_sha256": hashlib.sha256(b"synthetic-v1")
            .hexdigest()
            .encode(),
        }
    )


def write_fixtures() -> None:
    for directory in (GEO_DIR, ARTIFACTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _with_metadata(pa.Table.from_pylist(REGISTRY_ROWS)),
            directory / "tempo_region_registry.parquet",
            compression="zstd",
        )
        pq.write_table(
            _with_metadata(pa.Table.from_pylist(WEIGHT_ROWS)),
            directory / "tempo_grid_region_weights.parquet",
            compression="zstd",
        )


if __name__ == "__main__":
    write_fixtures()
    print(f"Wrote fixtures to {GEO_DIR} and {ARTIFACTS_DIR}")
