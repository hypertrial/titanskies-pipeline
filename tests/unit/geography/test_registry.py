from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scripts.build_region_artifacts import build_artifacts
from scripts.generate_geo_fixtures import REGISTRY_ROWS, WEIGHT_ROWS

from titanskies_pipeline.geography.build import publish_artifact_generation
from titanskies_pipeline.geography.registry import (
    GeoArtifacts,
    load_geo_artifacts,
    persist_geo_artifacts,
)
from titanskies_pipeline.storage.duckdb.connection import get_connection
from titanskies_pipeline.storage.duckdb.schemas.constants import tempo_ops_tbl


@pytest.fixture
def valid_artifacts(tmp_path: Path) -> GeoArtifacts:
    metrics = build_artifacts(tmp_path / "geo", use_synthetic=True)
    return load_geo_artifacts(metrics["manifest_path"], allow_synthetic=True)


def _publish(tmp_path: Path, registry: pa.Table, weights: pa.Table) -> Path:
    if "canonical_region_id" in registry.column_names:
        registry = registry.sort_by([("canonical_region_id", "ascending")])
    weights = weights.sort_by(
        [
            ("canonical_region_id", "ascending"),
            ("grid_row", "ascending"),
            ("grid_col", "ascending"),
        ]
    )
    metadata = {
        b"grid_version": b"synthetic-v1",
        b"geometry_version": b"test-v1",
        b"source_manifest_sha256": b"a" * 64,
    }
    registry = registry.replace_schema_metadata(metadata)
    weights = weights.replace_schema_metadata(metadata)
    source = tmp_path / "source"
    source.mkdir(parents=True)
    registry_path = source / "registry.parquet"
    weights_path = source / "weights.parquet"
    pq.write_table(registry, registry_path)
    pq.write_table(weights, weights_path)
    result = publish_artifact_generation(
        output_dir=tmp_path / "geo",
        registry_source=registry_path,
        weights_source=weights_path,
        artifact_mode="synthetic",
        geometry_version="test-v1",
        grid_version="synthetic-v1",
        source_manifest_sha256="a" * 64,
        region_count=registry.num_rows,
        weight_count=weights.num_rows,
    )
    return result["manifest_path"]


def test_load_geo_artifacts_success_and_synthetic_guard(valid_artifacts):
    assert valid_artifacts.geometry_version == "test-v1"
    assert valid_artifacts.region_count == 9
    assert valid_artifacts.weight_count == 9
    assert valid_artifacts.artifact_mode == "synthetic"
    with pytest.raises(ValueError, match="requires a production"):
        load_geo_artifacts(valid_artifacts.manifest_path)


def test_load_geo_artifacts_missing_and_invalid_manifest(tmp_path):
    with pytest.raises(FileNotFoundError, match="manifest not found"):
        load_geo_artifacts(tmp_path / "missing.json")
    invalid = tmp_path / "invalid.json"
    invalid.write_text("not-json")
    with pytest.raises(ValueError, match="Invalid geography manifest"):
        load_geo_artifacts(invalid)
    invalid.write_text("{}")
    with pytest.raises(ValueError, match="missing fields"):
        load_geo_artifacts(invalid)


def test_manifest_rejects_unsupported_version_and_metadata_mismatch(valid_artifacts):
    payload = json.loads(valid_artifacts.manifest_path.read_text())
    payload["manifest_version"] = "2"
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Unsupported geography manifest version"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    payload["manifest_version"] = "1"
    payload["grid_version"] = "wrong-grid"
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="metadata does not match"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)


def test_manifest_rejects_unknown_mode_and_escaping_paths(valid_artifacts):
    payload = json.loads(valid_artifacts.manifest_path.read_text())
    payload["artifact_mode"] = "mystery"
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Unknown geography artifact mode"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)
    payload["artifact_mode"] = "synthetic"
    payload["registry"]["path"] = "../outside.parquet"
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="escapes"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)


def test_manifest_rejects_absolute_path_and_checksum(valid_artifacts):
    payload = json.loads(valid_artifacts.manifest_path.read_text())
    payload["registry"]["path"] = str(valid_artifacts.registry_path)
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="manifest-relative"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)
    payload["registry"]["path"] = valid_artifacts.registry_path.relative_to(
        valid_artifacts.manifest_path.parent
    ).as_posix()
    payload["registry"]["sha256"] = "bad"
    valid_artifacts.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="registry checksum mismatch"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)


def test_artifact_schema_version_and_count_validation(tmp_path):
    weights = pa.Table.from_pylist(WEIGHT_ROWS)
    with pytest.raises(ValueError, match="schema validation failed"):
        _publish(tmp_path / "bad-columns", pa.table({"x": [1]}), weights)

    registry = pa.Table.from_pylist(REGISTRY_ROWS)
    mismatch = _publish(tmp_path / "bad-version", registry, weights)
    payload = json.loads(mismatch.read_text())
    payload["geometry_version"] = "other"
    mismatch.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="metadata does not match"):
        load_geo_artifacts(mismatch, allow_synthetic=True)

    wrong_versions = registry.set_column(
        registry.schema.get_field_index("geometry_version"),
        "geometry_version",
        pa.array(["wrong"] * registry.num_rows),
    )
    mismatch = _publish(tmp_path / "bad-column-version", wrong_versions, weights)
    with pytest.raises(ValueError, match="geometry_version mismatch"):
        load_geo_artifacts(mismatch, allow_synthetic=True)


def test_persist_geo_artifacts_is_atomic(duck, valid_artifacts, tmp_path):
    with get_connection() as conn:
        metrics = persist_geo_artifacts(valid_artifacts, conn=conn)
        before = conn.execute(
            f"SELECT count(*) FROM {tempo_ops_tbl('region_registry')}"
        ).fetchone()[0]
        manifest_before = conn.execute(
            f"SELECT build_id FROM {tempo_ops_tbl('geography_artifact_manifest')}"
        ).fetchone()[0]
        duplicate_path = tmp_path / "duplicate.parquet"
        duplicate = pa.Table.from_pylist([REGISTRY_ROWS[0], REGISTRY_ROWS[0]])
        pq.write_table(duplicate, duplicate_path)
        invalid = replace(valid_artifacts, registry_path=duplicate_path, region_count=2)
        with pytest.raises(Exception, match="duplicate|constraint|PRIMARY"):
            persist_geo_artifacts(invalid, conn=conn)
        after = conn.execute(
            f"SELECT count(*) FROM {tempo_ops_tbl('region_registry')}"
        ).fetchone()[0]
        manifest_after = conn.execute(
            f"SELECT build_id FROM {tempo_ops_tbl('geography_artifact_manifest')}"
        ).fetchone()[0]
    assert metrics == {"regions_loaded": 9, "weights_loaded": 9}
    assert before == after == 9
    assert manifest_before == manifest_after


def test_manifest_entry_file_checksum_schema_count_and_empty_rejections(
    valid_artifacts, tmp_path
):
    original = json.loads(valid_artifacts.manifest_path.read_text())

    def write(payload):
        valid_artifacts.manifest_path.write_text(json.dumps(payload))

    payload = {**original, "registry": []}
    write(payload)
    with pytest.raises(ValueError, match="entries must be objects"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    payload = json.loads(json.dumps(original))
    payload["weights"].pop("row_count")
    write(payload)
    with pytest.raises(ValueError, match="weights.*incomplete"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    payload = json.loads(json.dumps(original))
    payload["weights"]["path"] = "generations/missing/weights.parquet"
    write(payload)
    with pytest.raises(FileNotFoundError, match="weights artifact not found"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    payload = json.loads(json.dumps(original))
    payload["weights"]["sha256"] = "bad"
    write(payload)
    with pytest.raises(ValueError, match="weights checksum mismatch"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    bad_schema = tmp_path / "bad-schema.parquet"
    pq.write_table(pa.table({"geometry_version": ["test-v1"]}), bad_schema)
    payload = json.loads(json.dumps(original))
    payload["weights"] = {
        "path": bad_schema.relative_to(valid_artifacts.manifest_path.parent).as_posix()
        if bad_schema.is_relative_to(valid_artifacts.manifest_path.parent)
        else valid_artifacts.weights_path.relative_to(
            valid_artifacts.manifest_path.parent
        ).as_posix(),
        "sha256": hashlib.sha256(bad_schema.read_bytes()).hexdigest(),
        "row_count": 1,
    }
    if not bad_schema.is_relative_to(valid_artifacts.manifest_path.parent):
        bad_schema = valid_artifacts.manifest_path.parent / "bad-schema.parquet"
        pq.write_table(pa.table({"geometry_version": ["test-v1"]}), bad_schema)
        payload["weights"]["path"] = "bad-schema.parquet"
        payload["weights"]["sha256"] = hashlib.sha256(
            bad_schema.read_bytes()
        ).hexdigest()
    write(payload)
    with pytest.raises(ValueError, match="weights missing required columns"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    payload = json.loads(json.dumps(original))
    payload["registry"]["row_count"] += 1
    write(payload)
    with pytest.raises(ValueError, match="row count"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)

    empty = valid_artifacts.manifest_path.parent / "empty-weights.parquet"
    schema = pq.read_schema(valid_artifacts.weights_path)
    pq.write_table(pa.Table.from_batches([], schema=schema), empty)
    payload = json.loads(json.dumps(original))
    payload["weights"] = {
        "path": empty.name,
        "sha256": hashlib.sha256(empty.read_bytes()).hexdigest(),
        "row_count": 0,
    }
    write(payload)
    with pytest.raises(ValueError, match="must not be empty"):
        load_geo_artifacts(valid_artifacts.manifest_path, allow_synthetic=True)


def test_populated_warehouse_rejects_geometry_version_change(duck, valid_artifacts):
    with get_connection() as conn:
        persist_geo_artifacts(valid_artifacts, conn=conn)
        conn.execute(
            """
            INSERT INTO tempo_no2_raw.region_hour_aggregates
            VALUES (
                TIMESTAMP '2026-07-12 12:00:00', 'US', 'US', 'country',
                1.0, 1.0, 1.0, 1, 1, 1.0, 1.0, 1.0, true, 1, true,
                nextval('tempo_no2_hour_revision'), 'test-v1', current_timestamp
            )
            """
        )
        changed = replace(valid_artifacts, geometry_version="test-v2")
        with pytest.raises(RuntimeError, match="warehouse rebuild"):
            persist_geo_artifacts(changed, conn=conn)
