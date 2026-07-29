from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from titanskies_pipeline.plumegraph import release
from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)


def test_arrow_relation_and_primary_key_guards(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("create schema marts; create table marts.rows (id int)")
    conn.execute("insert into marts.rows values (1)")
    assert release._relation_exists(conn, "marts.rows")
    assert not release._relation_exists(conn, "marts.missing")
    assert release._arrow_table(conn, "marts.rows").num_rows == 1
    with pytest.raises(RuntimeError, match="Missing PlumeGraph"):
        release._arrow_table(conn, "marts.missing")
    empty = pa.table({"id": pa.array([], type=pa.int64())})
    release._assert_unique(empty, ["id"], "empty")
    release._assert_unique(pa.table({"id": [1, 2]}), [], "none")
    with pytest.raises(ValueError, match="missing columns"):
        release._assert_unique(pa.table({"id": [1]}), ["missing"], "rows")
    with pytest.raises(ValueError, match="duplicate"):
        release._assert_unique(pa.table({"id": [1, 1]}), ["id"], "rows")
    conn.close()


def test_parquet_and_geoparquet_validation(tmp_path, monkeypatch):
    table = pa.table({"id": [1], "geometry_wkb": [b"wkb"]})
    plain = tmp_path / "plain.parquet"
    release._write_parquet(table, plain)
    assert pq.read_table(plain).num_rows == 1
    geo = tmp_path / "geo.parquet"
    release._write_parquet(table, geo, geometry_column="geometry_wkb")
    assert b"geo" in pq.read_table(geo).schema.metadata
    with pytest.raises(ValueError, match="Missing release geometry"):
        release._write_parquet(table, tmp_path / "bad.parquet", geometry_column="bad")

    original = release.pq.read_table
    monkeypatch.setattr(
        release.pq,
        "read_table",
        lambda _path: pa.table({"different": [1]}),
    )
    with pytest.raises(ValueError, match="Failed to verify"):
        release._write_parquet(table, tmp_path / "mismatch.parquet")
    monkeypatch.setattr(
        release.pq,
        "read_table",
        lambda _path: table.replace_schema_metadata({}),
    )
    with pytest.raises(ValueError, match="GeoParquet metadata"):
        release._write_parquet(
            table,
            tmp_path / "missing-geo.parquet",
            geometry_column="geometry_wkb",
        )
    monkeypatch.setattr(release.pq, "read_table", original)


def test_release_preconditions_and_version_fallback(tmp_path, monkeypatch):
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    for value in ("", "../escape", "bad/name"):
        with pytest.raises(ValueError, match="safe"):
            release.build_release(
                release_version=value,
                output_dir=tmp_path,
                conn=conn,
            )
    with pytest.raises(RuntimeError, match="passing held-out"):
        release.build_release(
            release_version="v1",
            output_dir=tmp_path,
            conn=conn,
        )
    with pytest.raises(RuntimeError, match="No current"):
        release.build_release(
            release_version="v1",
            output_dir=tmp_path,
            require_passed_validation=False,
            conn=conn,
        )
    conn.execute(
        """
        insert into plumegraph_events_ops.current_generations
        values ('region', date '2024-01-01', 'run', current_timestamp);
        insert into plumegraph_events_ops.validation_runs
        values (
            'validation', 'stale', 'benchmark', 'held_out',
            1, 1, 1, 0, true, true, '{}', current_timestamp
        );
        """
    )
    with pytest.raises(RuntimeError, match="validation is stale"):
        release.build_release(
            release_version="v1",
            output_dir=tmp_path,
            validation_run_id="validation",
            conn=conn,
        )
    monkeypatch.setattr(
        release.importlib.metadata,
        "version",
        lambda _name: (_ for _ in ()).throw(
            release.importlib.metadata.PackageNotFoundError
        ),
    )
    assert release._package_version() == "0.6.0"
    conn.close()


def test_release_verification_rejects_format_escape_and_corruption(tmp_path):
    path = tmp_path / "release"
    path.mkdir()
    manifest_path = path / "manifest.json"
    manifest_path.write_text(json.dumps({"evidence_format": "bad"}))
    with pytest.raises(ValueError, match="Unsupported"):
        release.verify_release(path)
    manifest_path.write_text(
        json.dumps(
            {
                "evidence_format": "plumegraph-evidence-v1",
                "files": [{"path": "../escape", "sha256": "x", "bytes": 1}],
            }
        )
    )
    with pytest.raises(ValueError, match="path escape"):
        release.verify_release(path)
    manifest_path.write_text(
        json.dumps(
            {
                "evidence_format": "plumegraph-evidence-v1",
                "files": [{"path": "missing", "sha256": "x", "bytes": 1}],
            }
        )
    )
    with pytest.raises(ValueError, match="artifact failed"):
        release.verify_release(path)
    artifact = path / "artifact"
    artifact.write_text("body")
    manifest_path.write_text(
        json.dumps(
            {
                "evidence_format": "plumegraph-evidence-v1",
                "files": [
                    {
                        "path": "artifact",
                        "sha256": "wrong",
                        "bytes": artifact.stat().st_size,
                    }
                ],
            }
        )
    )
    with pytest.raises(ValueError, match="artifact failed"):
        release.verify_release(path)
