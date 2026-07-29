from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb
import pytest

from titanskies_pipeline.plumegraph import validation
from titanskies_pipeline.plumegraph.synthetic import write_synthetic_benchmark
from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)

UTC = timezone.utc


@pytest.fixture
def conn():
    connection = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(connection)
    yield connection
    connection.close()


def _document(path):
    write_synthetic_benchmark(path)
    return json.loads(path.read_text())


def test_benchmark_loader_validates_document_and_rows(tmp_path, conn):
    path = tmp_path / "benchmark.json"
    document = _document(path)
    assert (
        validation.load_benchmark(
            path,
            allow_incomplete=True,
            conn=conn,
        )["windows"]
        == 2
    )
    changed = _document(path)
    changed["windows"][0]["scene_class"] = "changed"
    path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="cannot change"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)

    path.write_text("[]")
    with pytest.raises(ValueError, match="Unsupported"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    document = _document(path)
    document["benchmark_version"] = ""
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="versions are required"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    document = _document(path)
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="200 windows"):
        validation.load_benchmark(path, conn=conn)
    document["windows"][0] = "bad"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="JSON objects"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    document = _document(path)
    document["windows"][1]["window_id"] = document["windows"][0]["window_id"]
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="duplicate windows"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    document = _document(path)
    document["windows"][0]["window_start"] = "2024-01-01T00:00:00"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="timezone"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    document = _document(path)
    document["windows"][0]["window_end"] = document["windows"][0]["window_start"]
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="non-empty"):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)
    _document(path)
    conn.execute("drop table plumegraph_events_raw.benchmark_labels")
    with pytest.raises(duckdb.CatalogException):
        validation.load_benchmark(path, allow_incomplete=True, conn=conn)


def test_production_benchmark_requires_split_and_double_review(tmp_path, conn):
    path = tmp_path / "benchmark.json"
    template = _document(path)["windows"][0]
    windows = []
    for index in range(200):
        item = {
            **template,
            "window_id": f"w{index}",
            "split_name": "calibration",
            "reviewer_count": 2,
        }
        windows.append(item)
    document = {
        "schema_version": "plumegraph-benchmark-v1",
        "benchmark_version": "v1",
        "protocol_version": "p1",
        "windows": windows,
    }
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="120/80"):
        validation.load_benchmark(path, conn=conn)
    for index, item in enumerate(windows):
        item["split_name"] = "calibration" if index < 120 else "held_out"
        item["reviewer_count"] = 1
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="20%"):
        validation.load_benchmark(path, conn=conn)
    for item in windows:
        item["reviewer_count"] = 2
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="facilities must be disjoint"):
        validation.load_benchmark(path, conn=conn)


def test_validation_helpers_and_empty_or_incomplete_guards(monkeypatch, conn):
    real_predictions = validation._benchmark_predictions
    assert validation._softmax_top_probability([1, 1], 1)[0] == 0
    arguments = {"minimum": 0.05, "maximum": 3, "step_size": 0.05}
    assert validation._fit_temperature([], **arguments) == 1
    assert validation._fit_temperature([(["a"], [1], "missing")], **arguments) == 1
    with pytest.raises(ValueError, match="No benchmark"):
        validation.run_validation("missing", allow_incomplete=True, conn=conn)
    monkeypatch.setattr(
        validation,
        "_benchmark_predictions",
        lambda *_args: {
            "one": {
                "split": "held_out",
                "plume_present": True,
                "expected": None,
                "confounding_class": "isolated",
                "episodes": {},
            }
        },
    )
    with pytest.raises(ValueError, match="all 200"):
        validation.run_validation("v1", conn=conn)
    metrics = validation.run_validation("v1", allow_incomplete=True, conn=conn)
    assert metrics.detection_precision is None
    assert metrics.detection_recall == 0
    assert metrics.source_top1_accuracy is None
    assert metrics.expected_calibration_error is None
    assert not metrics.passed

    class Rows:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [
                (
                    "window",
                    "held_out",
                    True,
                    "source",
                    "isolated",
                    "episode",
                    None,
                    None,
                    None,
                )
            ]

    predictions = real_predictions(Rows(), "v1")
    assert predictions["window"]["episodes"] == {}


def test_validation_counts_false_positive_and_wrong_top1(monkeypatch, conn):
    now = datetime(2024, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(
        validation,
        "_benchmark_predictions",
        lambda *_args: {
            "cal": {
                "split": "calibration",
                "plume_present": True,
                "expected": "a",
                "confounding_class": "isolated",
                "episodes": {"r": [("a", 1.0, 1), ("b", 0.0, 2)]},
            },
            "false-positive": {
                "split": "held_out",
                "plume_present": False,
                "expected": None,
                "confounding_class": "isolated",
                "episodes": {"r": [("a", 1.0, 1)]},
            },
            "wrong-source": {
                "split": "held_out",
                "plume_present": True,
                "expected": "b",
                "confounding_class": "isolated",
                "episodes": {"r": [("a", 1.0, 1), ("b", 0.0, 2)]},
            },
            "confounded-wrong-source": {
                "split": "held_out",
                "plume_present": True,
                "expected": "b",
                "confounding_class": "confounded",
                "episodes": {"r": [("a", 1.0, 1), ("b", 0.0, 2)]},
            },
        },
    )
    metrics = validation.run_validation("v2", allow_incomplete=True, conn=conn)
    assert metrics.detection_precision == pytest.approx(2 / 3)
    assert metrics.detection_recall == 1
    assert metrics.source_top1_accuracy == 0
    assert metrics.expected_calibration_error is not None
    assert not metrics.passed
    assert validation._db_time(now).tzinfo is None
    with pytest.raises(ValueError, match="timezone"):
        validation._db_time(now.replace(tzinfo=None))
