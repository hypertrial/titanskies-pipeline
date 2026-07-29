"""Expert-benchmark ingestion and release-gate metrics for PlumeGraph."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from titanskies_pipeline.config.settings_plumegraph import get_plumegraph_settings
from titanskies_pipeline.plumegraph.identity import (
    analysis_generation_manifest_identity,
    canonical_json,
    sha256_identity,
)
from titanskies_pipeline.plumegraph.science import expected_calibration_error
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    plumegraph_ops_tbl,
    plumegraph_raw_tbl,
)


@dataclass(frozen=True)
class ValidationMetrics:
    validation_run_id: str
    benchmark_windows: int
    detection_precision: float | None
    detection_recall: float | None
    source_top1_accuracy: float | None
    expected_calibration_error: float | None
    temperature: float | None
    probability_enabled: bool
    passed: bool


def _db_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Benchmark timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_benchmark(
    path: Path,
    *,
    allow_incomplete: bool = False,
    conn=None,
) -> dict[str, object]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    body = path.read_bytes()
    document = json.loads(body)
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != "plumegraph-benchmark-v1"
        or not isinstance(document.get("windows"), list)
    ):
        raise ValueError("Unsupported PlumeGraph benchmark document")
    benchmark_version = str(document.get("benchmark_version") or "")
    protocol_version = str(document.get("protocol_version") or "")
    windows = document["windows"]
    if not benchmark_version or not protocol_version:
        raise ValueError("Benchmark and protocol versions are required")
    if not allow_incomplete and len(windows) != 200:
        raise ValueError("Production PlumeGraph benchmark must contain 200 windows")
    seen: set[str] = set()
    split_counts = {"calibration": 0, "held_out": 0}
    double_reviewed = 0
    rows: list[list[object]] = []
    for item in windows:
        if not isinstance(item, dict):
            raise ValueError("Benchmark windows must be JSON objects")
        window_id = str(item["window_id"])
        split_name = str(item["split_name"])
        reviewer_count = int(item["reviewer_count"])
        if window_id in seen or split_name not in split_counts:
            raise ValueError("Benchmark contains duplicate windows or invalid splits")
        start = _parse_utc(item["window_start"])
        end = _parse_utc(item["window_end"])
        if start >= end:
            raise ValueError("Benchmark windows must be non-empty")
        seen.add(window_id)
        split_counts[split_name] += 1
        double_reviewed += int(reviewer_count >= 2)
        rows.append(
            [
                benchmark_version,
                window_id,
                str(item["facility_id"]),
                _db_time(start),
                _db_time(end),
                split_name,
                bool(item["plume_present"]),
                item.get("expected_source_facility_id"),
                str(item["scene_class"]),
                str(item["season"]),
                str(item["region_label"]),
                str(item["operation_class"]),
                str(item["confounding_class"]),
                reviewer_count,
                bool(item.get("adjudicated", False)),
                protocol_version,
                canonical_json(item.get("provenance", {})),
            ]
        )
    if not allow_incomplete and split_counts != {"calibration": 120, "held_out": 80}:
        raise ValueError("Benchmark must contain a 120/80 calibration/held-out split")
    if not allow_incomplete and double_reviewed < 40:
        raise ValueError("At least 20% of benchmark windows require double review")
    facilities_by_split = {
        split: {str(row[2]) for row in rows if row[5] == split}
        for split in split_counts
    }
    if facilities_by_split["calibration"] & facilities_by_split["held_out"]:
        raise ValueError(
            "Benchmark calibration and held-out facilities must be disjoint"
        )
    with _use_conn(conn) as connection:
        connection.begin()
        try:
            manifest_sha256 = hashlib.sha256(body).hexdigest()
            previous = connection.execute(
                f"""
                SELECT manifest_sha256
                FROM {plumegraph_ops_tbl("benchmark_manifests")}
                WHERE benchmark_version = ?
                """,
                [benchmark_version],
            ).fetchone()
            if previous and str(previous[0]) != manifest_sha256:
                raise ValueError("A frozen PlumeGraph benchmark version cannot change")
            connection.execute(
                f"""
                INSERT OR IGNORE INTO {plumegraph_ops_tbl("benchmark_manifests")}
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    benchmark_version,
                    manifest_sha256,
                    protocol_version,
                    len(rows),
                    _db_time(datetime.now(timezone.utc)),
                ],
            )
            for row in rows:
                connection.execute(
                    f"""
                    INSERT OR IGNORE INTO
                    {plumegraph_raw_tbl("benchmark_labels")}
                    VALUES ({", ".join("?" for _ in row)})
                    """,
                    row,
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return {
        "benchmark_version": benchmark_version,
        "windows": len(rows),
        "calibration_windows": split_counts["calibration"],
        "held_out_windows": split_counts["held_out"],
        "double_reviewed_windows": double_reviewed,
    }


def _softmax_top_probability(
    scores: Sequence[float],
    temperature: float,
) -> tuple[int, float]:
    scaled = [score / temperature for score in scores]
    maximum = max(scaled)
    exponentials = [math.exp(score - maximum) for score in scaled]
    total = sum(exponentials)
    probabilities = [value / total for value in exponentials]
    top = max(range(len(scores)), key=lambda index: (scores[index], -index))
    return top, probabilities[top]


def _fit_temperature(
    candidate_sets: Sequence[tuple[list[str], list[float], str]],
    *,
    minimum: float,
    maximum: float,
    step_size: float,
) -> float:
    if not candidate_sets:
        return 1.0
    best_temperature = 1.0
    best_loss = float("inf")
    steps = round((maximum - minimum) / step_size)
    for step in range(steps + 1):
        temperature = minimum + step * step_size
        loss = 0.0
        observations = 0
        for facility_ids, scores, expected in candidate_sets:
            if expected not in facility_ids:
                continue
            scaled = [score / temperature for score in scores]
            scaled_maximum = max(scaled)
            log_total = scaled_maximum + math.log(
                sum(math.exp(score - scaled_maximum) for score in scaled)
            )
            loss += log_total - scaled[facility_ids.index(expected)]
            observations += 1
        if observations and loss / observations < best_loss:
            best_loss = loss / observations
            best_temperature = temperature
    return best_temperature


def _benchmark_predictions(connection, benchmark_version: str):
    rows = connection.execute(
        f"""
        WITH current_episodes AS (
            SELECT episodes.*
            FROM {plumegraph_raw_tbl("episode_revisions")} AS episodes
            INNER JOIN {plumegraph_ops_tbl("current_generations")} AS generations
                ON episodes.analysis_run_id = generations.analysis_run_id
            QUALIFY row_number() OVER (
                PARTITION BY episodes.plume_id
                ORDER BY
                    generations.partition_date DESC,
                    episodes.created_at DESC,
                    episodes.episode_revision_id DESC
            ) = 1
        )
        SELECT
            labels.window_id,
            labels.split_name,
            labels.plume_present,
            labels.expected_source_facility_id,
            labels.confounding_class,
            episodes.episode_revision_id,
            candidates.facility_id,
            candidates.attribution_score,
            candidates.rank
        FROM {plumegraph_raw_tbl("benchmark_labels")} AS labels
        LEFT JOIN {plumegraph_raw_tbl("episode_target_links")} AS targets
            ON labels.facility_id = targets.facility_id
        LEFT JOIN current_episodes AS episodes
            ON targets.episode_revision_id = episodes.episode_revision_id
           AND episodes.start_time < labels.window_end
           AND episodes.end_time >= labels.window_start
        LEFT JOIN {plumegraph_raw_tbl("candidate_source_revisions")} AS candidates
            ON episodes.episode_revision_id = candidates.episode_revision_id
        WHERE labels.benchmark_version = ?
        ORDER BY labels.window_id, episodes.episode_revision_id, candidates.rank
        """,
        [benchmark_version],
    ).fetchall()
    windows: dict[str, dict[str, object]] = {}
    for row in rows:
        window = windows.setdefault(
            str(row[0]),
            {
                "split": str(row[1]),
                "plume_present": bool(row[2]),
                "expected": None if row[3] is None else str(row[3]),
                "confounding_class": str(row[4]),
                "episodes": {},
            },
        )
        if row[5] is not None and row[6] is not None:
            episode = window["episodes"].setdefault(str(row[5]), [])
            episode.append((str(row[6]), float(row[7]), int(row[8])))
    return windows


def run_validation(
    benchmark_version: str,
    *,
    allow_incomplete: bool = False,
    conn=None,
) -> ValidationMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    contract = get_plumegraph_settings().contract
    with _use_conn(conn) as connection:
        windows = _benchmark_predictions(connection, benchmark_version)
        if not windows:
            raise ValueError(f"No benchmark windows found for {benchmark_version}")
        if not allow_incomplete and len(windows) != 200:
            raise ValueError("Production validation requires all 200 benchmark windows")
        calibration_sets: list[tuple[list[str], list[float], str]] = []
        for window in windows.values():
            if window["split"] != "calibration" or not window["expected"]:
                continue
            for candidates in window["episodes"].values():
                calibration_sets.append(
                    (
                        [item[0] for item in candidates],
                        [item[1] for item in candidates],
                        str(window["expected"]),
                    )
                )
        temperature = _fit_temperature(
            calibration_sets,
            minimum=float(contract["temperature_search_min"]),
            maximum=float(contract["temperature_search_max"]),
            step_size=float(contract["temperature_search_step"]),
        )
        true_positive = false_positive = false_negative = 0
        top1_results: list[bool] = []
        top_probabilities: list[float] = []
        top_outcomes: list[bool] = []
        for window in windows.values():
            if window["split"] != "held_out":
                continue
            detected = bool(window["episodes"])
            present = bool(window["plume_present"])
            true_positive += int(detected and present)
            false_positive += int(detected and not present)
            false_negative += int(not detected and present)
            expected = window["expected"]
            if expected and window["confounding_class"] == "isolated":
                for candidates in window["episodes"].values():
                    facility_ids = [item[0] for item in candidates]
                    scores = [item[1] for item in candidates]
                    top_index, probability = _softmax_top_probability(
                        scores,
                        temperature,
                    )
                    outcome = facility_ids[top_index] == expected
                    top1_results.append(outcome)
                    top_probabilities.append(probability)
                    top_outcomes.append(outcome)
        precision_denominator = true_positive + false_positive
        recall_denominator = true_positive + false_negative
        precision = (
            true_positive / precision_denominator if precision_denominator else None
        )
        recall = true_positive / recall_denominator if recall_denominator else None
        top1 = sum(top1_results) / len(top1_results) if top1_results else None
        ece = (
            expected_calibration_error(
                top_probabilities,
                top_outcomes,
                bins=int(contract["calibration_bins"]),
            )
            if top_probabilities
            else None
        )
        probability_enabled = ece is not None and ece <= float(
            contract["calibration_ece_max"]
        )
        passed = (
            precision is not None
            and precision >= float(contract["detection_precision_min"])
            and recall is not None
            and recall >= float(contract["detection_recall_min"])
            and top1 is not None
            and top1 >= float(contract["source_top1_accuracy_min"])
        )
        generation_rows = connection.execute(
            f"""
                SELECT analysis_region_id, partition_date, analysis_run_id
                FROM {plumegraph_ops_tbl("current_generations")}
                ORDER BY analysis_region_id, partition_date
                """
        ).fetchall()
        analysis_manifest_sha = analysis_generation_manifest_identity(generation_rows)
        metrics_json = canonical_json(
            {
                "temperature": temperature,
                "benchmark_windows": len(windows),
                "true_positive": true_positive,
                "false_positive": false_positive,
                "false_negative": false_negative,
                "emission_bias": None,
                "emission_absolute_error": None,
                "interval_coverage": None,
            }
        )
        run_id = sha256_identity(
            benchmark_version,
            analysis_manifest_sha,
            metrics_json,
            precision,
            recall,
            top1,
            ece,
        )
        connection.execute(
            f"""
            INSERT OR IGNORE INTO {plumegraph_ops_tbl("validation_runs")}
            VALUES (?, ?, ?, 'held_out', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                analysis_manifest_sha,
                benchmark_version,
                precision,
                recall,
                top1,
                ece,
                probability_enabled,
                passed,
                metrics_json,
                _db_time(datetime.now(timezone.utc)),
            ],
        )
    return ValidationMetrics(
        validation_run_id=run_id,
        benchmark_windows=len(windows),
        detection_precision=precision,
        detection_recall=recall,
        source_top1_accuracy=top1,
        expected_calibration_error=ece,
        temperature=temperature,
        probability_enabled=probability_enabled,
        passed=passed,
    )


__all__ = ["ValidationMetrics", "load_benchmark", "run_validation"]
