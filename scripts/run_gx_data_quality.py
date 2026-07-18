#!/usr/bin/env python3
"""Run local Great Expectations-style checks against the dbt build database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import great_expectations as gx

PUBLIC_RELATIONS: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...], int], ...] = (
    (
        "tempo_no2_marts",
        "tempo_no2_region_hourly",
        ("canonical_region_id", "observation_hour"),
        ("canonical_region_id", "observation_hour", "no2_mean", "is_analysis_ready"),
        0,
    ),
    (
        "tempo_no2_marts",
        "tempo_no2_region_latest",
        ("canonical_region_id",),
        ("canonical_region_id", "latest_observation_hour", "data_age_hours"),
        0,
    ),
    (
        "tempo_no2_marts",
        "tempo_no2_country_hourly",
        ("country_code", "observation_hour"),
        ("country_code", "observation_hour", "no2_mean"),
        0,
    ),
    (
        "tempo_no2_observability",
        "tempo_no2_data_quality",
        ("canonical_region_id", "observation_hour", "issue_type"),
        ("canonical_region_id", "observation_hour", "issue_type", "severity"),
        0,
    ),
    (
        "tempo_no2_std_marts",
        "tempo_no2_std_region_hourly",
        ("canonical_region_id", "observation_hour"),
        ("canonical_region_id", "observation_hour", "no2_mean", "is_analysis_ready"),
        0,
    ),
    (
        "tempo_no2_std_marts",
        "tempo_no2_std_region_latest",
        ("canonical_region_id",),
        ("canonical_region_id", "latest_observation_hour", "data_age_hours"),
        0,
    ),
    (
        "tempo_no2_std_marts",
        "tempo_no2_std_country_hourly",
        ("country_code", "observation_hour"),
        ("country_code", "observation_hour", "no2_mean"),
        0,
    ),
    (
        "tempo_no2_std_observability",
        "tempo_no2_std_data_quality",
        ("canonical_region_id", "observation_hour", "issue_type"),
        ("canonical_region_id", "observation_hour", "issue_type", "severity"),
        0,
    ),
)


def _relation_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [schema, table],
        ).fetchone()[0]
    )


def _columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> set[str]:
    rows = conn.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = ? and table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _duplicate_count(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    grain: tuple[str, ...],
) -> int:
    cols = ", ".join(f'"{col}"' for col in grain)
    return int(
        conn.execute(
            f"""
            select count(*)
            from (
                select {cols}
                from "{schema}"."{table}"
                group by {cols}
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    )


def _coverage_out_of_range(
    conn: duckdb.DuckDBPyConnection, schema: str, table: str, column: str
) -> int:
    return int(
        conn.execute(
            f"""
            select count(*)
            from "{schema}"."{table}"
            where "{column}" < 0 or "{column}" > 1
            """
        ).fetchone()[0]
    )


def _dq_error_count(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(
        conn.execute(
            f"""
            select count(*)
            from "{schema}"."{table}"
            where severity = 'error'
            """
        ).fetchone()[0]
    )


def run_checks(duckdb_path: Path) -> dict[str, Any]:
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    results: list[dict[str, Any]] = []
    failed = 0
    try:
        for schema, table, grain, required_columns, max_duplicates in PUBLIC_RELATIONS:
            exists = _relation_exists(conn, schema, table)
            result = {
                "schema": schema,
                "table": table,
                "exists": exists,
                "passed": exists,
            }
            if not exists:
                failed += 1
                results.append(result)
                continue

            columns = _columns(conn, schema, table)
            missing = [col for col in required_columns if col not in columns]
            duplicates = _duplicate_count(conn, schema, table, grain)
            coverage_issues = 0
            if "coverage_fraction" in columns:
                coverage_issues = _coverage_out_of_range(
                    conn, schema, table, "coverage_fraction"
                )
            dq_errors = 0
            if table.endswith("data_quality"):
                dq_errors = _dq_error_count(conn, schema, table)

            passed = (
                not missing
                and duplicates <= max_duplicates
                and coverage_issues == 0
                and dq_errors == 0
            )
            if not passed:
                failed += 1
            result.update(
                {
                    "missing_columns": missing,
                    "duplicate_grain_rows": duplicates,
                    "coverage_out_of_range_rows": coverage_issues,
                    "dq_error_rows": dq_errors,
                    "passed": passed,
                }
            )
            results.append(result)
    finally:
        conn.close()

    return {
        "great_expectations_version": gx.__version__,
        "duckdb_path": str(duckdb_path),
        "failed_checks": failed,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--report-path", default=".cache/gx_data_quality_report.json")
    args = parser.parse_args()
    report = run_checks(Path(args.duckdb_path))
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))
    if report["failed_checks"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
