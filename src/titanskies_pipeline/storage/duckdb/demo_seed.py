"""Deterministic credential-free sample warehouse used by demos and dbt CI."""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb

from titanskies_pipeline.storage.duckdb.schemas.constants import (
    tempo_ops_tbl,
    tempo_raw_tbl,
)
from titanskies_pipeline.storage.duckdb.schemas.tempo import (
    create_all_tempo_test_tables,
    seed_test_tempo_pipeline_run_event,
)

SAMPLE_NOW = datetime(2026, 7, 12, 20)

REGIONS = (
    ("US", "country", "US", "US", "United States", None, "America/Chicago"),
    ("US", "state", "06", "US-CA", "California", "US", "America/Los_Angeles"),
    (
        "US",
        "county",
        "06037",
        "US-CA-037",
        "Los Angeles County",
        "US-CA",
        "America/Los_Angeles",
    ),
    ("CA", "country", "CA", "CA", "Canada", None, "America/Winnipeg"),
    ("CA", "province", "35", "CA-ON", "Ontario", "CA", "America/Toronto"),
    (
        "CA",
        "census_subdivision",
        "3520005",
        "CA-ON-3520005",
        "Toronto",
        "CA-ON",
        "America/Toronto",
    ),
    ("MX", "country", "MX", "MX", "Mexico", None, "America/Mexico_City"),
    (
        "MX",
        "state",
        "09",
        "MX-CMX",
        "Ciudad de Mexico",
        "MX",
        "America/Mexico_City",
    ),
    (
        "MX",
        "municipality",
        "09002",
        "MX-CMX-002",
        "Azcapotzalco",
        "MX-CMX",
        "America/Mexico_City",
    ),
)


def seed_demo_warehouse(
    conn: duckdb.DuckDBPyConnection,
    *,
    include_quality_issues: bool = True,
    anchor_time: datetime = SAMPLE_NOW,
) -> None:
    create_all_tempo_test_tables(conn)
    seed_test_tempo_pipeline_run_event(conn)
    registry_rows = [
        (*row, "sample-v03", "sample-checksum", anchor_time) for row in REGIONS
    ]
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {tempo_ops_tbl("region_registry")}
        (country_code, region_type, source_region_id, canonical_region_id,
         region_name, parent_region_id, timezone, geometry_version,
         geometry_checksum, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        registry_rows,
    )

    for day in range(8):
        observation = anchor_time - timedelta(days=7 - day)
        revision = conn.execute("SELECT nextval('tempo_no2_hour_revision')").fetchone()[
            0
        ]
        granule_id = f"sample-{observation:%Y%m%dT%H}"
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {tempo_ops_tbl("granule_inventory")}
            (granule_id, concept_id, acquisition_start, acquisition_end,
             cmr_revision_at, last_seen_at, download_url, local_path,
             checksum_sha256, file_size_bytes, observation_time, observation_hour,
             discovery_status, download_status, validation_status, processing_status,
             discovered_at, downloaded_at, validated_at, processed_at, error_message,
             updated_at)
            VALUES (?, 'SAMPLE', ?, ?, ?, ?, NULL, NULL, 'sample', 1024, ?, ?,
                    'discovered', 'downloaded', 'validated', 'processed', ?, ?, ?, ?,
                    NULL, ?)
            """,
            [granule_id, *([observation] * 11)],
        )
        source_granule_count = 2 if day == 7 else 1
        if source_granule_count == 2:
            sibling_id = f"{granule_id}-sibling"
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {tempo_ops_tbl("granule_inventory")}
                (granule_id, concept_id, acquisition_start, acquisition_end,
                 cmr_revision_at, last_seen_at, download_url, local_path,
                 checksum_sha256, file_size_bytes, observation_time, observation_hour,
                 discovery_status, download_status, validation_status, processing_status,
                 discovered_at, downloaded_at, validated_at, processed_at, error_message,
                 updated_at)
                VALUES (?, 'SAMPLE', ?, ?, ?, ?, NULL, NULL, 'sample', 1024, ?, ?,
                        'discovered', 'downloaded', 'validated', 'processed', ?, ?, ?, ?,
                        NULL, ?)
                """,
                [sibling_id, *([observation] * 11)],
            )
        aggregate_rows = []
        for index, region in enumerate(REGIONS):
            country, region_type, _source_id, region_id, *_rest = region
            total_area = 100.0 + index
            coverage = 0.82
            accepted = True
            if include_quality_issues and region_id == "CA-ON-3520005" and day == 2:
                coverage = 0.10
            if include_quality_issues and region_id == "MX-CMX-002" and day == 4:
                coverage = 0.0
                accepted = False
            valid_area = total_area * coverage
            mean = None if not accepted else 1.0e15 + index * 1.0e14 + day * 5.0e13
            aggregate_rows.append(
                (
                    observation,
                    region_id,
                    country,
                    region_type,
                    mean,
                    None if mean is None else mean * 0.96,
                    None if mean is None else mean * 1.18,
                    int(round(20 * coverage)),
                    20,
                    valid_area,
                    total_area,
                    coverage,
                    accepted,
                    source_granule_count,
                    True,
                    revision,
                    "sample-v03",
                    observation + timedelta(minutes=5),
                )
            )
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {tempo_raw_tbl("region_hour_aggregates")}
            (observation_hour, canonical_region_id, country_code,
             region_type, no2_mean, no2_median, no2_p90, valid_pixel_count,
             total_pixel_count, valid_area_km2, total_area_km2, coverage_fraction,
             quality_flag_accepted, source_granule_count, all_granules_validated,
             revision, geometry_version, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            aggregate_rows,
        )

    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {tempo_raw_tbl("grid_latest")}
        (grid_row, grid_col, latitude, longitude, cell_area_km2, observation_time,
         observation_hour, no2, quality_flag, quality_flag_accepted, granule_id,
         ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                2400,
                2499,
                62.01,
                -118.01,
                2.32,
                anchor_time,
                anchor_time,
                1.4e15,
                0,
                True,
                "sample-grid",
                anchor_time,
            ),
            (
                1050,
                3100,
                35.01,
                -105.99,
                4.05,
                anchor_time,
                anchor_time,
                1.8e15,
                1,
                True,
                "sample-grid",
                anchor_time,
            ),
            (
                250,
                5000,
                19.01,
                -67.99,
                4.69,
                anchor_time,
                anchor_time,
                None,
                2,
                False,
                "sample-grid",
                anchor_time,
            ),
        ],
    )


__all__ = ["REGIONS", "SAMPLE_NOW", "seed_demo_warehouse"]
