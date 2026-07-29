"""Credential-free, invented PlumeGraph fixture generation."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from titanskies_pipeline.plumegraph.identity import sha256_identity
from titanskies_pipeline.plumegraph.sources import (
    SourceIngestMetrics,
    normalize_camd_hour,
    normalize_tempo_pixel,
    pending_source_requests,
    persist_cohort,
    persist_normalized_records,
    plan_source_requests,
    write_source_snapshot,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import plumegraph_raw_tbl

SYNTHETIC_OBSERVATION_TIME = datetime(2024, 7, 15, 18, 30, tzinfo=timezone.utc)


def write_synthetic_cohort(path: Path) -> Path:
    facilities = []
    for index in range(75):
        row = index // 15
        column = index % 15
        facilities.append(
            {
                "facility_id": f"PG{index + 1:04d}",
                "facility_name": f"Example Test Plant {index + 1:02d}",
                "latitude": 36.0 + row * 0.002,
                "longitude": -96.0 + column * 0.002,
                "timezone": "America/Chicago",
                "utc_standard_offset_minutes": -360,
                "annual_nox_tons": float(1000 - index * 5),
                "is_cohort": True,
                "review_status": "synthetic",
                "inclusion_reason": (
                    "Invented example.test facility for credential-free validation"
                ),
            }
        )
    facilities.append(
        {
            "facility_id": "PGALT0001",
            "facility_name": "Example Test Alternative Source",
            "latitude": 36.01,
            "longitude": -95.92,
            "timezone": "America/Chicago",
            "utc_standard_offset_minutes": -360,
            "annual_nox_tons": 850.0,
            "is_cohort": False,
            "review_status": "synthetic",
            "inclusion_reason": (
                "Invented nearby alternative CAMD source for source-confusion tests"
            ),
        }
    )
    document = {
        "schema_version": "plumegraph-cohort-v1",
        "cohort_version": "synthetic-2024-v1",
        "review_status": "synthetic",
        "approved_by": None,
        "source_url": "https://example.test/plumegraph/cohort",
        "facilities": facilities,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return path


def _gps_seconds(value: datetime) -> float:
    gps_epoch = datetime(1980, 1, 6, tzinfo=timezone.utc)
    return (value - gps_epoch).total_seconds() + 18


def _geometry_helpers():
    from shapely.geometry import box
    from shapely.wkb import dumps

    return box, dumps


def _synthetic_pixels(observation_time: datetime) -> list[dict[str, object]]:
    box, dumps = _geometry_helpers()
    records: list[dict[str, object]] = []
    for index in range(40):
        latitude = 35.78 + (index % 10) * 0.045
        longitude = -96.62 - (index // 10) * 0.035
        records.append(
            {
                "mirror_step": index,
                "xtrack": 0,
                "time_gps_seconds": _gps_seconds(observation_time),
                "latitude": latitude,
                "longitude": longitude,
                "geometry_wkb": dumps(
                    box(
                        longitude - 0.01,
                        latitude - 0.01,
                        longitude + 0.01,
                        latitude + 0.01,
                    ),
                    hex=False,
                    big_endian=False,
                ),
                "pixel_area_km2": 5.0,
                "no2_vertical_column": 1e15 + (index % 5 - 2) * 1e13,
                "no2_uncertainty": 1e14,
                "quality_flag": 0,
                "cloud_fraction": 0.02,
                "snow_ice_fraction": 0.0,
                "amf_diagnostic_flag": 0,
                "solar_zenith_angle": 30.0,
                "viewing_zenith_angle": 10.0,
                "surface_pressure_hpa": 980.0,
                "collection_version": "V04",
                "no2_unit": "molecules/cm2",
            }
        )
    for index, (row, column, longitude, latitude) in enumerate(
        (
            (100, 100, -95.90, 36.00),
            (100, 101, -95.87, 36.00),
            (101, 100, -95.90, 36.03),
            (101, 101, -95.87, 36.03),
        )
    ):
        records.append(
            {
                "mirror_step": row,
                "xtrack": column,
                "time_gps_seconds": _gps_seconds(observation_time),
                "latitude": latitude,
                "longitude": longitude,
                "geometry_wkb": dumps(
                    box(
                        longitude - 0.02,
                        latitude - 0.02,
                        longitude + 0.02,
                        latitude + 0.02,
                    ),
                    hex=False,
                    big_endian=False,
                ),
                "pixel_area_km2": 16.0,
                "no2_vertical_column": 5e15 + index * 1e14,
                "no2_uncertainty": 1e14,
                "quality_flag": 0,
                "cloud_fraction": 0.02,
                "snow_ice_fraction": 0.0,
                "amf_diagnostic_flag": 0,
                "solar_zenith_angle": 30.0,
                "viewing_zenith_angle": 10.0,
                "surface_pressure_hpa": 980.0,
                "collection_version": "V04",
                "no2_unit": "molecules/cm2",
            }
        )
    records.append(
        {
            **records[-1],
            "mirror_step": 120,
            "xtrack": 120,
            "longitude": -95.7,
            "quality_flag": 1,
            "cloud_fraction": 0.5,
        }
    )
    return records


def seed_synthetic_sources(
    *,
    cohort_path: Path,
    raw_data_dir: Path,
    conn=None,
) -> dict[str, object]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    cohort_metrics = persist_cohort(
        cohort_path,
        require_approved=False,
        conn=conn,
    )
    start = SYNTHETIC_OBSERVATION_TIME.replace(minute=0)
    end = start + timedelta(days=1)
    discovery = plan_source_requests(
        window_start=start,
        window_end=end,
        conn=conn,
    )
    with _use_conn(conn) as connection:
        region_id = str(
            connection.execute(
                f"""
                SELECT analysis_region_id
                FROM {plumegraph_raw_tbl("analysis_regions")}
                ORDER BY analysis_region_id
                LIMIT 1
                """
            ).fetchone()[0]
        )
        requests = {
            request.connector: request
            for request in pending_source_requests(conn=connection)
            if request.analysis_region_id == region_id
        }
        pixels_raw = _synthetic_pixels(SYNTHETIC_OBSERVATION_TIME) + _synthetic_pixels(
            SYNTHETIC_OBSERVATION_TIME + timedelta(minutes=15)
        )
        harmony_body = (
            json.dumps(
                [
                    {
                        **record,
                        "geometry_wkb": bytes(record["geometry_wkb"]).hex(),
                    }
                    for record in pixels_raw
                ],
                sort_keys=True,
            )
            + "\n"
        ).encode()
        harmony_snapshot = write_source_snapshot(
            harmony_body,
            request=requests["harmony"],
            source_identity="example.test/TEMPO_NO2_L2_SYNTHETIC",
            extension="json",
            schema_fields=sorted(pixels_raw[0]),
            row_count=len(pixels_raw),
            source_revision_at=datetime(2024, 7, 16, tzinfo=timezone.utc),
            source_etag="example-tempo-etag",
            collected_at=datetime(2024, 7, 17, tzinfo=timezone.utc),
            raw_data_dir=raw_data_dir,
            register=False,
            conn=connection,
        )
        pixels = [
            normalize_tempo_pixel(
                record,
                analysis_region_id=region_id,
                granule_id="example.test/TEMPO_SYNTHETIC_20240715",
                upstream_revision="R1",
                snapshot=harmony_snapshot,
            )
            for record in pixels_raw
        ]
        harmony_ingest = persist_normalized_records(
            pixels=pixels,
            snapshot=harmony_snapshot,
            successful_request=requests["harmony"],
            raw_data_dir=raw_data_dir,
            conn=connection,
        )
        met_rows_raw = [
            {
                "valid_time": start,
                "wind_u_10m": 4.0,
                "wind_v_10m": 0.2,
                "wind_u_80m": 6.0,
                "wind_v_80m": 0.3,
                "pbl_height_m": 900.0,
                "surface_pressure_hpa": 980.0,
                "temperature_2m_k": 298.0,
            },
            {
                "valid_time": start + timedelta(hours=1),
                "wind_u_10m": 5.0,
                "wind_v_10m": 0.2,
                "wind_u_80m": 7.0,
                "wind_v_80m": 0.3,
                "pbl_height_m": 1000.0,
                "surface_pressure_hpa": 979.0,
                "temperature_2m_k": 299.0,
            },
        ]
        hrrr_body = (
            json.dumps(met_rows_raw, default=str, sort_keys=True) + "\n"
        ).encode()
        hrrr_snapshot = write_source_snapshot(
            hrrr_body,
            request=requests["hrrr"],
            source_identity="s3://example.test/hrrrzarr/synthetic",
            extension="json",
            schema_fields=sorted(met_rows_raw[0]),
            row_count=len(met_rows_raw),
            source_revision_at=start,
            source_etag="example-hrrr-etag",
            collected_at=datetime(2024, 7, 17, tzinfo=timezone.utc),
            raw_data_dir=raw_data_dir,
            register=False,
            conn=connection,
        )
        meteorology = [
            {
                "meteorology_revision_id": sha256_identity(
                    region_id,
                    row["valid_time"],
                    hrrr_snapshot.content_sha256,
                ),
                "analysis_region_id": region_id,
                "valid_time": row["valid_time"],
                "latitude": 36.0,
                "longitude": -96.0,
                "wind_u_10m": row["wind_u_10m"],
                "wind_v_10m": row["wind_v_10m"],
                "wind_u_80m": row["wind_u_80m"],
                "wind_v_80m": row["wind_v_80m"],
                "pbl_height_m": row["pbl_height_m"],
                "surface_pressure_hpa": row["surface_pressure_hpa"],
                "temperature_2m_k": row["temperature_2m_k"],
                "source_etag": hrrr_snapshot.source_etag,
                "source_snapshot_id": hrrr_snapshot.snapshot_id,
                "collected_at": hrrr_snapshot.collected_at,
            }
            for row in met_rows_raw
        ]
        hrrr_ingest = persist_normalized_records(
            meteorology=meteorology,
            snapshot=hrrr_snapshot,
            successful_request=requests["hrrr"],
            raw_data_dir=raw_data_dir,
            conn=connection,
        )
        camd_records = [
            {
                "facility_id": facility_id,
                "unit_id": "U1",
                "operating_date": "2024-07-15",
                "operating_hour": 12,
                "nox_mass_lbs": (
                    500.0
                    if facility_id == "PG0001"
                    else 450.0
                    if facility_id == "PGALT0001"
                    else max(1.0, 50 - index)
                ),
                "operating_time_hours": 1.0,
                "heat_input_mmbtu": 1000.0,
                "gross_load_mw": 100.0,
                "source_quality": "measured",
            }
            for index, facility_id in enumerate(
                [f"PG{value + 1:04d}" for value in range(75)] + ["PGALT0001"]
            )
        ]
        camd_body = (json.dumps(camd_records, sort_keys=True) + "\n").encode()
        camd_snapshot = write_source_snapshot(
            camd_body,
            request=requests["camd"],
            source_identity="https://example.test/camd/2024-hourly",
            extension="json",
            schema_fields=sorted(camd_records[0]),
            row_count=len(camd_records),
            source_revision_at=datetime(2025, 1, 15, tzinfo=timezone.utc),
            source_etag="example-camd-etag",
            collected_at=datetime(2025, 1, 16, tzinfo=timezone.utc),
            raw_data_dir=raw_data_dir,
            register=False,
            conn=connection,
        )
        emissions = [
            normalize_camd_hour(
                record,
                timezone_name="America/Chicago",
                utc_standard_offset_minutes=-360,
                snapshot=camd_snapshot,
            )
            for record in camd_records
        ]
        camd_ingest = persist_normalized_records(
            emissions=emissions,
            snapshot=camd_snapshot,
            successful_request=requests["camd"],
            raw_data_dir=raw_data_dir,
            conn=connection,
        )
        ingest = SourceIngestMetrics(
            snapshots=(
                harmony_ingest.snapshots + hrrr_ingest.snapshots + camd_ingest.snapshots
            ),
            pixels_inserted=harmony_ingest.pixels_inserted,
            meteorology_rows_inserted=hrrr_ingest.meteorology_rows_inserted,
            emission_revisions_inserted=camd_ingest.emission_revisions_inserted,
        )
    return {
        "cohort": cohort_metrics,
        "discovery": discovery.__dict__,
        "ingest": ingest.__dict__,
        "analysis_region_id": region_id,
        "snapshots": [
            harmony_snapshot.snapshot_id,
            hrrr_snapshot.snapshot_id,
            camd_snapshot.snapshot_id,
        ],
    }


def write_synthetic_benchmark(path: Path) -> Path:
    windows = []
    for index, split in enumerate(("calibration", "held_out")):
        windows.append(
            {
                "window_id": f"example-window-{index + 1}",
                "facility_id": f"PG{index + 1:04d}",
                "window_start": (
                    SYNTHETIC_OBSERVATION_TIME - timedelta(minutes=30)
                ).isoformat(),
                "window_end": (
                    SYNTHETIC_OBSERVATION_TIME + timedelta(minutes=30)
                ).isoformat(),
                "split_name": split,
                "plume_present": True,
                "expected_source_facility_id": "PG0001",
                "scene_class": "clear",
                "season": "summer",
                "region_label": "example",
                "operation_class": "high",
                "confounding_class": "isolated",
                "reviewer_count": 2,
                "adjudicated": True,
                "provenance": {"source": "https://example.test/benchmark"},
            }
        )
    document = {
        "schema_version": "plumegraph-benchmark-v1",
        "benchmark_version": "synthetic-benchmark-v1",
        "protocol_version": "synthetic-protocol-v1",
        "windows": windows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return path


__all__ = [
    "SYNTHETIC_OBSERVATION_TIME",
    "seed_synthetic_sources",
    "write_synthetic_benchmark",
    "write_synthetic_cohort",
]
