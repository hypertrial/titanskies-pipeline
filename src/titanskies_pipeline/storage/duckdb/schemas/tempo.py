"""TEMPO NO2 v0.3 DuckDB raw and ops table bootstrap."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from titanskies_pipeline.storage.duckdb.schemas.constants import (
    TEMPO_NO2_OPS_SCHEMA,
    TEMPO_NO2_RAW_SCHEMA,
    tempo_ops_tbl,
    tempo_raw_tbl,
)

WAREHOUSE_SCHEMA_VERSION = "0.3"


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchone()
    )


def _prepare_v03_boundary(conn: duckdb.DuckDBPyConnection) -> None:
    """Reject populated pre-v0.3 warehouses and remove empty bootstrap tables."""
    old_tables = (
        (TEMPO_NO2_RAW_SCHEMA, "region_granule_aggregates"),
        (TEMPO_NO2_OPS_SCHEMA, "granule_inventory"),
        (TEMPO_NO2_OPS_SCHEMA, "region_registry"),
        (TEMPO_NO2_OPS_SCHEMA, "pipeline_run_events"),
    )
    old_present = [
        (schema, table)
        for schema, table in old_tables
        if _table_exists(conn, schema, table)
    ]
    populated = any(
        conn.execute(f'SELECT 1 FROM "{schema}"."{table}" LIMIT 1').fetchone()
        for schema, table in old_present
    )
    if populated:
        raise RuntimeError(
            "TitanSkies warehouse schema 0.3 requires a rebuild; this database "
            "contains v0.1/v0.2 TEMPO rows. Back it up and create a new warehouse."
        )
    for schema, table in old_present:
        conn.execute(f'DROP TABLE "{schema}"."{table}"')


def bootstrap_tempo_tables(conn: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(conn, TEMPO_NO2_RAW_SCHEMA, "region_hour_aggregates"):
        _prepare_v03_boundary(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("warehouse_metadata")} (
            metadata_key VARCHAR NOT NULL PRIMARY KEY,
            metadata_value VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )
    existing_version = conn.execute(
        f"""
        SELECT metadata_value FROM {tempo_ops_tbl("warehouse_metadata")}
        WHERE metadata_key = 'schema_version'
        """
    ).fetchone()
    if existing_version and existing_version[0] != WAREHOUSE_SCHEMA_VERSION:
        raise RuntimeError(
            "TitanSkies warehouse schema 0.3 requires a rebuild; this database "
            f"contains schema {existing_version[0]}. Back it up and create a new warehouse."
        )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {tempo_ops_tbl("warehouse_metadata")}
        VALUES ('schema_version', ?, current_timestamp)
        """,
        [WAREHOUSE_SCHEMA_VERSION],
    )
    conn.execute("CREATE SEQUENCE IF NOT EXISTS tempo_no2_hour_revision START 1")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("region_registry")} (
            country_code VARCHAR NOT NULL,
            region_type VARCHAR NOT NULL,
            source_region_id VARCHAR NOT NULL,
            canonical_region_id VARCHAR NOT NULL PRIMARY KEY,
            region_name VARCHAR NOT NULL,
            parent_region_id VARCHAR,
            timezone VARCHAR NOT NULL,
            geometry_version VARCHAR NOT NULL,
            geometry_checksum VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("geography_artifact_manifest")} (
            build_id VARCHAR NOT NULL PRIMARY KEY,
            artifact_mode VARCHAR NOT NULL,
            geometry_version VARCHAR NOT NULL,
            source_manifest_sha256 VARCHAR NOT NULL,
            registry_path VARCHAR NOT NULL,
            weights_path VARCHAR NOT NULL,
            registry_checksum VARCHAR NOT NULL,
            weights_checksum VARCHAR NOT NULL,
            grid_version VARCHAR NOT NULL,
            region_count BIGINT NOT NULL,
            weight_count BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("granule_inventory")} (
            granule_id VARCHAR NOT NULL PRIMARY KEY,
            concept_id VARCHAR NOT NULL,
            acquisition_start TIMESTAMP,
            acquisition_end TIMESTAMP,
            cmr_revision_at TIMESTAMP,
            last_seen_at TIMESTAMP NOT NULL,
            download_url VARCHAR,
            local_path VARCHAR,
            checksum_sha256 VARCHAR,
            file_size_bytes BIGINT,
            observation_time TIMESTAMP,
            observation_hour TIMESTAMP,
            discovery_status VARCHAR NOT NULL,
            download_status VARCHAR NOT NULL,
            validation_status VARCHAR NOT NULL,
            processing_status VARCHAR NOT NULL,
            discovered_at TIMESTAMP NOT NULL,
            downloaded_at TIMESTAMP,
            validated_at TIMESTAMP,
            processed_at TIMESTAMP,
            error_message VARCHAR,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("pipeline_run_events")} (
            run_id VARCHAR NOT NULL,
            job_name VARCHAR NOT NULL,
            step VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            rows_written BIGINT,
            message VARCHAR,
            PRIMARY KEY (run_id, step)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_raw_tbl("region_hour_aggregates")} (
            observation_hour TIMESTAMP NOT NULL,
            canonical_region_id VARCHAR NOT NULL,
            country_code VARCHAR NOT NULL,
            region_type VARCHAR NOT NULL,
            no2_mean DOUBLE,
            no2_median DOUBLE,
            no2_p90 DOUBLE,
            valid_pixel_count BIGINT NOT NULL,
            total_pixel_count BIGINT NOT NULL,
            valid_area_km2 DOUBLE NOT NULL,
            total_area_km2 DOUBLE NOT NULL,
            coverage_fraction DOUBLE NOT NULL,
            quality_flag_accepted BOOLEAN NOT NULL,
            source_granule_count INTEGER NOT NULL,
            all_granules_validated BOOLEAN NOT NULL,
            revision BIGINT NOT NULL,
            geometry_version VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            PRIMARY KEY (observation_hour, canonical_region_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_raw_tbl("grid_latest")} (
            grid_row INTEGER NOT NULL,
            grid_col INTEGER NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            cell_area_km2 DOUBLE NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            observation_hour TIMESTAMP NOT NULL,
            no2 DOUBLE,
            quality_flag INTEGER NOT NULL,
            quality_flag_accepted BOOLEAN NOT NULL,
            granule_id VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            PRIMARY KEY (grid_row, grid_col)
        )
        """
    )


def ensure_tempo_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_agg_hour "
        f"ON {tempo_raw_tbl('region_hour_aggregates')} (observation_hour)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_grid_latest_hour "
        f"ON {tempo_raw_tbl('grid_latest')} (observation_hour)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_agg_region "
        f"ON {tempo_raw_tbl('region_hour_aggregates')} (canonical_region_id)"
    )


def bootstrap_all_tempo_tables(conn: duckdb.DuckDBPyConnection) -> None:
    bootstrap_tempo_tables(conn)
    ensure_tempo_indexes(conn)


def create_all_tempo_test_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{TEMPO_NO2_RAW_SCHEMA}"')
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{TEMPO_NO2_OPS_SCHEMA}"')
    bootstrap_all_tempo_tables(conn)


def seed_test_tempo_pipeline_run_event(conn: duckdb.DuckDBPyConnection) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {tempo_ops_tbl("pipeline_run_events")}
        (run_id, job_name, step, status, started_at, finished_at, rows_written, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "test-run",
            "tempo_no2_full_pipeline",
            "seed",
            "success",
            now,
            now,
            0,
            "test seed",
        ],
    )


__all__ = [
    "WAREHOUSE_SCHEMA_VERSION",
    "bootstrap_all_tempo_tables",
    "bootstrap_tempo_tables",
    "create_all_tempo_test_tables",
    "ensure_tempo_indexes",
    "seed_test_tempo_pipeline_run_event",
]
