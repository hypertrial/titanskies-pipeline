"""TEMPO NO2 v0.4 DuckDB raw and ops table bootstrap (NRT + standard scopes)."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    TEMPO_NO2_OPS_SCHEMA,
    TEMPO_NO2_RAW_SCHEMA,
    TEMPO_NO2_STD_OPS_SCHEMA,
    TEMPO_NO2_STD_RAW_SCHEMA,
    hour_revision_sequence,
    tempo_ops_tbl,
    tempo_raw_tbl,
)

WAREHOUSE_SCHEMA_VERSION = "0.4"

_SCOPE_SCHEMAS: dict[str, tuple[str, str]] = {
    SCOPE_NO2: (TEMPO_NO2_RAW_SCHEMA, TEMPO_NO2_OPS_SCHEMA),
    SCOPE_NO2_STD: (TEMPO_NO2_STD_RAW_SCHEMA, TEMPO_NO2_STD_OPS_SCHEMA),
}
ALL_TEMPO_SCOPES: tuple[str, ...] = (SCOPE_NO2, SCOPE_NO2_STD)


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


def _prepare_v04_boundary(conn: duckdb.DuckDBPyConnection) -> None:
    """Reject populated pre-v0.4 warehouses and remove empty bootstrap tables."""
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
            "TitanSkies warehouse schema 0.4 requires a rebuild; this database "
            "contains pre-0.4 TEMPO rows. Back it up and create a new warehouse."
        )
    for schema, table in old_present:
        conn.execute(f'DROP TABLE "{schema}"."{table}"')


def _check_and_stamp_schema_version(conn: duckdb.DuckDBPyConnection) -> None:
    """Validate/record the single shared warehouse schema version (tempo_no2_ops)."""
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
            "TitanSkies warehouse schema 0.4 requires a rebuild; this database "
            f"contains schema {existing_version[0]}. Back it up and create a new warehouse."
        )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {tempo_ops_tbl("warehouse_metadata")}
        VALUES ('schema_version', ?, current_timestamp)
        """,
        [WAREHOUSE_SCHEMA_VERSION],
    )


def bootstrap_tempo_tables(
    conn: duckdb.DuckDBPyConnection, *, scope: str = SCOPE_NO2
) -> None:
    raw_schema, ops_schema = _SCOPE_SCHEMAS[scope]
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{raw_schema}"')
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{ops_schema}"')
    if scope == SCOPE_NO2 and not _table_exists(
        conn, raw_schema, "region_hour_aggregates"
    ):
        _prepare_v04_boundary(conn)
    _check_and_stamp_schema_version(conn)
    conn.execute(
        f"CREATE SEQUENCE IF NOT EXISTS {hour_revision_sequence(scope=scope)} START 1"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("region_registry", scope=scope)} (
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
        CREATE TABLE IF NOT EXISTS
        {tempo_ops_tbl("geography_artifact_manifest", scope=scope)} (
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
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("granule_inventory", scope=scope)} (
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
        CREATE TABLE IF NOT EXISTS {tempo_ops_tbl("pipeline_run_events", scope=scope)} (
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
        CREATE TABLE IF NOT EXISTS
        {tempo_raw_tbl("region_hour_aggregates", scope=scope)} (
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
        CREATE TABLE IF NOT EXISTS {tempo_raw_tbl("grid_latest", scope=scope)} (
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


def ensure_tempo_indexes(
    conn: duckdb.DuckDBPyConnection, *, scope: str = SCOPE_NO2
) -> None:
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_{scope}_agg_hour "
        f"ON {tempo_raw_tbl('region_hour_aggregates', scope=scope)} (observation_hour)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_{scope}_grid_latest_hour "
        f"ON {tempo_raw_tbl('grid_latest', scope=scope)} (observation_hour)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tempo_{scope}_agg_region "
        f"ON {tempo_raw_tbl('region_hour_aggregates', scope=scope)} "
        "(canonical_region_id)"
    )


def bootstrap_all_tempo_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for scope in ALL_TEMPO_SCOPES:
        bootstrap_tempo_tables(conn, scope=scope)
        ensure_tempo_indexes(conn, scope=scope)


def create_all_tempo_test_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for raw_schema, ops_schema in _SCOPE_SCHEMAS.values():
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{raw_schema}"')
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{ops_schema}"')
    bootstrap_all_tempo_tables(conn)


def seed_test_tempo_pipeline_run_event(
    conn: duckdb.DuckDBPyConnection, *, scope: str = SCOPE_NO2
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    job_name = (
        "tempo_no2_full_pipeline"
        if scope == SCOPE_NO2
        else f"tempo_{scope}_full_pipeline"
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {tempo_ops_tbl("pipeline_run_events", scope=scope)}
        (run_id, job_name, step, status, started_at, finished_at, rows_written, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "test-run",
            job_name,
            "seed",
            "success",
            now,
            now,
            0,
            "test seed",
        ],
    )


__all__ = [
    "ALL_TEMPO_SCOPES",
    "WAREHOUSE_SCHEMA_VERSION",
    "bootstrap_all_tempo_tables",
    "bootstrap_tempo_tables",
    "create_all_tempo_test_tables",
    "ensure_tempo_indexes",
    "seed_test_tempo_pipeline_run_event",
]
