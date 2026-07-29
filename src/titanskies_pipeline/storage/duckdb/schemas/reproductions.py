"""Operations tables for pinned research-paper reproduction profiles."""

from __future__ import annotations

import duckdb

from titanskies_pipeline.naming import SOURCE_ANDREADIS2025, SOURCE_SUN2025
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    ANDREADIS2025_REPRO_OPS_SCHEMA,
    SUN2025_REPRO_OPS_SCHEMA,
    reproduction_ops_tbl,
)

REPRODUCTION_PROFILES = (SOURCE_SUN2025, SOURCE_ANDREADIS2025)
_SCHEMAS = {
    SOURCE_SUN2025: SUN2025_REPRO_OPS_SCHEMA,
    SOURCE_ANDREADIS2025: ANDREADIS2025_REPRO_OPS_SCHEMA,
}


def _bootstrap_profile(
    conn: duckdb.DuckDBPyConnection,
    *,
    profile_id: str,
) -> None:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{_SCHEMAS[profile_id]}"')
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "source_contracts")} (
            source_id VARCHAR NOT NULL,
            profile_version VARCHAR NOT NULL,
            paper_doi VARCHAR NOT NULL,
            provider VARCHAR NOT NULL,
            source_version VARCHAR NOT NULL,
            access_method VARCHAR NOT NULL,
            required BOOLEAN NOT NULL,
            required_exactness VARCHAR NOT NULL,
            allowed_fallbacks_json VARCHAR NOT NULL,
            canonical_url VARCHAR NOT NULL,
            concept_id VARCHAR,
            source_doi VARCHAR,
            manifest_sha256 VARCHAR NOT NULL,
            scientific_contract_sha256 VARCHAR NOT NULL,
            contract_json VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (
                manifest_sha256,
                scientific_contract_sha256,
                source_id
            )
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "source_requests")} (
            request_id VARCHAR NOT NULL PRIMARY KEY,
            source_id VARCHAR NOT NULL,
            access_method VARCHAR NOT NULL,
            request_json VARCHAR NOT NULL,
            manifest_sha256 VARCHAR NOT NULL,
            planned_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "preflight_runs")} (
            preflight_run_id VARCHAR NOT NULL PRIMARY KEY,
            manifest_sha256 VARCHAR NOT NULL,
            scientific_contract_sha256 VARCHAR NOT NULL,
            inventory_sha256 VARCHAR NOT NULL,
            inventory_mode VARCHAR NOT NULL,
            exact_mode BOOLEAN NOT NULL,
            status VARCHAR NOT NULL,
            source_count INTEGER NOT NULL,
            required_source_count INTEGER NOT NULL,
            object_count BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            unknown_size_count BIGINT NOT NULL,
            blocking_sources_json VARCHAR NOT NULL,
            report_json VARCHAR NOT NULL,
            completed_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "source_completeness")} (
            preflight_run_id VARCHAR NOT NULL,
            source_id VARCHAR NOT NULL,
            exactness_status VARCHAR NOT NULL,
            object_count BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            unknown_size_count BIGINT NOT NULL,
            blocking_reason VARCHAR,
            checked_at TIMESTAMP NOT NULL,
            PRIMARY KEY (preflight_run_id, source_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "source_objects")} (
            source_object_revision_id VARCHAR NOT NULL PRIMARY KEY,
            source_id VARCHAR NOT NULL,
            object_id VARCHAR NOT NULL,
            exactness_status VARCHAR NOT NULL,
            canonical_url VARCHAR NOT NULL,
            source_revision VARCHAR,
            checksum_algorithm VARCHAR,
            checksum VARCHAR,
            object_etag VARCHAR,
            size_bytes BIGINT,
            schema_fingerprint VARCHAR,
            object_json VARCHAR NOT NULL,
            discovered_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "preflight_source_objects")} (
            preflight_run_id VARCHAR NOT NULL,
            source_object_revision_id VARCHAR NOT NULL,
            source_id VARCHAR NOT NULL,
            inventory_sha256 VARCHAR NOT NULL,
            linked_at TIMESTAMP NOT NULL,
            PRIMARY KEY (preflight_run_id, source_object_revision_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {reproduction_ops_tbl(profile_id, "acquisition_generations")} (
            generation_id VARCHAR NOT NULL PRIMARY KEY,
            preflight_run_id VARCHAR NOT NULL,
            input_manifest_sha256 VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            promoted_at TIMESTAMP
        )
        """,
    )
    for statement in statements:
        conn.execute(statement)


def bootstrap_reproduction_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for profile_id in REPRODUCTION_PROFILES:
        _bootstrap_profile(conn, profile_id=profile_id)


__all__ = ["REPRODUCTION_PROFILES", "bootstrap_reproduction_tables"]
