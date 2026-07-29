"""RiverPulse events raw and operations table bootstrap."""

from __future__ import annotations

import duckdb

from titanskies_pipeline.storage.duckdb.schemas.constants import (
    RIVERPULSE_EVENTS_OPS_SCHEMA,
    RIVERPULSE_EVENTS_RAW_SCHEMA,
    riverpulse_ops_tbl,
    riverpulse_raw_tbl,
)


def bootstrap_riverpulse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{RIVERPULSE_EVENTS_OPS_SCHEMA}"')
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{RIVERPULSE_EVENTS_RAW_SCHEMA}"')
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_ops_tbl("network_artifact_manifest")} (
            build_id VARCHAR NOT NULL PRIMARY KEY,
            artifact_mode VARCHAR NOT NULL,
            network_version VARCHAR NOT NULL,
            source_manifest_sha256 VARCHAR NOT NULL,
            reaches_path VARCHAR NOT NULL,
            edges_path VARCHAR NOT NULL,
            reaches_sha256 VARCHAR NOT NULL,
            edges_sha256 VARCHAR NOT NULL,
            reach_count BIGINT NOT NULL,
            edge_count BIGINT NOT NULL,
            resolved_anchors_json VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_ops_tbl("source_requests")} (
            request_id VARCHAR NOT NULL PRIMARY KEY,
            connector VARCHAR NOT NULL,
            collection_name VARCHAR NOT NULL,
            reach_id VARCHAR NOT NULL,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            field_contract_version VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            attempts INTEGER NOT NULL,
            http_status INTEGER,
            row_count BIGINT,
            error_message VARCHAR,
            planned_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_ops_tbl("source_snapshots")} (
            snapshot_id VARCHAR NOT NULL PRIMARY KEY,
            request_id VARCHAR NOT NULL,
            response_sha256 VARCHAR NOT NULL,
            artifact_uri VARCHAR NOT NULL,
            http_status INTEGER NOT NULL,
            row_count BIGINT NOT NULL,
            collected_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_raw_tbl("reaches")} (
            network_version VARCHAR NOT NULL,
            reach_id VARCHAR NOT NULL,
            basin_key VARCHAR NOT NULL,
            river_name VARCHAR,
            reach_length_m DOUBLE,
            flow_accumulation DOUBLE,
            distance_to_outlet_m DOUBLE,
            geometry_wkb BLOB NOT NULL,
            centroid_longitude DOUBLE NOT NULL,
            centroid_latitude DOUBLE NOT NULL,
            is_outlet_anchor BOOLEAN NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (network_version, reach_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_raw_tbl("reach_edges")} (
            network_version VARCHAR NOT NULL,
            from_reach_id VARCHAR NOT NULL,
            to_reach_id VARCHAR NOT NULL,
            is_selection_boundary BOOLEAN NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (network_version, from_reach_id, to_reach_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_raw_tbl("observation_revisions")} (
            observation_revision_id VARCHAR NOT NULL PRIMARY KEY,
            observation_id VARCHAR NOT NULL,
            reach_id VARCHAR NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            cycle_id INTEGER NOT NULL,
            pass_id INTEGER NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            river_name VARCHAR,
            wse DOUBLE,
            wse_u DOUBLE,
            wse_r_u DOUBLE,
            wse_c DOUBLE,
            wse_c_u DOUBLE,
            width DOUBLE,
            width_u DOUBLE,
            width_c DOUBLE,
            width_c_u DOUBLE,
            slope DOUBLE,
            slope_u DOUBLE,
            slope_r_u DOUBLE,
            slope2 DOUBLE,
            slope2_u DOUBLE,
            slope2_r_u DOUBLE,
            wse_unit VARCHAR NOT NULL,
            width_unit VARCHAR NOT NULL,
            slope_unit VARCHAR NOT NULL,
            unconstrained_discharge_quality_bits BIGINT,
            constrained_discharge_quality_bits BIGINT,
            reach_quality INTEGER,
            reach_quality_bits BIGINT,
            dark_fraction DOUBLE,
            ice_climatology_flag INTEGER,
            ice_dynamic_flag INTEGER,
            partial_flag INTEGER,
            good_node_count INTEGER,
            observed_node_fraction DOUBLE,
            crossover_calibration_quality INTEGER,
            upstream_reach_count INTEGER,
            downstream_reach_count INTEGER,
            upstream_reach_ids VARCHAR,
            downstream_reach_ids VARCHAR,
            distance_to_outlet_m DOUBLE,
            reach_length_m DOUBLE,
            continent_id VARCHAR,
            range_start_time TIMESTAMP,
            range_end_time TIMESTAMP,
            collection_name VARCHAR NOT NULL,
            collection_version VARCHAR NOT NULL,
            crid VARCHAR NOT NULL,
            sword_version VARCHAR NOT NULL,
            granule_id VARCHAR NOT NULL,
            source_ingest_time TIMESTAMP NOT NULL,
            collected_at TIMESTAMP NOT NULL,
            response_sha256 VARCHAR NOT NULL,
            canonical_record_json VARCHAR NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {riverpulse_raw_tbl("discharge_revisions")} (
            observation_revision_id VARCHAR NOT NULL,
            algorithm VARCHAR NOT NULL,
            is_constrained BOOLEAN NOT NULL,
            discharge_value DOUBLE,
            discharge_uncertainty DOUBLE,
            discharge_quality INTEGER,
            scale_factor DOUBLE,
            discharge_unit VARCHAR NOT NULL,
            collection_name VARCHAR NOT NULL,
            collection_version VARCHAR NOT NULL,
            sword_version VARCHAR NOT NULL,
            response_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (
                observation_revision_id,
                algorithm,
                is_constrained
            )
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {riverpulse_raw_tbl("observation_snapshot_links")} (
            observation_revision_id VARCHAR NOT NULL,
            snapshot_id VARCHAR NOT NULL,
            linked_at TIMESTAMP NOT NULL,
            PRIMARY KEY (observation_revision_id, snapshot_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_riverpulse_observation_identity
        ON {riverpulse_raw_tbl("observation_revisions")}
        (observation_id, source_ingest_time)
        """
    )


__all__ = ["bootstrap_riverpulse_tables"]
