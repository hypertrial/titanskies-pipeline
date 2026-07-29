"""PlumeGraph event-ledger raw and operations table bootstrap."""

from __future__ import annotations

import duckdb

from titanskies_pipeline.storage.duckdb.schemas.constants import (
    PLUMEGRAPH_EVENTS_OPS_SCHEMA,
    PLUMEGRAPH_EVENTS_RAW_SCHEMA,
    plumegraph_ops_tbl,
    plumegraph_raw_tbl,
)


def bootstrap_plumegraph_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{PLUMEGRAPH_EVENTS_OPS_SCHEMA}"')
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{PLUMEGRAPH_EVENTS_RAW_SCHEMA}"')
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("cohort_manifests")} (
            cohort_version VARCHAR NOT NULL PRIMARY KEY,
            manifest_sha256 VARCHAR NOT NULL,
            source_manifest_sha256 VARCHAR NOT NULL,
            artifact_uri VARCHAR NOT NULL,
            facility_count INTEGER NOT NULL,
            review_status VARCHAR NOT NULL,
            approved_by VARCHAR,
            loaded_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("source_requests")} (
            request_id VARCHAR NOT NULL PRIMARY KEY,
            connector VARCHAR NOT NULL,
            source_version VARCHAR NOT NULL,
            analysis_region_id VARCHAR,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            request_json VARCHAR NOT NULL,
            request_contract_version VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            attempts INTEGER NOT NULL,
            http_status INTEGER,
            error_message VARCHAR,
            planned_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("source_snapshots")} (
            snapshot_id VARCHAR NOT NULL PRIMARY KEY,
            request_id VARCHAR NOT NULL,
            connector VARCHAR NOT NULL,
            source_identity VARCHAR NOT NULL,
            source_revision_at TIMESTAMP,
            artifact_uri VARCHAR NOT NULL,
            content_sha256 VARCHAR NOT NULL,
            source_etag VARCHAR,
            schema_fingerprint VARCHAR NOT NULL,
            row_count BIGINT NOT NULL,
            collected_at TIMESTAMP NOT NULL,
            source_lineage_json VARCHAR NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("normalized_artifacts")} (
            normalized_artifact_id VARCHAR NOT NULL PRIMARY KEY,
            source_snapshot_id VARCHAR NOT NULL,
            connector VARCHAR NOT NULL,
            analysis_region_id VARCHAR,
            partition_date DATE NOT NULL,
            artifact_uri VARCHAR NOT NULL,
            content_sha256 VARCHAR NOT NULL,
            schema_fingerprint VARCHAR NOT NULL,
            row_count BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("analysis_runs")} (
            analysis_run_id VARCHAR NOT NULL PRIMARY KEY,
            analysis_region_id VARCHAR NOT NULL,
            partition_date DATE NOT NULL,
            input_manifest_sha256 VARCHAR NOT NULL,
            contract_version VARCHAR NOT NULL,
            algorithm_version VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            episode_count BIGINT NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            error_message VARCHAR
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("current_generations")} (
            analysis_region_id VARCHAR NOT NULL,
            partition_date DATE NOT NULL,
            analysis_run_id VARCHAR NOT NULL,
            promoted_at TIMESTAMP NOT NULL,
            PRIMARY KEY (analysis_region_id, partition_date)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("validation_runs")} (
            validation_run_id VARCHAR NOT NULL PRIMARY KEY,
            analysis_run_manifest_sha256 VARCHAR NOT NULL,
            benchmark_version VARCHAR NOT NULL,
            split_name VARCHAR NOT NULL,
            detection_precision DOUBLE,
            detection_recall DOUBLE,
            source_top1_accuracy DOUBLE,
            expected_calibration_error DOUBLE,
            probability_enabled BOOLEAN NOT NULL,
            passed BOOLEAN NOT NULL,
            metrics_json VARCHAR NOT NULL,
            completed_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("benchmark_manifests")} (
            benchmark_version VARCHAR NOT NULL PRIMARY KEY,
            manifest_sha256 VARCHAR NOT NULL,
            protocol_version VARCHAR NOT NULL,
            window_count INTEGER NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_ops_tbl("release_manifests")} (
            release_id VARCHAR NOT NULL PRIMARY KEY,
            evidence_format VARCHAR NOT NULL,
            release_version VARCHAR NOT NULL,
            analysis_manifest_sha256 VARCHAR NOT NULL,
            validation_run_id VARCHAR,
            artifact_uri VARCHAR NOT NULL,
            manifest_sha256 VARCHAR NOT NULL,
            episode_count BIGINT NOT NULL,
            published_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("facilities")} (
            cohort_version VARCHAR NOT NULL,
            facility_id VARCHAR NOT NULL,
            facility_name VARCHAR NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            geometry_wkb BLOB NOT NULL,
            timezone VARCHAR NOT NULL,
            utc_standard_offset_minutes INTEGER NOT NULL,
            annual_nox_tons DOUBLE,
            is_cohort BOOLEAN NOT NULL,
            review_status VARCHAR NOT NULL,
            inclusion_reason VARCHAR NOT NULL,
            source_snapshot_id VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (cohort_version, facility_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("analysis_regions")} (
            cohort_version VARCHAR NOT NULL,
            analysis_region_id VARCHAR NOT NULL PRIMARY KEY,
            facility_ids_json VARCHAR NOT NULL,
            geometry_wkb BLOB NOT NULL,
            aoi_radius_km DOUBLE NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("hourly_emission_revisions")} (
            emission_revision_id VARCHAR NOT NULL PRIMARY KEY,
            emission_id VARCHAR NOT NULL,
            facility_id VARCHAR NOT NULL,
            unit_id VARCHAR NOT NULL,
            operating_date DATE NOT NULL,
            operating_hour INTEGER NOT NULL,
            observation_start_utc TIMESTAMP NOT NULL,
            nox_mass_lbs DOUBLE,
            operating_time_hours DOUBLE,
            heat_input_mmbtu DOUBLE,
            gross_load_mw DOUBLE,
            source_quality VARCHAR,
            source_revision_at TIMESTAMP,
            source_snapshot_id VARCHAR NOT NULL,
            canonical_record_json VARCHAR NOT NULL,
            collected_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("retrieval_pixel_revisions")} (
            pixel_revision_id VARCHAR NOT NULL PRIMARY KEY,
            pixel_id VARCHAR NOT NULL,
            analysis_region_id VARCHAR NOT NULL,
            granule_id VARCHAR NOT NULL,
            mirror_step INTEGER NOT NULL,
            xtrack INTEGER NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            original_time DOUBLE NOT NULL,
            time_standard VARCHAR NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            geometry_wkb BLOB,
            pixel_area_km2 DOUBLE,
            no2_vertical_column DOUBLE,
            no2_uncertainty DOUBLE,
            no2_unit VARCHAR NOT NULL,
            quality_flag INTEGER,
            cloud_fraction DOUBLE,
            snow_ice_fraction DOUBLE,
            amf_diagnostic_flag BIGINT,
            solar_zenith_angle DOUBLE,
            viewing_zenith_angle DOUBLE,
            surface_pressure_hpa DOUBLE,
            collection_name VARCHAR NOT NULL,
            collection_version VARCHAR NOT NULL,
            source_revision_at TIMESTAMP,
            source_snapshot_id VARCHAR NOT NULL,
            canonical_record_json VARCHAR NOT NULL,
            collected_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("meteorology_observations")} (
            meteorology_revision_id VARCHAR NOT NULL PRIMARY KEY,
            analysis_region_id VARCHAR NOT NULL,
            valid_time TIMESTAMP NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            wind_u_10m DOUBLE,
            wind_v_10m DOUBLE,
            wind_u_80m DOUBLE,
            wind_v_80m DOUBLE,
            pbl_height_m DOUBLE,
            surface_pressure_hpa DOUBLE,
            temperature_2m_k DOUBLE,
            source_etag VARCHAR,
            source_snapshot_id VARCHAR NOT NULL,
            collected_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_revisions")} (
            episode_revision_id VARCHAR NOT NULL PRIMARY KEY,
            plume_id VARCHAR NOT NULL,
            analysis_run_id VARCHAR NOT NULL,
            pollutant VARCHAR NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            observation_count INTEGER NOT NULL,
            plume_pixel_count INTEGER NOT NULL,
            background_pixel_count INTEGER NOT NULL,
            enhancement_molecules_cm2 DOUBLE,
            background_molecules_cm2 DOUBLE,
            estimated_age_hours DOUBLE,
            attribution_class VARCHAR NOT NULL,
            evidence_status VARCHAR NOT NULL,
            is_analysis_ready BOOLEAN NOT NULL,
            contract_version VARCHAR NOT NULL,
            algorithm_version VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_geometries")} (
            episode_revision_id VARCHAR NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            geometry_wkb BLOB NOT NULL,
            centroid_latitude DOUBLE NOT NULL,
            centroid_longitude DOUBLE NOT NULL,
            wind_u_ms DOUBLE,
            wind_v_ms DOUBLE,
            PRIMARY KEY (episode_revision_id, observation_time)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_tracking_edges")} (
            episode_revision_id VARCHAR NOT NULL,
            tracking_edge_id VARCHAR NOT NULL,
            from_component_id VARCHAR NOT NULL,
            to_component_id VARCHAR NOT NULL,
            from_time TIMESTAMP NOT NULL,
            to_time TIMESTAMP NOT NULL,
            gap_hours DOUBLE NOT NULL,
            geometry_iou DOUBLE NOT NULL,
            advection_residual_km DOUBLE NOT NULL,
            concentration_ratio DOUBLE NOT NULL,
            PRIMARY KEY (episode_revision_id, tracking_edge_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_lineage")} (
            from_episode_revision_id VARCHAR NOT NULL,
            to_episode_revision_id VARCHAR NOT NULL,
            relation_type VARCHAR NOT NULL,
            temporal_overlap DOUBLE,
            mean_geometry_iou DOUBLE,
            PRIMARY KEY (
                from_episode_revision_id,
                to_episode_revision_id,
                relation_type
            )
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_target_links")} (
            episode_revision_id VARCHAR NOT NULL,
            facility_id VARCHAR NOT NULL,
            PRIMARY KEY (episode_revision_id, facility_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("candidate_source_revisions")} (
            episode_revision_id VARCHAR NOT NULL,
            facility_id VARCHAR NOT NULL,
            rank INTEGER NOT NULL,
            trajectory_score DOUBLE NOT NULL,
            emissions_score DOUBLE,
            distance_score DOUBLE NOT NULL,
            annual_prior_score DOUBLE NOT NULL,
            attribution_score DOUBLE NOT NULL,
            attribution_probability DOUBLE,
            attribution_class VARCHAR NOT NULL,
            distance_km DOUBLE NOT NULL,
            is_cohort BOOLEAN NOT NULL,
            is_probability_ready BOOLEAN NOT NULL,
            PRIMARY KEY (episode_revision_id, facility_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("emission_estimate_revisions")} (
            episode_revision_id VARCHAR NOT NULL,
            variant_id VARCHAR NOT NULL,
            wind_variant VARCHAR NOT NULL,
            wind_speed_ms DOUBLE NOT NULL,
            lifetime_hours DOUBLE NOT NULL,
            no2_nox_ratio DOUBLE NOT NULL,
            no2_flux_kg_h DOUBLE NOT NULL,
            nox_flux_kg_h DOUBLE NOT NULL,
            retrieval_uncertainty_kg_h DOUBLE,
            background_uncertainty_kg_h DOUBLE,
            wind_uncertainty_kg_h DOUBLE,
            geometry_uncertainty_kg_h DOUBLE,
            is_central BOOLEAN NOT NULL,
            PRIMARY KEY (episode_revision_id, variant_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("episode_pixel_links")} (
            episode_revision_id VARCHAR NOT NULL,
            pixel_revision_id VARCHAR NOT NULL,
            evidence_role VARCHAR NOT NULL,
            filter_reason VARCHAR,
            enhancement_molecules_cm2 DOUBLE,
            PRIMARY KEY (episode_revision_id, pixel_revision_id, evidence_role)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("provenance_links")} (
            episode_revision_id VARCHAR NOT NULL,
            source_type VARCHAR NOT NULL,
            source_snapshot_id VARCHAR NOT NULL,
            input_identity VARCHAR NOT NULL,
            PRIMARY KEY (
                episode_revision_id,
                source_type,
                source_snapshot_id,
                input_identity
            )
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {plumegraph_raw_tbl("benchmark_labels")} (
            benchmark_version VARCHAR NOT NULL,
            window_id VARCHAR NOT NULL,
            facility_id VARCHAR NOT NULL,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            split_name VARCHAR NOT NULL,
            plume_present BOOLEAN NOT NULL,
            expected_source_facility_id VARCHAR,
            scene_class VARCHAR NOT NULL,
            season VARCHAR NOT NULL,
            region_label VARCHAR NOT NULL,
            operation_class VARCHAR NOT NULL,
            confounding_class VARCHAR NOT NULL,
            reviewer_count INTEGER NOT NULL,
            adjudicated BOOLEAN NOT NULL,
            protocol_version VARCHAR NOT NULL,
            provenance_json VARCHAR NOT NULL,
            PRIMARY KEY (benchmark_version, window_id)
        )
        """,
    )
    for statement in statements:
        conn.execute(statement)
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_plumegraph_pixel_identity
        ON {plumegraph_raw_tbl("retrieval_pixel_revisions")}
        (pixel_id, source_revision_at, collected_at)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_plumegraph_emission_identity
        ON {plumegraph_raw_tbl("hourly_emission_revisions")}
        (emission_id, source_revision_at, collected_at)
        """
    )


__all__ = ["bootstrap_plumegraph_tables"]
