"""Offline end-to-end contract for the PlumeGraph evidence ledger."""

from __future__ import annotations

import json
import os
import shutil
from datetime import date, datetime, timezone

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile
from tests.integration.duckdb.test_golden_marts import DBT_ROOT, _run_dbt

import titanskies_pipeline.storage.duckdb.connection as connection
from titanskies_pipeline.plumegraph import analysis as analysis_module
from titanskies_pipeline.plumegraph import release as release_module
from titanskies_pipeline.plumegraph.analysis import run_pending_analysis
from titanskies_pipeline.plumegraph.release import build_release, verify_release
from titanskies_pipeline.plumegraph.sources import (
    persist_normalized_records,
    source_request_from_row,
    write_source_snapshot,
)
from titanskies_pipeline.plumegraph.synthetic import (
    seed_synthetic_sources,
    write_synthetic_benchmark,
    write_synthetic_cohort,
)
from titanskies_pipeline.plumegraph.validation import load_benchmark, run_validation


def _build_args(*extra: str) -> list[str]:
    return ["build", *extra, "--select", "tag:plumegraph,tag:events"]


def test_synthetic_plumegraph_is_idempotent_and_releasable(
    tmp_path,
    dbt_profiles_dir,
    monkeypatch,
):
    db_path = tmp_path / "plumegraph.duckdb"
    raw_dir = tmp_path / "raw"
    release_dir = tmp_path / "releases"
    write_dbt_profile(dbt_profiles_dir, db_path)
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("PLUMEGRAPH_RAW_DATA_DIR", str(raw_dir))
    monkeypatch.setenv("PLUMEGRAPH_RELEASE_DIR", str(release_dir))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    cohort = write_synthetic_cohort(tmp_path / "cohort.json")
    benchmark = write_synthetic_benchmark(tmp_path / "benchmark.json")
    conn = connection.get_persistent_connection()
    try:
        seeded = seed_synthetic_sources(
            cohort_path=cohort,
            raw_data_dir=raw_dir,
            conn=conn,
        )
        real_estimate = analysis_module.estimate_emissions
        monkeypatch.setattr(
            analysis_module,
            "estimate_emissions",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("synthetic analysis failure")
            ),
        )
        with pytest.raises(analysis_module.PlumeGraphAnalysisError):
            run_pending_analysis(
                partition_dates=[date(2024, 7, 15)],
                conn=conn,
            )
        assert (
            conn.execute(
                "select count(*) from plumegraph_events_ops.current_generations"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                """
            select status
            from plumegraph_events_ops.analysis_runs
            order by started_at desc
            limit 1
            """
            ).fetchone()[0]
            == "failed"
        )
        monkeypatch.setattr(analysis_module, "estimate_emissions", real_estimate)
        first = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
        repeated = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
        assert seeded["cohort"]["cohort_facilities"] == 75
        assert seeded["ingest"]["snapshots"] == 3
        artifacts = conn.execute(
            """
            select artifact_uri, content_sha256, row_count
            from plumegraph_events_ops.normalized_artifacts
            order by connector
            """
        ).fetchall()
        assert len(artifacts) == 3
        assert all((raw_dir / row[0]).is_file() for row in artifacts)
        assert sum(row[2] for row in artifacts) == 168
        assert first == repeated
        assert first.episodes_inserted == 1
        assert (
            conn.execute(
                "select count(*) from plumegraph_events_raw.episode_revisions"
            ).fetchone()[0]
            == 1
        )
        request = source_request_from_row(
            conn.execute(
                """
                select request_id, connector, source_version, analysis_region_id,
                       window_start, window_end, request_contract_version,
                       request_json
                from plumegraph_events_ops.source_requests
                where connector = 'harmony'
                limit 1
                """
            ).fetchone()
        )
        correction = write_source_snapshot(
            b"[]\n",
            request=request,
            source_identity="https://example.test/TEMPO_NO2_L2_CORRECTION",
            extension="json",
            schema_fields=[],
            row_count=0,
            source_revision_at=datetime(2024, 7, 18, tzinfo=timezone.utc),
            collected_at=datetime(2024, 7, 19, tzinfo=timezone.utc),
            raw_data_dir=raw_dir,
            register=False,
            conn=conn,
        )
        persist_normalized_records(
            snapshot=correction,
            successful_request=request,
            raw_data_dir=raw_dir,
            conn=conn,
        )
        corrected = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
        assert corrected.episodes_inserted == 1
        assert (
            run_pending_analysis(
                partition_dates=[date(2024, 7, 15)],
                conn=conn,
            )
            == corrected
        )
        assert (
            conn.execute(
                "select count(*) from plumegraph_events_raw.episode_revisions"
            ).fetchone()[0]
            == 2
        )
        assert (
            conn.execute(
                "select count(*) from plumegraph_events_raw.episode_lineage"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "select count(*) from plumegraph_events_raw.episode_tracking_edges"
            ).fetchone()[0]
            == 2
        )
    finally:
        conn.close()

    env = os.environ.copy()
    _run_dbt(_build_args(), profiles_dir=dbt_profiles_dir, env=env)

    conn = connection.get_persistent_connection()
    conn.execute(
        """
        insert into plumegraph_events_raw.retrieval_pixel_revisions
        select
            'late-authoritative-correction',
            pixel_id,
            analysis_region_id,
            granule_id,
            mirror_step,
            xtrack,
            observation_time,
            original_time,
            time_standard,
            latitude,
            longitude,
            geometry_wkb,
            pixel_area_km2,
            no2_vertical_column + 1e12,
            no2_uncertainty,
            no2_unit,
            quality_flag,
            cloud_fraction,
            snow_ice_fraction,
            amf_diagnostic_flag,
            solar_zenith_angle,
            viewing_zenith_angle,
            surface_pressure_hpa,
            collection_name,
            collection_version,
            timestamp '2024-07-20 00:00:00',
            ?,
            '{"correction": true}',
            timestamp '2024-01-01 00:00:00'
        from plumegraph_events_raw.retrieval_pixel_revisions
        order by pixel_revision_id
        limit 1
        """,
        [correction.snapshot_id],
    )
    conn.execute(
        """
        insert into plumegraph_events_raw.hourly_emission_revisions
        select
            'late-emission-correction',
            emission_id,
            facility_id,
            unit_id,
            operating_date,
            operating_hour,
            observation_start_utc,
            nox_mass_lbs + 1,
            operating_time_hours,
            heat_input_mmbtu,
            gross_load_mw,
            source_quality,
            timestamp '2025-02-01 00:00:00',
            source_snapshot_id,
            '{"correction": true}',
            timestamp '2024-01-01 00:00:00'
        from plumegraph_events_raw.hourly_emission_revisions
        order by emission_revision_id
        limit 1
        """
    )
    conn.close()
    _run_dbt(_build_args(), profiles_dir=dbt_profiles_dir, env=env)
    conn = duckdb.connect(str(db_path))
    assert (
        conn.execute(
            """
        select count(*)
        from plumegraph_events_intermediate.int_plumegraph_events_current_pixels
        where pixel_revision_id = 'late-authoritative-correction'
        """
        ).fetchone()[0]
        == 1
    )
    assert (
        conn.execute(
            """
        select count(*)
        from plumegraph_events_intermediate.int_plumegraph_events_current_emissions
        where emission_revision_id = 'late-emission-correction'
        """
        ).fetchone()[0]
        == 1
    )
    conn.close()

    copied_dbt = tmp_path / "dbt-contract-change"
    shutil.copytree(
        DBT_ROOT,
        copied_dbt,
        ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"),
    )
    contract_path = copied_dbt / "seeds" / "plumegraph_events_contract.csv"
    contract_path.write_text(
        contract_path.read_text().replace(
            "default,0.6.0,",
            "default,0.6-test,",
        )
    )
    _run_dbt(
        _build_args(),
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=copied_dbt,
    )
    conn = duckdb.connect(str(db_path))
    assert conn.execute(
        """
        select distinct contract_version
        from plumegraph_events_intermediate.int_plumegraph_events_current_pixels
        """
    ).fetchall() == [("0.6-test",)]
    conn.execute(
        """
        create table plumegraph_incremental_snapshot as
        select *
        from plumegraph_events_intermediate.int_plumegraph_events_current_pixels
        """
    )
    conn.close()
    _run_dbt(
        _build_args("--full-refresh"),
        profiles_dir=dbt_profiles_dir,
        env=env,
        project_dir=copied_dbt,
    )
    conn = duckdb.connect(str(db_path))
    difference = conn.execute(
        """
        select count(*) from (
            (
                select * from plumegraph_incremental_snapshot
                except
                select *
                from plumegraph_events_intermediate.int_plumegraph_events_current_pixels
            )
            union all
            (
                select *
                from plumegraph_events_intermediate.int_plumegraph_events_current_pixels
                except
                select * from plumegraph_incremental_snapshot
            )
        )
        """
    ).fetchone()[0]
    conn.close()
    assert difference == 0

    conn = connection.get_persistent_connection()
    try:
        benchmark_info = load_benchmark(
            benchmark,
            allow_incomplete=True,
            conn=conn,
        )
        validation = run_validation(
            "synthetic-benchmark-v1",
            allow_incomplete=True,
            conn=conn,
        )
        assert benchmark_info["windows"] == 2
        assert validation.passed
        assert validation.probability_enabled is False
    finally:
        conn.close()

    _run_dbt(_build_args(), profiles_dir=dbt_profiles_dir, env=env)
    conn = connection.get_persistent_connection()
    try:
        release = build_release(
            release_version="synthetic-v1",
            output_dir=release_dir,
            validation_run_id=validation.validation_run_id,
            conn=conn,
        )
        manifest = verify_release(release.release_path)
        repeated_release = build_release(
            release_version="synthetic-v1",
            output_dir=release_dir,
            validation_run_id=validation.validation_run_id,
            conn=conn,
        )
        assert release.episode_count == 1
        assert repeated_release == release
        assert manifest["evidence_format"] == "plumegraph-evidence-v1"
        assert manifest["files"]
        assert len(manifest["normalized_artifacts"]) == 4
        bundles = [
            json.loads(path.read_text())
            for path in sorted((release.release_path / "evidence").glob("*.json"))
        ]
        assert len(bundles) == 2
        assert all(bundle["candidate_sources"] for bundle in bundles)
        assert all(bundle["emission_estimates"] for bundle in bundles)
        assert all(bundle["evidence_pixels"] for bundle in bundles)
        assert all(bundle["geometries"] for bundle in bundles)
        assert all(bundle["provenance"] for bundle in bundles)

        real_hash = release_module.sha256_file
        calls: dict[str, int] = {}

        def changing_hash(path):
            key = str(path)
            calls[key] = calls.get(key, 0) + 1
            if path.name == "facilities.parquet" and calls[key] == 2:
                return "0" * 64
            return real_hash(path)

        monkeypatch.setattr(release_module, "sha256_file", changing_hash)
        with pytest.raises(ValueError, match="checksum mismatch"):
            build_release(
                release_version="checksum-failure",
                output_dir=release_dir,
                validation_run_id=validation.validation_run_id,
                conn=conn,
            )
        monkeypatch.setattr(release_module, "sha256_file", real_hash)

        manifest_path = release.release_path / "manifest.json"
        manifest_path.write_text(manifest_path.read_text() + " ")
        with pytest.raises(FileExistsError, match="immutable"):
            build_release(
                release_version="synthetic-v1",
                output_dir=release_dir,
                validation_run_id=validation.validation_run_id,
                conn=conn,
            )
    finally:
        conn.close()

    read_only = duckdb.connect(str(db_path), read_only=True)
    try:
        assert read_only.execute(
            """
            select
                count(distinct episodes.episode_revision_id) = 1
                and count(distinct provenance.source_snapshot_id) = 4
                and count(distinct provenance.normalized_artifact_id) = 4
            from plumegraph_events_marts.plumegraph_events_episodes as episodes
            inner join plumegraph_events_marts.plumegraph_events_provenance
                as provenance
                on episodes.episode_revision_id = provenance.episode_revision_id
            """
        ).fetchone()[0]
        assert (
            read_only.execute(
                """
            select count(*)
            from plumegraph_events_marts.plumegraph_events_emission_estimates
            """
            ).fetchone()[0]
            == 108
        )
        assert (
            read_only.execute(
                """
            select count(*)
            from plumegraph_events_marts.plumegraph_events_facilities
            where is_alternative_source
            """
            ).fetchone()[0]
            == 1
        )
        assert (
            read_only.execute(
                """
            select count(*)
            from plumegraph_events_marts.plumegraph_events_candidate_sources
            where is_alternative_source
            """
            ).fetchone()[0]
            == 2
        )
    finally:
        read_only.close()
        connection.reset_duckdb_connection_state()


def test_single_scan_episode_is_retained_with_explicit_abstention(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "single-scan.duckdb"
    raw_dir = tmp_path / "raw"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("PLUMEGRAPH_RAW_DATA_DIR", str(raw_dir))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        cohort = write_synthetic_cohort(tmp_path / "cohort.json")
        seed_synthetic_sources(
            cohort_path=cohort,
            raw_data_dir=raw_dir,
            conn=conn,
        )
        conn.execute(
            """
            delete from plumegraph_events_raw.retrieval_pixel_revisions
            where observation_time > (
                select min(observation_time)
                from plumegraph_events_raw.retrieval_pixel_revisions
            )
            """
        )
        metrics = run_pending_analysis(
            partition_dates=[date(2024, 7, 15)],
            conn=conn,
        )
        assert metrics.episodes_inserted == 1
        assert conn.execute(
            """
            select
                not is_analysis_ready
                and evidence_status = 'insufficient_evidence'
                and attribution_class = 'insufficient_evidence'
            from plumegraph_events_raw.episode_revisions
            """
        ).fetchone()[0]
        assert conn.execute(
            """
            select
                count(*) > 0
                and count_if(attribution_probability is not null) = 0
                and count_if(is_probability_ready) = 0
            from plumegraph_events_raw.candidate_source_revisions
            """
        ).fetchone()[0]
    finally:
        conn.close()
        connection.reset_duckdb_connection_state()
