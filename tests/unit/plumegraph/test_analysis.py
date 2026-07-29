from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import duckdb
import pytest
from shapely.geometry import box

from titanskies_pipeline.config.settings_plumegraph import load_plumegraph_contract
from titanskies_pipeline.plumegraph import analysis
from titanskies_pipeline.plumegraph.science import DetectionResult
from titanskies_pipeline.plumegraph.synthetic import (
    seed_synthetic_sources,
    write_synthetic_cohort,
)
from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)

UTC = timezone.utc
CONTRACT = load_plumegraph_contract()


@dataclass
class _Candidate:
    facility_id: str
    score: float


def _component(
    timestamp,
    geometry,
    pixel_ids=("p",),
    *,
    targets=("f",),
):
    return analysis._Component(
        set(targets),
        timestamp,
        DetectionResult(
            tuple(pixel_ids),
            ("b",),
            (),
            1.0,
            0.1,
            2.0,
            True,
            "analysis_ready",
        ),
        geometry,
        1,
        0,
        1,
        0,
    )


def test_time_and_meteorology_interpolation_edges():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    assert analysis._aware(now.replace(tzinfo=None)).tzinfo == UTC
    with pytest.raises(ValueError, match="timezone"):
        analysis._db_time(now.replace(tzinfo=None))
    rows = [
        (now, 0, 0, 10, 0, 1, None, 300),
        (now + timedelta(hours=1), 2, 2, 20, 2, 3, None, 302),
    ]
    exact = analysis._interpolate_meteorology(rows, now, 90)
    assert exact["u80"] == 10
    assert "pressure" not in exact
    midpoint = analysis._interpolate_meteorology(
        rows,
        now + timedelta(minutes=30),
        90,
    )
    assert midpoint["u80"] == 15
    assert midpoint["temperature"] == 301
    assert "pressure" not in midpoint
    variants = analysis._wind_sensitivity_variants(
        rows,
        [_component(now + timedelta(minutes=30), box(0, 0, 1, 1))],
    )
    assert set(variants) == {
        "10m",
        "80m",
        "10m_previous",
        "80m_previous",
        "10m_next",
        "80m_next",
    }
    assert (
        analysis._interpolate_meteorology(
            rows,
            now - timedelta(minutes=1),
            90,
        )
        is None
    )
    assert (
        analysis._interpolate_meteorology(
            rows,
            now + timedelta(hours=2),
            90,
        )
        is None
    )
    far_rows = [
        (now, 1, 1, 1, 1, 1, 1, 1),
        (now + timedelta(hours=4), 1, 1, 1, 1, 1, 1, 1),
    ]
    assert (
        analysis._interpolate_meteorology(
            far_rows,
            now + timedelta(hours=2),
            90,
        )
        is None
    )


def test_component_deduplication_tracking_and_jaccard():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    shape = box(0, 0, 1, 1)
    first = _component(now, shape, ("a", "b"), targets=("f1",))
    duplicate = _component(now, shape, ("a", "b", "c"), targets=("f2",))
    other = _component(now, box(10, 10, 11, 11), ("z",))
    deduplicated = analysis._deduplicate_components(
        [first, duplicate, other],
        0.5,
    )
    assert len(deduplicated) == 2
    assert deduplicated[0].target_ids == {"f1", "f2"}
    assert analysis._jaccard(set(), set()) == 1

    next_scan = _component(now + timedelta(hours=1), shape, ("d",))
    far = _component(now + timedelta(hours=1), box(20, 20, 21, 21), ("e",))
    same_time = _component(now, shape, ("f",))
    tracks = analysis._component_tracks(
        [first, next_scan, far, same_time],
        CONTRACT,
    )
    assert sorted(len(track) for track in tracks) == [1, 3]
    edges = analysis._track_edges(max(tracks, key=len), CONTRACT)
    assert len(edges) == 2
    assert all(edge.from_time < edge.to_time for edge in edges)
    assert analysis._tracking_edge(first, same_time, CONTRACT) is None
    directional_origin = _component(
        now,
        box(0, 0, 0.01, 0.01),
        ("origin",),
    )
    directional_origin.wind_u_80m = 10
    westward = _component(
        now + timedelta(hours=1),
        box(-0.324, 0, -0.314, 0.01),
        ("west",),
    )
    assert analysis._tracking_edge(directional_origin, westward, CONTRACT) is None


def test_lineage_assignments_are_mutual_and_explicit():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    shape = box(0, 0, 1, 1)
    prior = [analysis._PriorEpisode("old", "plume", {now: shape})]
    inherited, edges = analysis._lineage_assignments(
        prior,
        [[_component(now, shape)]],
        CONTRACT,
    )
    assert inherited == {0: "plume"}
    assert edges[0][0][1] == "supersedes"

    inherited, edges = analysis._lineage_assignments(
        prior,
        [[_component(now, shape)], [_component(now, shape)]],
        CONTRACT,
    )
    assert inherited == {}
    assert {edge[1] for values in edges.values() for edge in values} == {"split_from"}

    inherited, edges = analysis._lineage_assignments(
        [
            analysis._PriorEpisode("old1", "p1", {now: shape}),
            analysis._PriorEpisode("old2", "p2", {now: shape}),
        ],
        [[_component(now, shape)]],
        CONTRACT,
    )
    assert inherited == {}
    assert {edge[1] for edge in edges[0]} == {"merged_from"}

    inherited, edges = analysis._lineage_assignments(
        prior,
        [
            [
                _component(
                    now + timedelta(hours=1),
                    box(10, 10, 11, 11),
                )
            ]
        ],
        CONTRACT,
    )
    assert inherited == {}
    assert edges == {}
    inherited, edges = analysis._lineage_assignments(
        prior,
        [[_component(now, box(10, 10, 11, 11))]],
        CONTRACT,
    )
    assert inherited == {}
    assert edges == {}


def test_prior_episode_lookup_and_failed_partition_status():
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    assert analysis._prior_episodes(conn, "region", date(2024, 1, 1)) == []
    conn.execute(
        """
        insert into plumegraph_events_raw.retrieval_pixel_revisions (
            pixel_revision_id, pixel_id, analysis_region_id, granule_id,
            mirror_step, xtrack, observation_time, original_time,
            time_standard, no2_unit, collection_name, collection_version,
            source_snapshot_id, canonical_record_json, collected_at
        )
        values (
            'incomplete', 'pixel', 'region', 'granule', 0, 0,
            timestamp '2024-01-01 00:00:00', 1, 'GPS', 'molecules/cm2',
            'TEMPO_NO2_L2', 'V04', 'snapshot', '{}', current_timestamp
        )
        """
    )
    assert (
        analysis._current_pixels(
            conn,
            "region",
            date(2024, 1, 1),
            overlap_hours=3,
        )
        == []
    )
    conn.execute(
        """
        insert into plumegraph_events_ops.analysis_runs
        values ('old-run', 'region', date '2024-01-01', 'manifest',
                'contract', 'algorithm', 'success', 1,
                current_timestamp, current_timestamp, null);
        insert into plumegraph_events_ops.current_generations
        values ('region', date '2024-01-01', 'old-run', current_timestamp);
        insert into plumegraph_events_raw.episode_revisions
        values ('old-revision', 'old-plume', 'old-run', 'NO2',
                current_timestamp, current_timestamp, 1, 1, 1,
                1, 1, 0, 'likely', 'analysis_ready', true,
                'contract', 'algorithm', current_timestamp);
        """
    )
    from shapely.wkb import dumps

    conn.execute(
        """
        insert into plumegraph_events_raw.episode_geometries
        values ('old-revision', current_timestamp, ?, 0, 0, 1, 0)
        """,
        [dumps(box(0, 0, 1, 1))],
    )
    prior = analysis._prior_episodes(conn, "region", date(2024, 1, 1))
    assert prior[0].plume_id == "old-plume"
    previous_day = analysis._prior_episodes(conn, "region", date(2024, 1, 2))
    assert previous_day[0].plume_id == "old-plume"
    with pytest.raises(ValueError, match="Unknown PlumeGraph"):
        analysis._facilities(conn, "missing")
    with pytest.raises(ValueError, match="Unknown PlumeGraph"):
        analysis.run_analysis_partition("missing", date(2024, 1, 2), conn=conn)
    assert (
        conn.execute(
            """
        select status
        from plumegraph_events_ops.analysis_runs
        where analysis_region_id = 'missing'
        """
        ).fetchone()[0]
        == "failed"
    )
    conn.close()


def test_latest_calibration_decodes_optional_values():
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    assert analysis._latest_calibration(conn) == (None, None)
    conn.execute(
        """
        insert into plumegraph_events_ops.validation_runs
        values (
            'v1', 'manifest', 'benchmark', 'held_out',
            null, null, null, null, false, false, '{}', current_timestamp
        )
        """
    )
    assert analysis._latest_calibration(conn) == (None, None)
    conn.execute(
        """
        insert into plumegraph_events_ops.validation_runs
        values (
            'v2', 'manifest', 'benchmark', 'held_out',
            1, 1, 1, 0.05, true, true, '{"temperature": 2}',
            current_timestamp + interval '1 second'
        )
        """
    )
    assert analysis._latest_calibration(conn) == (2, 0.05)
    conn.close()


def test_current_meteorology_prefers_authoritative_source_revision():
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    now = datetime(2024, 1, 1, tzinfo=UTC)
    conn.execute(
        """
        insert into plumegraph_events_ops.source_requests (
            request_id, connector, source_version, analysis_region_id,
            window_start, window_end, request_json, request_contract_version,
            status, attempts, planned_at, updated_at
        )
        values (
            'request', 'hrrr', 'v1', 'region',
            timestamp '2024-01-01', timestamp '2024-01-02', '{}', 'contract',
            'success', 1, timestamp '2024-01-01', timestamp '2024-01-01'
        );
        insert into plumegraph_events_ops.source_snapshots
        values
            ('older', 'request', 'hrrr', 'source',
             timestamp '2024-01-01', 'older.json', 'a', null, 'schema', 1,
             timestamp '2024-01-03', '{}'),
            ('newer', 'request', 'hrrr', 'source',
             timestamp '2024-01-02', 'newer.json', 'b', null, 'schema', 1,
             timestamp '2024-01-01', '{}');
        insert into plumegraph_events_raw.meteorology_observations
        values
            ('older-row', 'region', timestamp '2024-01-01 01:00:00',
             35, -100, 1, 0, 2, 0, 1, 1000, 300, null, 'older',
             timestamp '2024-01-03'),
            ('newer-row', 'region', timestamp '2024-01-01 01:00:00',
             35, -100, 3, 0, 4, 0, 1, 1000, 300, null, 'newer',
             timestamp '2024-01-01');
        """
    )
    rows = analysis._current_meteorology(
        conn,
        "region",
        now,
        now + timedelta(days=1),
    )
    assert len(rows) == 1
    assert rows[0][1:5] == (3, 0, 4, 0)
    conn.close()


def test_pending_analysis_infers_dates_and_records_incomplete_meteorology(tmp_path):
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    cohort = write_synthetic_cohort(tmp_path / "cohort.json")
    seed_synthetic_sources(
        cohort_path=cohort,
        raw_data_dir=tmp_path / "raw",
        conn=conn,
    )
    conn.execute("delete from plumegraph_events_raw.meteorology_observations")
    metrics = analysis.run_pending_analysis(conn=conn)
    assert metrics.partitions_succeeded == 1
    assert metrics.episodes_inserted == 0
    conn.close()


def test_pending_analysis_commits_siblings_and_reports_failures(monkeypatch):
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    conn.execute(
        """
        insert into plumegraph_events_raw.analysis_regions
        values ('v1', 'a', '[]', 'x', 100, current_timestamp),
               ('v1', 'b', '[]', 'x', 100, current_timestamp)
        """
    )

    def run(region_id, partition_date, **_kwargs):
        if region_id == "b":
            raise RuntimeError("failed")
        return f"{region_id}:{partition_date}", 2

    monkeypatch.setattr(analysis, "run_analysis_partition", run)
    with pytest.raises(analysis.PlumeGraphAnalysisError) as caught:
        analysis.run_pending_analysis(
            partition_dates=[date(2024, 1, 1)],
            conn=conn,
        )
    assert caught.value.metrics.partitions_succeeded == 1
    assert caught.value.metrics.episodes_inserted == 2
    assert caught.value.failed_partitions == ("b:2024-01-01",)
    assert "successful siblings" in str(caught.value)
    conn.close()


def test_scientific_result_serializes_components_and_candidates():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    result = analysis._episode_scientific_result(
        [_component(now, box(0, 0, 1, 1))],
        [_Candidate(facility_id="f", score=1)],
    )
    assert result["components"][0]["pixels"] == ("p",)
    assert result["candidates"][0]["facility_id"] == "f"
    assert not analysis._track_is_analysis_ready([_component(now, box(0, 0, 1, 1))])
    assert analysis._track_is_analysis_ready(
        [
            _component(now, box(0, 0, 1, 1)),
            _component(now + timedelta(hours=1), box(0, 0, 1, 1)),
        ]
    )
