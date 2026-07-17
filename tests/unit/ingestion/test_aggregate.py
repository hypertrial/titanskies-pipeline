import numpy as np
import pyarrow as pa
import pytest

from titanskies_pipeline.geography.tempo_grid import GRID_VERSION
from titanskies_pipeline.ingestion.tempo.aggregate import (
    RegionWeights,
    aggregate_region_hour,
)
from titanskies_pipeline.ingestion.tempo.netcdf import (
    NetcdfGrid,
    accepted_quality_flags,
    quality_mask,
    weighted_stats,
)


def test_accepted_quality_flags():
    assert accepted_quality_flags("0,1") == {0, 1}


def test_quality_mask():
    quality = np.array([[0, 1], [2, 0]])
    mask = quality_mask(quality, [0, 1])
    assert mask.tolist() == [[True, True], [False, True]]


def test_weighted_stats_empty():
    stats = weighted_stats(np.array([]), np.array([]))
    assert stats["valid_pixel_count"] == 0


def test_aggregate_region_hour():
    grid = NetcdfGrid(
        no2=np.array([[1.0, 2.0], [3.0, 4.0]]),
        quality=np.array([[0, 0], [0, 0]]),
        lat=np.zeros((2, 2)),
        lon=np.zeros((2, 2)),
        observation_hour="2026-07-12 12:00:00",
    )
    weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US-CA-037",
                "overlap_weight": 1.0,
            }
        ]
    )
    aggregates = aggregate_region_hour(
        grids=[("g1", grid)],
        weights=weights,
        region_meta={"US-CA-037": ("US", "county")},
        geometry_version="test-v1",
        accepted_flags="0,1",
    )
    assert len(aggregates) == 1
    assert aggregates[0].no2_mean == pytest.approx(1.0)
    assert grid.latlon_at(1, 1) == (0.0, 0.0)


def test_region_weights_reject_missing_columns_and_out_of_bounds_cells():
    with pytest.raises(ValueError, match="missing columns"):
        RegionWeights.from_table(pa.table({"grid_row": [0]}), grid_version="tiny")
    with pytest.raises(ValueError, match="must be sorted"):
        RegionWeights.from_table(
            pa.table(
                {
                    "grid_row": [0, 0, 0],
                    "grid_col": [0, 1, 2],
                    "canonical_region_id": ["A", "B", "A"],
                    "overlap_weight": [1.0, 1.0, 1.0],
                }
            ),
            grid_version="tiny",
            sorted_input=True,
        )

    grid = NetcdfGrid(
        no2=np.ones((1, 1)),
        quality=np.zeros((1, 1), dtype=int),
        lat=np.array([35.0]),
        lon=np.array([-106.0]),
        observation_hour="2026-07-12 20:00:00",
    )
    weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 1,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            }
        ]
    )
    with pytest.raises(ValueError, match="exceed NetCDF grid dimensions"):
        aggregate_region_hour(
            grids=[("g-out-of-bounds", grid)],
            weights=weights,
            region_meta={"US": ("US", "country")},
            geometry_version="tiny",
            accepted_flags="0",
        )


def test_aggregate_region_hour_has_no_bbox_fallback():
    grid = NetcdfGrid(
        no2=np.array([[1.0, 2.0], [3.0, 4.0]]),
        quality=np.array([[0, 0], [0, 0]]),
        lat=np.array([34.0, 33.9]),
        lon=np.array([-118.2, -118.1]),
        observation_hour="2026-07-12 12:00:00",
    )
    aggregates = aggregate_region_hour(
        grids=[("g-bbox", grid)],
        weights=RegionWeights.from_rows([]),
        region_meta={"US-CA-037": ("US", "county"), "US-TX": ("US", "state")},
        geometry_version="test-v1",
        accepted_flags="0,1",
    )
    assert aggregates == []
    assert grid.latlon_at(0, 0) == (34.0, -118.2)


def test_area_coverage_weighted_statistics_and_zero_valid_visibility():
    grid = NetcdfGrid(
        no2=np.array([[10.0, 20.0]]),
        quality=np.array([[0, 2]]),
        lat=np.array([35.0]),
        lon=np.array([-106.0, -105.98]),
        observation_hour="2026-07-12 20:00:00",
    )
    rows = [
        {
            "grid_row": 0,
            "grid_col": 0,
            "canonical_region_id": "US",
            "overlap_weight": 1.0,
        },
        {
            "grid_row": 0,
            "grid_col": 1,
            "canonical_region_id": "US",
            "overlap_weight": 3.0,
        },
        {
            "grid_row": 0,
            "grid_col": 0,
            "canonical_region_id": "US-CA",
            "overlap_weight": 1.0,
        },
        {
            "grid_row": 0,
            "grid_col": 1,
            "canonical_region_id": "US-CA",
            "overlap_weight": 1.0,
        },
        {
            "grid_row": 0,
            "grid_col": 1,
            "canonical_region_id": "US-CA-037",
            "overlap_weight": 3.0,
        },
    ]
    aggregates = aggregate_region_hour(
        grids=[("g-area", grid)],
        weights=RegionWeights.from_rows(rows),
        region_meta={
            "US": ("US", "country"),
            "US-CA": ("US", "state"),
            "US-CA-037": ("US", "county"),
        },
        geometry_version="test-v1",
        accepted_flags="0",
    )
    by_id = {row.canonical_region_id: row for row in aggregates}
    assert by_id["US"].coverage_fraction == pytest.approx(0.25)
    assert by_id["US"].no2_mean == pytest.approx(10.0)
    assert by_id["US-CA"].coverage_fraction == pytest.approx(0.5)
    assert by_id["US-CA"].valid_pixel_count == 1
    assert by_id["US-CA"].total_pixel_count == 2
    zero = by_id["US-CA-037"]
    assert zero.coverage_fraction == 0.0
    assert zero.no2_mean is None
    assert zero.no2_median is None
    assert zero.no2_p90 is None
    assert zero.quality_flag_accepted is False


def test_unequal_overlap_area_weights_mean_median_and_p90():
    grid = NetcdfGrid(
        no2=np.array([[10.0, 20.0]]),
        quality=np.array([[0, 0]]),
        lat=np.array([35.0]),
        lon=np.array([-106.0, -105.98]),
        observation_hour="2026-07-12 20:00:00",
    )
    weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            },
            {
                "grid_row": 0,
                "grid_col": 1,
                "canonical_region_id": "US",
                "overlap_weight": 3.0,
            },
        ]
    )
    result = aggregate_region_hour(
        grids=[("g-weighted", grid)],
        weights=weights,
        region_meta={"US": ("US", "country")},
        geometry_version="test-v1",
        accepted_flags="0",
    )[0]
    assert result.no2_mean == pytest.approx(17.5)
    assert result.no2_median == pytest.approx(20.0)
    assert result.no2_p90 == pytest.approx(20.0)


def test_exact_pooled_hour_repeats_overlap_for_each_scan():
    weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            },
            {
                "grid_row": 0,
                "grid_col": 1,
                "canonical_region_id": "US",
                "overlap_weight": 3.0,
            },
        ]
    )
    first = NetcdfGrid(
        no2=np.array([[10.0, 20.0]]),
        quality=np.array([[0, 0]]),
        lat=np.array([35.0]),
        lon=np.array([-106.0, -105.98]),
        observation_hour="2026-07-12 20:00:00",
    )
    second = NetcdfGrid(
        no2=np.array([[30.0, 40.0]]),
        quality=np.array([[0, 2]]),
        lat=first.lat,
        lon=first.lon,
        observation_hour=first.observation_hour,
    )
    pooled = aggregate_region_hour(
        grids=[("g1", first), ("g2", second)],
        weights=weights,
        region_meta={"US": ("US", "country")},
        geometry_version="test-v1",
        accepted_flags="0",
    )[0]
    assert pooled.source_granule_count == 2
    assert pooled.valid_pixel_count == 3
    assert pooled.total_pixel_count == 4
    assert pooled.valid_area_km2 == pytest.approx(5.0)
    assert pooled.total_area_km2 == pytest.approx(8.0)
    assert pooled.no2_mean == pytest.approx(20.0)
    assert pooled.no2_median == pytest.approx(20.0)
    assert pooled.no2_p90 == pytest.approx(30.0)


def test_production_weights_reject_grid_contract_mismatch():
    grid = NetcdfGrid(
        no2=np.ones((1, 1)),
        quality=np.zeros((1, 1), dtype=int),
        lat=np.array([14.01]),
        lon=np.array([-167.99]),
        observation_hour="2026-07-12 20:00:00",
    )
    weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            }
        ],
        grid_version=GRID_VERSION,
    )
    with pytest.raises(ValueError, match="grid shape mismatch"):
        aggregate_region_hour(
            grids=[("g-bad-grid", grid)],
            weights=weights,
            region_meta={"US": ("US", "country")},
            geometry_version="v02",
            accepted_flags="0",
        )


def test_exact_hour_pooling_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="At least one"):
        aggregate_region_hour(
            grids=[],
            weights=RegionWeights.from_rows([]),
            region_meta={},
            geometry_version="test",
            accepted_flags="0",
        )

    base = NetcdfGrid(
        no2=np.ones((1, 1)),
        quality=np.zeros((1, 1), dtype=int),
        lat=np.array([14.01]),
        lon=np.array([-167.99]),
        observation_hour="2026-07-12 20:00:00",
    )
    other_hour = NetcdfGrid(
        no2=base.no2,
        quality=base.quality,
        lat=base.lat,
        lon=base.lon,
        observation_hour="2026-07-12 21:00:00",
    )
    with pytest.raises(ValueError, match="share one observation hour"):
        aggregate_region_hour(
            grids=[("one", base), ("two", other_hour)],
            weights=RegionWeights.from_rows([]),
            region_meta={},
            geometry_version="test",
            accepted_flags="0",
        )

    bad_shape = NetcdfGrid(
        no2=np.ones((1, 2)),
        quality=np.zeros((1, 1), dtype=int),
        lat=base.lat,
        lon=np.array([-167.99, -167.97]),
        observation_hour=base.observation_hour,
    )
    with pytest.raises(ValueError, match="shapes must match"):
        aggregate_region_hour(
            grids=[("bad", bad_shape)],
            weights=RegionWeights.from_rows([]),
            region_meta={},
            geometry_version="test",
            accepted_flags="0",
        )

    production_weights = RegionWeights.from_rows(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            }
        ],
        grid_version=GRID_VERSION,
    )
    with pytest.raises(ValueError, match="grid shape mismatch"):
        aggregate_region_hour(
            grids=[("production", base)],
            weights=production_weights,
            region_meta={"US": ("US", "country")},
            geometry_version="test",
            accepted_flags="0",
        )

    out_of_bounds = RegionWeights.from_rows(
        [
            {
                "grid_row": 1,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
            }
        ]
    )
    with pytest.raises(ValueError, match="exceed NetCDF grid dimensions"):
        aggregate_region_hour(
            grids=[("out", base)],
            weights=out_of_bounds,
            region_meta={"US": ("US", "country")},
            geometry_version="test",
            accepted_flags="0",
        )
