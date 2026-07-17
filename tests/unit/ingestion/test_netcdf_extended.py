from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from titanskies_pipeline.geography.tempo_grid import (
    TEMPO_GRID,
    col_longitudes,
    row_latitudes,
)
from titanskies_pipeline.ingestion.tempo.netcdf import (
    NO2_VAR,
    QUALITY_VAR,
    accepted_quality_flags,
    extract_grid,
    validate_netcdf,
    weighted_stats,
)

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "netcdf" / "tempo_no2_sample.nc"
)


@pytest.fixture(scope="module", autouse=True)
def ensure_fixture():
    if not FIXTURE.exists():
        from scripts.generate_netcdf_fixtures import write_fixture

        write_fixture(FIXTURE)


def _write_minimal_netcdf(
    path: Path,
    *,
    include_time: bool = True,
    no2_dims: tuple[str, ...] = ("time", "y", "x"),
    omit_vars: set[str] | None = None,
) -> None:
    omit_vars = omit_vars or set()
    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray]] = {}
    if "latitude" not in omit_vars:
        data_vars["latitude"] = (("y", "x"), np.zeros((2, 2), dtype=np.float32))
    if "longitude" not in omit_vars:
        data_vars["longitude"] = (("y", "x"), np.zeros((2, 2), dtype=np.float32))
    if NO2_VAR not in omit_vars:
        shape = tuple(1 if dim == "time" else 2 for dim in no2_dims)
        data_vars[NO2_VAR] = (no2_dims, np.ones(shape, dtype=np.float32))
    if QUALITY_VAR not in omit_vars:
        shape = tuple(1 if dim == "time" else 2 for dim in no2_dims)
        data_vars[QUALITY_VAR] = (no2_dims, np.zeros(shape, dtype=np.int16))
    if include_time:
        data_vars["time"] = ("time", np.array([np.datetime64("2026-07-12T12:00:00")]))
    xr.Dataset(data_vars).to_netcdf(path)


def test_validate_netcdf_missing_variable(tmp_path):
    path = tmp_path / "missing_var.nc"
    _write_minimal_netcdf(path, omit_vars={NO2_VAR})
    with pytest.raises(ValueError, match=f"Missing required variable: {NO2_VAR}"):
        validate_netcdf(path)


def test_validate_netcdf_bad_dimensions(tmp_path):
    path = tmp_path / "bad_dims.nc"
    _write_minimal_netcdf(path, no2_dims=("time",), include_time=True)
    with pytest.raises(ValueError, match="two spatial dimensions"):
        validate_netcdf(path)


def test_validate_netcdf_rejects_multiple_time_slices(tmp_path):
    path = tmp_path / "multiple_times.nc"
    dataset = xr.Dataset(
        {
            "latitude": (("y", "x"), np.zeros((2, 2))),
            "longitude": (("y", "x"), np.zeros((2, 2))),
            NO2_VAR: (("time", "y", "x"), np.ones((2, 2, 2))),
            QUALITY_VAR: (("time", "y", "x"), np.zeros((2, 2, 2))),
        }
    )
    dataset.to_netcdf(path)
    with pytest.raises(ValueError, match="exactly one time slice"):
        validate_netcdf(path)


def test_validate_netcdf_rejects_mismatched_science_shapes(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "mismatched.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("short", 1)
        dataset.createVariable("latitude", "f4", ("y",))
        dataset.createVariable("longitude", "f4", ("x",))
        dataset.createVariable(NO2_VAR, "f4", ("y", "x"))
        dataset.createVariable(QUALITY_VAR, "i2", ("y", "short"))
    with pytest.raises(ValueError, match="equal two-dimensional shapes"):
        validate_netcdf(path)


def test_production_validation_requires_exact_native_coordinates(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "bad-production-coordinates.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("latitude", TEMPO_GRID.rows)
        dataset.createDimension("longitude", TEMPO_GRID.cols)
        lat = dataset.createVariable("latitude", "f8", ("latitude",))
        lon = dataset.createVariable("longitude", "f8", ("longitude",))
        dataset.createVariable(NO2_VAR, "f4", ("latitude", "longitude"))
        dataset.createVariable(QUALITY_VAR, "i2", ("latitude", "longitude"))
        lat[:] = row_latitudes()
        lon[:] = col_longitudes()
        lat[0] = 0.0
    with pytest.raises(ValueError, match="coordinates do not match"):
        validate_netcdf(path, production=True)


def test_production_validation_rejects_shape_and_two_dimensional_coordinates(
    tmp_path, monkeypatch
):
    small = tmp_path / "small.nc"
    _write_minimal_netcdf(small)
    with pytest.raises(ValueError, match="grid shape mismatch"):
        validate_netcdf(small, production=True)

    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.netcdf.TEMPO_GRID",
        type("Grid", (), {"rows": 2, "cols": 2})(),
    )
    with pytest.raises(ValueError, match="one-dimensional"):
        validate_netcdf(small, production=True)


def test_extract_grid_without_time_is_rejected(tmp_path):
    path = tmp_path / "custom_hour.nc"
    _write_minimal_netcdf(path, include_time=False, no2_dims=("y", "x"))
    with pytest.raises(ValueError, match="time coordinate is missing"):
        extract_grid(path)


def test_extract_grid_decodes_cf_time_from_fixture():
    grid = extract_grid(FIXTURE)
    assert grid.observation_hour.endswith(":00:00")


def test_observation_time_decode_failure_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "fallback_hour.nc"
    _write_minimal_netcdf(path, include_time=True, no2_dims=("time", "y", "x"))
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.netcdf.xr.coding.times.decode_cf_datetime",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad time")),
    )
    with pytest.raises(ValueError, match="could not be decoded"):
        extract_grid(path)


def test_observation_time_is_normalized_to_naive_utc(tmp_path, monkeypatch):
    path = tmp_path / "aware-hour.nc"
    _write_minimal_netcdf(path)
    monkeypatch.setattr(
        "titanskies_pipeline.ingestion.tempo.netcdf.xr.coding.times.decode_cf_datetime",
        lambda *_a, **_k: np.array(["2026-07-12T14:30:00+02:00"]),
    )
    grid = extract_grid(path)
    assert grid.observation_time == datetime(2026, 7, 12, 12, 30)


def test_accepted_quality_flags_pipe_separator():
    assert accepted_quality_flags("0|1|2") == {0, 1, 2}


def test_weighted_stats_empty_and_zero_weight():
    empty = weighted_stats(np.array([]), np.array([]))
    assert empty["valid_pixel_count"] == 0
    assert np.isnan(empty["mean"])

    zero_weight = weighted_stats(np.array([1.0, 2.0]), np.array([0.0, 0.0]))
    assert zero_weight["valid_pixel_count"] == 2
    assert np.isnan(zero_weight["mean"])


def test_extract_grid_from_fixture():
    grid = extract_grid(FIXTURE)
    assert grid.no2.shape == (2, 2)
    assert grid.observation_hour


def test_extract_grid_product_group_and_1d_coordinates(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "tempo_product_group.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("latitude", 2)
        dataset.createDimension("longitude", 2)
        dataset.createDimension("time", 1)
        lat = dataset.createVariable("latitude", "f4", ("latitude",))
        lon = dataset.createVariable("longitude", "f4", ("longitude",))
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        lat[:] = [34.0, 33.9]
        lon[:] = [-118.2, -118.1]
        time_var[:] = [488_000.0]
        product = dataset.createGroup("product")
        no2 = product.createVariable(
            "vertical_column_troposphere", "f4", ("time", "latitude", "longitude")
        )
        quality = product.createVariable(
            "main_data_quality_flag", "i2", ("time", "latitude", "longitude")
        )
        no2[0, :, :] = [[1.0, 2.0], [3.0, 4.0]]
        quality[0, :, :] = [[0, 0], [0, 0]]

    grid = extract_grid(path)
    assert grid.no2.shape == (2, 2)
    assert grid.lat.shape == (2,)
    assert grid.lon.shape == (2,)
    assert grid.latlon_at(1, 0) == pytest.approx((33.9, -118.2))


def test_validate_netcdf_missing_coordinates(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "missing_coords.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var[:] = [488_000.0]
        product = dataset.createGroup("product")
        product.createVariable("vertical_column_troposphere", "f4", ("y", "x"))
        product.createVariable("main_data_quality_flag", "i2", ("y", "x"))

    with pytest.raises(ValueError, match="Missing required coordinate"):
        validate_netcdf(path)


def test_observation_time_without_units_is_rejected(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "time_without_units.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        dataset.createVariable("latitude", "f4", ("y", "x"))
        dataset.createVariable("longitude", "f4", ("y", "x"))
        dataset.createVariable("vertical_column_troposphere", "f4", ("time", "y", "x"))
        dataset.createVariable("main_data_quality_flag", "i2", ("time", "y", "x"))
        dataset.createVariable("time", "f8", ("time",))[:] = [1.0]

    with pytest.raises(ValueError, match="has no units"):
        extract_grid(path)


def test_validate_netcdf_reads_coordinates_from_product_group(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "coords_in_product.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var[:] = [488_000.0]
        product = dataset.createGroup("product")
        product.createVariable("latitude", "f4", ("y", "x"))
        product.createVariable("longitude", "f4", ("y", "x"))
        product.createVariable("vertical_column_troposphere", "f4", ("y", "x"))
        product.createVariable("main_data_quality_flag", "i2", ("y", "x"))

    validate_netcdf(path)


def test_extract_grid_reads_coordinates_from_product_group(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "coords_only_in_product.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var[:] = [488_000.0]
        product = dataset.createGroup("product")
        lat = product.createVariable("latitude", "f4", ("y", "x"))
        lon = product.createVariable("longitude", "f4", ("y", "x"))
        no2 = product.createVariable("vertical_column_troposphere", "f4", ("y", "x"))
        quality = product.createVariable("main_data_quality_flag", "i2", ("y", "x"))
        lat[:, :] = [[34.0, 34.1], [33.9, 34.2]]
        lon[:, :] = [[-118.2, -118.1], [-118.3, -118.0]]
        no2[:, :] = [[1.0, 2.0], [3.0, 4.0]]
        quality[:, :] = [[0, 0], [0, 0]]

    grid = extract_grid(path)
    assert grid.no2.shape == (2, 2)
    assert grid.lat.shape == (2, 2)


def test_extract_grid_reads_coordinates_from_mixed_locations(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "mixed_coordinates.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var[:] = [488_000.0]
        dataset.createVariable("latitude", "f4", ("y", "x"))[:] = 35.0
        product = dataset.createGroup("product")
        product.createVariable("longitude", "f4", ("y", "x"))[:] = -106.0
        product.createVariable(NO2_VAR, "f4", ("y", "x"))[:] = 1.0
        product.createVariable(QUALITY_VAR, "i2", ("y", "x"))[:] = 0

    grid = extract_grid(path)
    assert grid.lat.shape == grid.lon.shape == (2, 2)


def test_extract_grid_preserves_independent_no2_and_quality_masks(tmp_path):
    from netCDF4 import Dataset

    path = tmp_path / "independent_masks.nc"
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 1)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var[:] = [488_000.0]
        dataset.createVariable("latitude", "f4", ("y", "x"))[:] = [[35.0, 35.0]]
        dataset.createVariable("longitude", "f4", ("y", "x"))[:] = [[-106.0, -105.98]]
        no2 = dataset.createVariable(NO2_VAR, "f4", ("y", "x"), fill_value=-1.0e30)
        quality = dataset.createVariable(
            QUALITY_VAR, "i2", ("y", "x"), fill_value=-9999
        )
        no2[:] = np.ma.array([[1.0, 2.0]], mask=[[True, False]])
        quality[:] = np.ma.array([[0, 1]], mask=[[False, True]])

    grid = extract_grid(path)
    assert np.isnan(grid.no2[0, 0])
    assert grid.no2[0, 1] == pytest.approx(2.0)
    assert grid.quality.tolist() == [[0, -9999]]
