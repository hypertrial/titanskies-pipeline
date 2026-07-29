from __future__ import annotations

from types import SimpleNamespace

import netCDF4
import numpy as np
import pytest
from shapely.wkb import loads

from titanskies_pipeline.plumegraph import tempo_l2


def _write_l2(path, *, omit=(), nan_time=False):
    dataset = netCDF4.Dataset(path, "w", format="NETCDF4")
    dataset.createDimension("mirror_step", 2)
    dataset.createDimension("xtrack", 2)
    dataset.createDimension("corner", 4)
    if "mirror_step" not in omit:
        dataset.createVariable("mirror_step", "i4", ("mirror_step",))[:] = [10, 11]
    if "xtrack" not in omit:
        dataset.createVariable("xtrack", "i4", ("xtrack",))[:] = [20, 21]
    product = dataset.createGroup("product")
    geolocation = dataset.createGroup("geolocation")
    support = dataset.createGroup("support_data")
    for group in (product, geolocation, support):
        group.createDimension("mirror_step", 2)
        group.createDimension("xtrack", 2)
    geolocation.createDimension("corner", 4)
    if "time" not in omit:
        geolocation.createVariable("time", "f8", ("mirror_step",))[:] = [
            np.nan if nan_time else 1_400_000_018,
            1_400_000_078,
        ]
    latitude = geolocation.createVariable(
        "latitude",
        "f8",
        ("mirror_step", "xtrack"),
    )
    longitude = geolocation.createVariable(
        "longitude",
        "f8",
        ("mirror_step", "xtrack"),
    )
    latitude[:] = [[35, 35], [35.1, np.nan]]
    longitude[:] = [[-100, -99.9], [-100, -99.9]]
    lat_bounds = geolocation.createVariable(
        "latitude_bounds",
        "f8",
        ("mirror_step", "xtrack", "corner"),
    )
    lon_bounds = geolocation.createVariable(
        "longitude_bounds",
        "f8",
        ("mirror_step", "xtrack", "corner"),
    )
    lat_bounds[:] = [
        [[34.99, 34.99, 35.01, 35.01], [35, 35, 35, 35]],
        [[35.09, 35.09, 35.11, 35.11], [35.09, 35.09, 35.11, 35.11]],
    ]
    lon_bounds[:] = [
        [[-100.01, -99.99, -99.99, -100.01], [-99.9, -99.9, -99.9, -99.9]],
        [[np.nan, -99.99, -99.99, -100.01], [-99.91, -99.89, -99.89, -99.91]],
    ]
    for name, values in (
        ("vertical_column_troposphere", [[-1, 2], [3, 4]]),
        ("vertical_column_troposphere_uncertainty", [[1, 1], [1, 1]]),
        ("main_data_quality_flag", [[0, 1], [0, 0]]),
    ):
        if name not in omit:
            variable = product.createVariable(
                name,
                "f8",
                ("mirror_step", "xtrack"),
            )
            variable[:] = values
            if name == "vertical_column_troposphere":
                variable.units = "molecules cm-2"
    support.createVariable(
        "eff_cloud_fraction",
        "f8",
        ("mirror_step", "xtrack"),
    )[:] = 0.01
    support.createVariable(
        "snow_ice_flag",
        "f8",
        ("mirror_step", "xtrack"),
    )[:] = 0
    support.createVariable(
        "amf_diagnostic_flag",
        "i4",
        ("mirror_step", "xtrack"),
    )[:] = 0
    support.createVariable(
        "surface_pressure",
        "f8",
        ("mirror_step", "xtrack"),
    )[:] = [[98000, 980], [980, 980]]
    geolocation.createVariable(
        "solar_zenith_angle",
        "f8",
        ("mirror_step", "xtrack"),
    )[:] = 30
    geolocation.createVariable(
        "satellite_zenith_angle",
        "f8",
        ("mirror_step", "xtrack"),
    )[:] = 10
    dataset.close()
    return path


def test_value_helpers_cover_shapes_and_missing_values():
    one = SimpleNamespace(values=np.array([1, np.nan]))
    two = SimpleNamespace(values=np.array([[2]]))
    three = SimpleNamespace(values=np.zeros((1, 1, 1)))
    assert tempo_l2._value(None, 0, 0) is None
    assert tempo_l2._value(one, 0, 99) == 1
    assert tempo_l2._value(one, 1, 0) is None
    assert tempo_l2._value(two, 0, 0) == 2
    assert tempo_l2._value(two, 3, 0) is None
    assert tempo_l2._value(three, 0, 0) is None
    assert tempo_l2._first({"b": 2}, ("a", "b")) == 2
    assert tempo_l2._first({}, ("a",)) is None


def test_read_l2_preserves_negative_values_geometry_and_optional_fields(tmp_path):
    records = tempo_l2.read_tempo_l2_netcdf(_write_l2(tmp_path / "tempo.nc"))
    assert len(records) == 3
    first = records[0]
    assert first["mirror_step"] == 10
    assert first["xtrack"] == 20
    assert first["no2_vertical_column"] == -1
    assert first["surface_pressure_hpa"] == 980
    assert first["cloud_fraction"] == 0.01
    assert first["viewing_zenith_angle"] == 10
    assert first["no2_unit"] == "molecules cm-2"
    assert loads(first["geometry_wkb"]).is_valid
    assert first["pixel_area_km2"] > 0
    assert records[1]["geometry_wkb"] is None
    assert records[-1]["geometry_wkb"] is None


def test_read_l2_rejects_schema_and_skips_missing_time(tmp_path):
    with pytest.raises(ValueError, match="missing required variables"):
        tempo_l2.read_tempo_l2_netcdf(
            _write_l2(
                tmp_path / "missing.nc",
                omit=("vertical_column_troposphere_uncertainty",),
            )
        )
    records = tempo_l2.read_tempo_l2_netcdf(
        _write_l2(tmp_path / "nan-time.nc", nan_time=True)
    )
    assert all(record["mirror_step"] == 11 for record in records)
