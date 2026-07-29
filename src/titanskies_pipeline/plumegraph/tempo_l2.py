"""TEMPO Level 2 V04 NetCDF normalization for PlumeGraph."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Sequence


def _first(dataset, names: Sequence[str]):
    for name in names:
        if name in dataset:
            return dataset[name]
    return None


def _finite_or_none(value: object) -> float | None:
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _value(variable, mirror_index: int, xtrack_index: int) -> float | None:
    if variable is None:
        return None
    values = variable.values
    try:
        if values.ndim == 2:
            raw = values[mirror_index, xtrack_index]
        elif values.ndim == 1:
            raw = values[mirror_index]
        else:
            return None
    except IndexError:
        return None
    return _finite_or_none(raw)


def read_tempo_l2_netcdf(path: Path) -> list[dict[str, object]]:
    import xarray as xr
    from pyproj import Geod
    from shapely.geometry import Polygon
    from shapely.wkb import dumps

    root = xr.open_dataset(path, decode_times=False, mask_and_scale=True)
    product = xr.open_dataset(
        path,
        group="product",
        decode_times=False,
        mask_and_scale=True,
    )
    geolocation = xr.open_dataset(
        path,
        group="geolocation",
        decode_times=False,
        mask_and_scale=True,
    )
    support = xr.open_dataset(
        path,
        group="support_data",
        decode_times=False,
        mask_and_scale=True,
    )
    try:
        mirror_steps = _first(root, ("mirror_step",))
        xtracks = _first(root, ("xtrack",))
        times = _first(geolocation, ("time",))
        latitudes = _first(geolocation, ("latitude",))
        longitudes = _first(geolocation, ("longitude",))
        latitude_bounds = _first(geolocation, ("latitude_bounds",))
        longitude_bounds = _first(geolocation, ("longitude_bounds",))
        vcd = _first(product, ("vertical_column_troposphere",))
        uncertainty = _first(
            product,
            ("vertical_column_troposphere_uncertainty",),
        )
        quality = _first(product, ("main_data_quality_flag",))
        required = {
            "mirror_step": mirror_steps,
            "xtrack": xtracks,
            "time": times,
            "latitude": latitudes,
            "longitude": longitudes,
            "latitude_bounds": latitude_bounds,
            "longitude_bounds": longitude_bounds,
            "vertical_column_troposphere": vcd,
            "vertical_column_troposphere_uncertainty": uncertainty,
            "main_data_quality_flag": quality,
        }
        missing = [name for name, variable in required.items() if variable is None]
        if missing:
            raise ValueError(
                "TEMPO L2 NetCDF missing required variables: "
                + ", ".join(sorted(missing))
            )
        cloud_fraction = _first(
            support,
            (
                "effective_cloud_fraction",
                "eff_cloud_fraction",
                "cloud_fraction",
            ),
        )
        snow_ice = _first(
            support,
            ("snow_ice_fraction", "snow_ice_flag"),
        )
        amf_flag = _first(support, ("amf_diagnostic_flag",))
        surface_pressure = _first(support, ("surface_pressure",))
        solar_zenith = _first(geolocation, ("solar_zenith_angle",))
        viewing_zenith = _first(
            geolocation,
            ("viewing_zenith_angle", "satellite_zenith_angle"),
        )
        geod = Geod(ellps="WGS84")
        records: list[dict[str, object]] = []
        for mirror_index, mirror_step in enumerate(mirror_steps.values):
            original_time = _value(times, mirror_index, 0)
            if original_time is None:
                continue
            for xtrack_index, xtrack in enumerate(xtracks.values):
                latitude = _value(latitudes, mirror_index, xtrack_index)
                longitude = _value(longitudes, mirror_index, xtrack_index)
                if latitude is None or longitude is None:
                    continue
                corners = [
                    (
                        _finite_or_none(
                            longitude_bounds.values[
                                mirror_index,
                                xtrack_index,
                                corner,
                            ]
                        ),
                        _finite_or_none(
                            latitude_bounds.values[
                                mirror_index,
                                xtrack_index,
                                corner,
                            ]
                        ),
                    )
                    for corner in range(4)
                ]
                geometry_wkb = None
                area_km2 = None
                if all(
                    corner[0] is not None and corner[1] is not None
                    for corner in corners
                ):
                    polygon = Polygon(corners)
                    if polygon.is_valid and not polygon.is_empty and polygon.area > 0:
                        geometry_wkb = dumps(
                            polygon,
                            hex=False,
                            big_endian=False,
                        )
                        area_m2, _ = geod.geometry_area_perimeter(polygon)
                        area_km2 = abs(area_m2) / 1_000_000
                pressure = _value(surface_pressure, mirror_index, xtrack_index)
                if pressure is not None and pressure > 2000:
                    pressure /= 100
                records.append(
                    {
                        "mirror_step": int(mirror_step),
                        "xtrack": int(xtrack),
                        "time_gps_seconds": original_time,
                        "latitude": latitude,
                        "longitude": longitude,
                        "geometry_wkb": geometry_wkb,
                        "pixel_area_km2": area_km2,
                        "no2_vertical_column": _value(
                            vcd,
                            mirror_index,
                            xtrack_index,
                        ),
                        "no2_uncertainty": _value(
                            uncertainty,
                            mirror_index,
                            xtrack_index,
                        ),
                        "quality_flag": _value(
                            quality,
                            mirror_index,
                            xtrack_index,
                        ),
                        "cloud_fraction": _value(
                            cloud_fraction,
                            mirror_index,
                            xtrack_index,
                        ),
                        "snow_ice_fraction": _value(
                            snow_ice,
                            mirror_index,
                            xtrack_index,
                        ),
                        "amf_diagnostic_flag": _value(
                            amf_flag,
                            mirror_index,
                            xtrack_index,
                        ),
                        "solar_zenith_angle": _value(
                            solar_zenith,
                            mirror_index,
                            xtrack_index,
                        ),
                        "viewing_zenith_angle": _value(
                            viewing_zenith,
                            mirror_index,
                            xtrack_index,
                        ),
                        "surface_pressure_hpa": pressure,
                        "collection_version": "V04",
                        "no2_unit": str(getattr(vcd, "units", None) or "molecules/cm2"),
                    }
                )
        return records
    finally:
        root.close()
        product.close()
        geolocation.close()
        support.close()


__all__ = ["read_tempo_l2_netcdf"]
