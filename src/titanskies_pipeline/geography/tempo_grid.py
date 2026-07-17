"""NASA TEMPO NO2 L3 NRT V02 grid contract."""

from __future__ import annotations

from dataclasses import dataclass
from math import pi

import numpy as np

GRID_VERSION = "tempo-no2-l3-nrt-v02"
GRID_ROWS = 2_950
GRID_COLS = 7_750
GRID_STEP_DEGREES = 0.02
LATITUDE_START = 14.01
LONGITUDE_START = -167.99
EARTH_RADIUS_KM = 6_371.0088


@dataclass(frozen=True)
class TempoGridSpec:
    version: str = GRID_VERSION
    rows: int = GRID_ROWS
    cols: int = GRID_COLS
    latitude_start: float = LATITUDE_START
    longitude_start: float = LONGITUDE_START
    step_degrees: float = GRID_STEP_DEGREES

    @property
    def latitude_end(self) -> float:
        return self.latitude_start + (self.rows - 1) * self.step_degrees

    @property
    def longitude_end(self) -> float:
        return self.longitude_start + (self.cols - 1) * self.step_degrees


TEMPO_GRID = TempoGridSpec()


def validate_grid_coordinates(
    lat: np.ndarray,
    lon: np.ndarray,
    *,
    spec: TempoGridSpec = TEMPO_GRID,
) -> None:
    if lat.ndim != 1 or lon.ndim != 1:
        raise ValueError("TEMPO V02 coordinates must be one-dimensional")
    if lat.shape != (spec.rows,) or lon.shape != (spec.cols,):
        raise ValueError(
            "TEMPO grid shape mismatch: "
            f"expected {(spec.rows, spec.cols)}, got {(lat.size, lon.size)}"
        )
    expected_lat = np.linspace(
        spec.latitude_start, spec.latitude_end, spec.rows, dtype=float
    )
    expected_lon = np.linspace(
        spec.longitude_start, spec.longitude_end, spec.cols, dtype=float
    )
    if not np.allclose(lat, expected_lat, atol=1e-5, rtol=0) or not np.allclose(
        lon, expected_lon, atol=1e-5, rtol=0
    ):
        raise ValueError(f"TEMPO coordinates do not match {spec.version}")


def cell_area_km2(latitude: np.ndarray | float, *, step: float = 0.02) -> np.ndarray:
    centers = np.asarray(latitude, dtype=float)
    half = step / 2.0
    lat_lo = np.deg2rad(centers - half)
    lat_hi = np.deg2rad(centers + half)
    lon_width = step * pi / 180.0
    return (EARTH_RADIUS_KM**2) * lon_width * (np.sin(lat_hi) - np.sin(lat_lo))


def row_latitudes(spec: TempoGridSpec = TEMPO_GRID) -> np.ndarray:
    return spec.latitude_start + np.arange(spec.rows, dtype=float) * spec.step_degrees


def col_longitudes(spec: TempoGridSpec = TEMPO_GRID) -> np.ndarray:
    return spec.longitude_start + np.arange(spec.cols, dtype=float) * spec.step_degrees


__all__ = [
    "GRID_VERSION",
    "TEMPO_GRID",
    "TempoGridSpec",
    "cell_area_km2",
    "col_longitudes",
    "row_latitudes",
    "validate_grid_coordinates",
]
