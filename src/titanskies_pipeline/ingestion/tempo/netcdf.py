"""NetCDF validation and NO2 extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import xarray as xr
from netCDF4 import Dataset

from titanskies_pipeline.geography.tempo_grid import (
    TEMPO_GRID,
    validate_grid_coordinates,
)

NO2_VAR = "vertical_column_troposphere"
QUALITY_VAR = "main_data_quality_flag"
LAT_VAR = "latitude"
LON_VAR = "longitude"
PRODUCT_GROUP = "product"


@dataclass(frozen=True)
class NetcdfGrid:
    no2: np.ndarray
    quality: np.ndarray
    lat: np.ndarray
    lon: np.ndarray
    observation_hour: str
    observation_time: datetime | None = None

    def latlon_at(self, row: int, col: int) -> tuple[float, float]:
        if self.lat.ndim == 1 and self.lon.ndim == 1:
            return float(self.lat[row]), float(self.lon[col])
        return float(self.lat[row, col]), float(self.lon[row, col])


def accepted_quality_flags(contract_flags: str) -> set[int]:
    normalized = contract_flags.replace("|", ",")
    return {int(part.strip()) for part in normalized.split(",") if part.strip()}


def _product_handle(dataset: Dataset) -> Dataset:
    if PRODUCT_GROUP in dataset.groups:
        return dataset.groups[PRODUCT_GROUP]
    return dataset


def _science_shape(variable) -> tuple[int, int]:
    shape = tuple(variable.shape)
    dimensions = tuple(variable.dimensions)
    if dimensions and dimensions[0] == "time":
        if shape[0] != 1:
            raise ValueError("Science arrays must contain exactly one time slice")
        shape = shape[1:]
    if "time" in dimensions[1:] or len(shape) != 2:
        raise ValueError("Science arrays must have exactly two spatial dimensions")
    return shape


def validate_netcdf(path: Path, *, production: bool = False) -> None:
    with Dataset(path) as dataset:
        product = _product_handle(dataset)
        for var in (NO2_VAR, QUALITY_VAR):
            if var not in product.variables:
                raise ValueError(f"Missing required variable: {var}")
        no2_shape = _science_shape(product.variables[NO2_VAR])
        quality_shape = _science_shape(product.variables[QUALITY_VAR])
        if len(no2_shape) != 2 or no2_shape != quality_shape:
            raise ValueError(
                "NO2 and quality science arrays must have equal two-dimensional shapes"
            )
        for var in (LAT_VAR, LON_VAR):
            if var not in dataset.variables and var not in product.variables:
                raise ValueError(f"Missing required coordinate: {var}")
        if production:
            expected = (TEMPO_GRID.rows, TEMPO_GRID.cols)
            if no2_shape != expected:
                raise ValueError(
                    f"Production TEMPO grid shape mismatch: expected {expected}, got {no2_shape}"
                )
            lat_var = (
                dataset.variables[LAT_VAR]
                if LAT_VAR in dataset.variables
                else product.variables[LAT_VAR]
            )
            lon_var = (
                dataset.variables[LON_VAR]
                if LON_VAR in dataset.variables
                else product.variables[LON_VAR]
            )
            if lat_var.ndim != 1 or lon_var.ndim != 1:
                raise ValueError(
                    "Production TEMPO coordinates must be native one-dimensional V02 arrays"
                )
            validate_grid_coordinates(
                np.asarray(lat_var[:], dtype=float), np.asarray(lon_var[:], dtype=float)
            )


def _read_observation_time(path: Path) -> datetime:
    with Dataset(path) as dataset:
        time_var = dataset.variables.get("time")
        if time_var is None or time_var.size == 0:
            raise ValueError("NetCDF time coordinate is missing")
        units = getattr(time_var, "units", None)
        if not units:
            raise ValueError("NetCDF time coordinate has no units")
        calendar = getattr(time_var, "calendar", "standard")
        try:
            decoded = xr.coding.times.decode_cf_datetime(
                time_var[:1],
                units,
                calendar=calendar,
            )
            text = str(decoded[0])[:26].replace(" ", "T")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except (TypeError, ValueError) as exc:
            raise ValueError("NetCDF observation time could not be decoded") from exc


def extract_grid(path: Path, *, production: bool = False) -> NetcdfGrid:
    validate_netcdf(path, production=production)
    observation_time = _read_observation_time(path)
    observation_hour = observation_time.replace(minute=0, second=0, microsecond=0)
    with Dataset(path) as dataset:
        product = _product_handle(dataset)
        no2_values = product.variables[NO2_VAR][:]
        quality_values = product.variables[QUALITY_VAR][:]
        no2 = np.asarray(np.ma.filled(no2_values, np.nan), dtype=float)
        quality = np.asarray(np.ma.filled(quality_values, -9999), dtype=int)
        if no2.ndim == 3:
            no2 = no2[0]
        if quality.ndim == 3:
            quality = quality[0]

        lat_source = dataset if LAT_VAR in dataset.variables else product
        lon_source = dataset if LON_VAR in dataset.variables else product
        lat = np.asarray(lat_source.variables[LAT_VAR][:], dtype=float)
        lon = np.asarray(lon_source.variables[LON_VAR][:], dtype=float)

    return NetcdfGrid(
        no2=no2,
        quality=quality,
        lat=lat,
        lon=lon,
        observation_hour=observation_hour.isoformat(sep=" "),
        observation_time=observation_time,
    )


def quality_mask(quality: np.ndarray, accepted: Iterable[int]) -> np.ndarray:
    accepted_set = set(accepted)
    return np.isin(quality, list(accepted_set))


def weighted_stats(values: np.ndarray, weights: np.ndarray) -> dict[str, float | int]:
    if values.size == 0:
        return {
            "mean": float("nan"),
            "median": float("nan"),
            "p90": float("nan"),
            "valid_pixel_count": 0,
        }
    total_weight = float(weights.sum())
    if total_weight <= 0:
        return {
            "mean": float("nan"),
            "median": float("nan"),
            "p90": float("nan"),
            "valid_pixel_count": int(values.size),
        }
    order = np.argsort(values)
    sorted_values = values[order]
    sorted_weights = weights[order]
    cumulative = np.cumsum(sorted_weights)
    median_idx = int(np.searchsorted(cumulative, total_weight / 2.0))
    median_idx = min(median_idx, sorted_values.size - 1)
    p90_idx = int(np.searchsorted(cumulative, total_weight * 0.9))
    p90_idx = min(p90_idx, sorted_values.size - 1)
    mean = float(np.average(values, weights=weights))
    return {
        "mean": mean,
        "median": float(sorted_values[median_idx]),
        "p90": float(sorted_values[p90_idx]),
        "valid_pixel_count": int(values.size),
    }


__all__ = [
    "LAT_VAR",
    "LON_VAR",
    "NO2_VAR",
    "QUALITY_VAR",
    "NetcdfGrid",
    "accepted_quality_flags",
    "extract_grid",
    "quality_mask",
    "validate_netcdf",
    "weighted_stats",
]
