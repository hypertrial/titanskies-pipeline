#!/usr/bin/env python3
"""Generate synthetic TEMPO-shaped NetCDF fixtures for tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from netCDF4 import Dataset

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "tests" / "fixtures" / "netcdf"


def write_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with Dataset(path, "w") as dataset:
        dataset.createDimension("y", 2)
        dataset.createDimension("x", 2)
        dataset.createDimension("time", 1)

        lat = dataset.createVariable("latitude", "f4", ("y", "x"))
        lon = dataset.createVariable("longitude", "f4", ("y", "x"))
        no2 = dataset.createVariable(
            "vertical_column_troposphere", "f4", ("time", "y", "x")
        )
        quality = dataset.createVariable(
            "main_data_quality_flag", "i2", ("time", "y", "x")
        )
        time_var = dataset.createVariable("time", "f8", ("time",))
        time_var.units = "hours since 1970-01-01 00:00:00"
        time_var.calendar = "standard"

        lat[:, :] = np.array([[34.0, 34.1], [33.9, 34.2]], dtype=np.float32)
        lon[:, :] = np.array([[-118.2, -118.1], [-118.3, -118.0]], dtype=np.float32)
        no2[0, :, :] = np.array([[2.5e15, 3.1e15], [1.8e15, np.nan]], dtype=np.float32)
        quality[0, :, :] = np.array([[0, 1], [0, 2]], dtype=np.int16)
        epoch = datetime(2026, 7, 12, 12, tzinfo=timezone.utc).timestamp() / 3600.0
        time_var[:] = [epoch]


if __name__ == "__main__":
    write_fixture(OUT_DIR / "tempo_no2_sample.nc")
    print(f"Wrote {OUT_DIR / 'tempo_no2_sample.nc'}")
