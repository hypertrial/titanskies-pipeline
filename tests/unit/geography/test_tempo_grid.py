from __future__ import annotations

import numpy as np
import pytest

from titanskies_pipeline.geography.tempo_grid import (
    TempoGridSpec,
    cell_area_km2,
    col_longitudes,
    row_latitudes,
    validate_grid_coordinates,
)

TINY = TempoGridSpec(
    version="tiny", rows=2, cols=3, latitude_start=10.0, longitude_start=-20.0
)


def test_grid_spec_centers_and_cell_area():
    assert TINY.latitude_end == 10.02
    assert TINY.longitude_end == -19.96
    assert row_latitudes(TINY).tolist() == pytest.approx([10.0, 10.02])
    assert col_longitudes(TINY).tolist() == pytest.approx([-20.0, -19.98, -19.96])
    areas = cell_area_km2(np.array([0.0, 60.0]))
    assert areas[0] > areas[1] > 0


def test_validate_grid_coordinates_accepts_contract_and_rejects_variants():
    lat = row_latitudes(TINY)
    lon = col_longitudes(TINY)
    validate_grid_coordinates(lat, lon, spec=TINY)
    validate_grid_coordinates(lat.astype(np.float32), lon.astype(np.float32), spec=TINY)

    with pytest.raises(ValueError, match="one-dimensional"):
        validate_grid_coordinates(lat[:, None], lon, spec=TINY)
    with pytest.raises(ValueError, match="shape mismatch"):
        validate_grid_coordinates(lat[:1], lon, spec=TINY)
    with pytest.raises(ValueError, match="do not match"):
        validate_grid_coordinates(lat + 0.01, lon, spec=TINY)
    with pytest.raises(ValueError, match="do not match"):
        validate_grid_coordinates(lat, lon + 0.01, spec=TINY)
