from pathlib import Path

import pytest

from titanskies_pipeline.ingestion.tempo.netcdf import extract_grid, validate_netcdf

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "netcdf" / "tempo_no2_sample.nc"
)


@pytest.fixture(scope="module", autouse=True)
def ensure_fixture():
    if not FIXTURE.exists():
        from scripts.generate_netcdf_fixtures import write_fixture

        write_fixture(FIXTURE)


def test_validate_and_extract_netcdf():
    validate_netcdf(FIXTURE)
    grid = extract_grid(FIXTURE)
    assert grid.no2.shape == (2, 2)
    assert grid.quality[0, 0] == 0
