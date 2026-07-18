from __future__ import annotations

import pytest

from titanskies_pipeline.naming import SCOPE_NO2, SCOPE_NO2_STD
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    TEMPO_NO2_OPS_SCHEMA,
    TEMPO_NO2_RAW_SCHEMA,
    TEMPO_NO2_STD_OPS_SCHEMA,
    TEMPO_NO2_STD_RAW_SCHEMA,
    hour_revision_sequence,
    tempo_ops_tbl,
    tempo_raw_tbl,
)


def test_tempo_raw_tbl_defaults_to_nrt_scope():
    assert tempo_raw_tbl("grid_latest") == f'"{TEMPO_NO2_RAW_SCHEMA}"."grid_latest"'


def test_tempo_raw_tbl_supports_std_scope():
    assert tempo_raw_tbl("grid_latest", scope=SCOPE_NO2_STD) == (
        f'"{TEMPO_NO2_STD_RAW_SCHEMA}"."grid_latest"'
    )


def test_tempo_raw_tbl_rejects_unknown_scope():
    with pytest.raises(ValueError, match="Unknown TEMPO scope"):
        tempo_raw_tbl("grid_latest", scope="bogus")


def test_tempo_ops_tbl_defaults_to_nrt_scope():
    assert tempo_ops_tbl("region_registry") == (
        f'"{TEMPO_NO2_OPS_SCHEMA}"."region_registry"'
    )


def test_tempo_ops_tbl_supports_std_scope():
    assert tempo_ops_tbl("region_registry", scope=SCOPE_NO2_STD) == (
        f'"{TEMPO_NO2_STD_OPS_SCHEMA}"."region_registry"'
    )


def test_tempo_ops_tbl_rejects_unknown_scope():
    with pytest.raises(ValueError, match="Unknown TEMPO scope"):
        tempo_ops_tbl("region_registry", scope="bogus")


def test_hour_revision_sequence_by_scope():
    assert hour_revision_sequence(scope=SCOPE_NO2) == "tempo_no2_hour_revision"
    assert hour_revision_sequence(scope=SCOPE_NO2_STD) == "tempo_no2_std_hour_revision"


def test_hour_revision_sequence_rejects_unknown_scope():
    with pytest.raises(ValueError, match="Unknown TEMPO scope"):
        hour_revision_sequence(scope="bogus")
