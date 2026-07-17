from titanskies_pipeline.naming import (
    SCOPE_NO2,
    SOURCE_TEMPO,
    TEMPO_NO2,
    asset_key,
    flat_name,
    schema_name,
)


def test_flat_name():
    assert flat_name(SOURCE_TEMPO, SCOPE_NO2, "raw", "granules") == (
        "tempo_no2_raw_granules"
    )


def test_schema_name():
    assert schema_name(SOURCE_TEMPO, SCOPE_NO2, "marts") == "tempo_no2_marts"


def test_asset_key():
    assert asset_key(SOURCE_TEMPO, SCOPE_NO2, "raw", "granules").path == [
        "tempo",
        "no2",
        "raw",
        "granules",
    ]


def test_constants():
    assert TEMPO_NO2 == "tempo_no2"
