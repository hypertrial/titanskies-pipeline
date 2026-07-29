from titanskies_pipeline.naming import (
    ANDREADIS2025_REPRO,
    RIVERPULSE_EVENTS,
    SCOPE_EVENTS,
    SCOPE_NO2,
    SCOPE_REPRO,
    SOURCE_ANDREADIS2025,
    SOURCE_RIVERPULSE,
    SOURCE_SUN2025,
    SOURCE_TEMPO,
    SUN2025_REPRO,
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
    assert RIVERPULSE_EVENTS == "riverpulse_events"
    assert flat_name(SOURCE_RIVERPULSE, SCOPE_EVENTS) == "riverpulse_events"
    assert SUN2025_REPRO == "sun2025_repro"
    assert ANDREADIS2025_REPRO == "andreadis2025_repro"
    assert flat_name(SOURCE_SUN2025, SCOPE_REPRO) == "sun2025_repro"
    assert flat_name(SOURCE_ANDREADIS2025, SCOPE_REPRO) == "andreadis2025_repro"
