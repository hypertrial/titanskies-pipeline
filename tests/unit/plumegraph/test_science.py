from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from titanskies_pipeline.config.settings_plumegraph import load_plumegraph_contract
from titanskies_pipeline.plumegraph.science import (
    CandidateInput,
    PixelRecord,
    classify_candidates,
    detect_plumes,
    estimate_emissions,
    expected_calibration_error,
    is_pixel_ready,
)

UTC = timezone.utc


@pytest.fixture
def contract():
    return load_plumegraph_contract()


def _pixel(
    identity: str,
    row: int,
    column: int,
    *,
    latitude: float = 0,
    longitude: float = 0,
    value: float = 1e15,
) -> PixelRecord:
    return PixelRecord(
        identity,
        row,
        column,
        datetime(2024, 1, 1, tzinfo=UTC),
        latitude,
        longitude,
        4.0,
        value,
        1e13,
        0,
        0.01,
        b"wkb",
        "V04",
        "snapshot",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("quality_flag", 1),
        ("cloud_fraction", float("nan")),
        ("cloud_fraction", 0.1),
        ("no2_vertical_column", float("nan")),
        ("no2_uncertainty", float("nan")),
        ("no2_uncertainty", 0),
        ("area_km2", float("nan")),
        ("area_km2", 0),
        ("geometry_wkb", b""),
        ("collection_version", "V03"),
        ("source_snapshot_id", ""),
    ],
)
def test_pixel_readiness_rejects_each_failed_invariant(contract, field, value):
    pixel = _pixel("p", 0, 0, value=-1)
    assert is_pixel_ready(pixel, contract)
    assert not is_pixel_ready(replace(pixel, **{field: value}), contract)


def test_detection_statuses_and_connected_component(contract):
    invalid = replace(_pixel("bad", 0, 0), quality_flag=1)
    result = detect_plumes(
        [invalid],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=1,
        wind_v_ms=0,
        contract=contract,
    )
    assert result[0].status == "no_eligible_pixels"

    ready = _pixel("ready", 0, 0)
    result = detect_plumes(
        [ready],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=float("nan"),
        wind_v_ms=0,
        contract=contract,
    )
    assert result[0].status == "meteorology_incomplete"
    result = detect_plumes(
        [ready],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=0,
        wind_v_ms=0,
        contract=contract,
    )
    assert result[0].status == "meteorology_incomplete"

    background = [
        _pixel(
            f"b{index}",
            index,
            0,
            longitude=-0.6,
            latitude=(index - 15) * 0.001,
            value=1e15 + (index % 3) * 1e12,
        )
        for index in range(30)
    ]
    insufficient = detect_plumes(
        background[:29],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=1,
        wind_v_ms=0,
        contract=contract,
    )
    assert insufficient[0].status == "insufficient_background"

    no_detection = detect_plumes(
        background,
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=1,
        wind_v_ms=0,
        contract=contract,
    )
    assert no_detection[0].status == "no_detection"

    isolated_seed = _pixel("seed", 100, 100, longitude=0.1, value=5e15)
    too_small = detect_plumes(
        [*background, isolated_seed],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=1,
        wind_v_ms=0,
        contract=contract,
    )
    assert too_small[0].status == "insufficient_component"
    assert too_small[0].plume_pixel_ids == ("seed",)
    assert not too_small[0].is_ready

    plume = [
        _pixel(f"p{index}", 100, 100 + index, longitude=0.1, value=5e15)
        for index in range(3)
    ]
    detections = detect_plumes(
        [*background, *plume, invalid],
        target_latitude=0,
        target_longitude=0,
        wind_u_ms=1,
        wind_v_ms=0,
        contract=contract,
    )
    assert detections[0].is_ready
    assert detections[0].plume_pixel_ids == ("p0", "p1", "p2")
    assert detections[0].rejected_pixel_ids == ("bad",)


def test_candidate_ranking_probability_and_abstention(contract):
    candidates = [
        CandidateInput("a", 1.2, 100, -1, 1000, True),
        CandidateInput("b", -1, 10, 100, 1, False),
    ]
    ranked = classify_candidates(
        candidates,
        contract=contract,
        temperature=0.1,
        calibration_ece=0.01,
    )
    assert ranked[0].facility_id == "a"
    assert ranked[0].classification == "likely"
    assert ranked[0].probability is not None
    assert all(item.is_probability_ready for item in ranked)

    tied = classify_candidates(
        [
            CandidateInput("a", 1, 1, 1, 1, True),
            CandidateInput("b", 1, 1, 1, 1, False),
        ],
        contract=contract,
        temperature=1,
        calibration_ece=0,
    )
    assert {item.classification for item in tied} == {"ambiguous"}
    assert (
        classify_candidates(
            [],
            contract=contract,
            temperature=1,
            calibration_ece=0,
        )
        == []
    )
    abstained = classify_candidates(
        [replace(candidates[0], concurrent_nox_lbs=None), candidates[1]],
        contract=contract,
        temperature=1,
        calibration_ece=0,
    )
    assert all(item.probability is None for item in abstained)
    assert all(item.classification == "insufficient_evidence" for item in abstained)
    with pytest.raises(ValueError, match="positive and finite"):
        classify_candidates(
            candidates,
            contract=contract,
            temperature=0,
            calibration_ece=0,
        )


def test_calibration_and_emission_ensemble(contract):
    assert expected_calibration_error([0.0, 1.0], [False, True], bins=2) == 0
    with pytest.raises(ValueError, match="non-empty"):
        expected_calibration_error([], [])
    with pytest.raises(ValueError, match="at least one"):
        expected_calibration_error([0.5], [True], bins=0)
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        expected_calibration_error([2], [True])

    plume = [_pixel("p", 0, 0, value=2e15)]
    assert (
        estimate_emissions(
            [],
            background_molecules_cm2=1e15,
            wind_speeds_ms={"80m": 1},
            plume_length_km=1,
            contract=contract,
        )
        == []
    )
    with pytest.raises(ValueError, match="positive and finite"):
        estimate_emissions(
            plume,
            background_molecules_cm2=1e15,
            wind_speeds_ms={"80m": 1},
            plume_length_km=0,
            contract=contract,
        )
    estimates = estimate_emissions(
        plume,
        background_molecules_cm2=1e15,
        wind_speeds_ms={"bad": float("nan"), "zero": 0, "10m": 4, "80m": 6},
        plume_length_km=10,
        contract=contract,
    )
    assert len(estimates) == 18
    assert sum(item.is_central for item in estimates) == 1
    assert all(item.no2_flux_kg_h > 0 for item in estimates)
