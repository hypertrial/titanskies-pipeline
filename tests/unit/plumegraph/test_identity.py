from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from titanskies_pipeline.plumegraph.identity import (
    analysis_generation_manifest_identity,
    analysis_partition_identity,
    camd_local_standard_hour_to_utc,
    canonical_json,
    emission_identity,
    episode_revision_identity,
    gps_seconds_to_utc,
    normalize_utc,
    pixel_identity,
    pixel_revision_identity,
    sha256_identity,
)

UTC = timezone.utc


def test_canonical_identity_is_deterministic_and_typed():
    moment = datetime(2024, 1, 1, tzinfo=UTC)
    assert canonical_json({"b": b"\x01", "a": moment}) == (
        '{"a":"2024-01-01T00:00:00+00:00","b":"01"}'
    )
    with pytest.raises(TypeError, match="Cannot canonicalize set"):
        canonical_json({"unsupported": {1}})
    assert sha256_identity("a", 1) == sha256_identity("a", 1)
    assert pixel_identity("concept", "granule", 1, 2, moment) != pixel_identity(
        "concept", "granule", 1, 3, moment
    )
    stable = pixel_identity("concept", "granule", 1, 2, moment)
    assert pixel_revision_identity(stable, "r1", "a" * 64, "{}") != (
        pixel_revision_identity(stable, "r2", "a" * 64, "{}")
    )
    assert emission_identity("1", "A", date(2024, 1, 1), 0) == emission_identity(
        "1", "A", date(2024, 1, 1), 0
    )
    assert analysis_partition_identity(
        "region", date(2024, 1, 1), "v1", "a1", ["b", "a"]
    ) == analysis_partition_identity("region", date(2024, 1, 1), "v1", "a1", ["a", "b"])
    assert analysis_partition_identity(
        "region", date(2024, 1, 1), "v1", "a1", ["a"]
    ) != analysis_partition_identity("region", date(2024, 1, 1), "v1", "a2", ["a"])
    generations = [
        ("region-b", date(2024, 1, 2), "run-b"),
        ("region-a", date(2024, 1, 1), "run-a"),
    ]
    assert analysis_generation_manifest_identity(
        generations
    ) == analysis_generation_manifest_identity(list(reversed(generations)))
    assert episode_revision_identity(
        "run", ["b", "a"], ["edge"], ["p2", "p1"], {"x": 1}
    ) == episode_revision_identity("run", ["a", "b"], ["edge"], ["p1", "p2"], {"x": 1})


def test_time_conversions_enforce_explicit_contracts():
    with pytest.raises(ValueError, match="timezone"):
        normalize_utc(datetime(2024, 1, 1))
    assert normalize_utc(datetime(2024, 1, 1, tzinfo=UTC)).tzinfo == UTC
    gps_epoch_seconds_2024 = (
        datetime(2024, 1, 1, tzinfo=UTC) - datetime(1980, 1, 6, tzinfo=UTC)
    ).total_seconds() + 18
    assert gps_seconds_to_utc(gps_epoch_seconds_2024) == datetime(
        2024, 1, 1, tzinfo=UTC
    )
    with pytest.raises(ValueError, match="outside"):
        gps_seconds_to_utc(-1)
    with pytest.raises(ValueError, match="between 0 and 23"):
        camd_local_standard_hour_to_utc(
            date(2024, 1, 1),
            24,
            timezone_name="America/Chicago",
            utc_standard_offset_minutes=-360,
        )
    with pytest.raises(ValueError, match="does not match"):
        camd_local_standard_hour_to_utc(
            date(2024, 1, 1),
            0,
            timezone_name="America/Chicago",
            utc_standard_offset_minutes=-300,
        )
    assert camd_local_standard_hour_to_utc(
        date(2024, 7, 15),
        12,
        timezone_name="America/Chicago",
        utc_standard_offset_minutes=-360,
    ) == datetime(2024, 7, 15, 18, tzinfo=UTC)
