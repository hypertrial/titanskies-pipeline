from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from titanskies_pipeline.ingestion.tempo.cmr import (
    DiscoveredGranule,
    _acquisition_range,
    _cmr_revision_at,
    _granule_id,
    _granule_urls,
    _parse_datetime,
    _preferred_netcdf_url,
    discover_granules,
)


def test_parse_datetime():
    aware = datetime(2026, 7, 12, 12, 30, tzinfo=timezone.utc)
    assert _parse_datetime(aware) == datetime(2026, 7, 12, 12, 30)
    assert _parse_datetime("2026-07-12T12:30:00Z") == datetime(2026, 7, 12, 12, 30)
    assert _parse_datetime(None) is None
    assert _parse_datetime("not-a-date") is None
    assert _parse_datetime(datetime(2026, 7, 12, 12, 30)) == datetime(
        2026, 7, 12, 12, 30
    )
    assert _parse_datetime("   ") is None
    assert _parse_datetime("2026-07-12T14:30:00+02:00") == datetime(2026, 7, 12, 12, 30)
    assert _parse_datetime("2026-07-12T12:30:00") == datetime(2026, 7, 12, 12, 30)


def test_granule_id_sources_and_missing():
    assert _granule_id(SimpleNamespace(granule_ur="G-UR")) == "G-UR"
    assert _granule_id({"meta": {"native-id": "G-NATIVE"}}) == "G-NATIVE"
    assert _granule_id({"umm": {"GranuleUR": "G-UMM"}}) == "G-UMM"
    assert _granule_id({"id": "G-ID"}) == "G-ID"
    assert _granule_id({"meta": "invalid", "id": "G-BAD-META"}) == "G-BAD-META"
    with pytest.raises(ValueError, match="missing identifier"):
        _granule_id({})
    assert _granule_id({"meta": {"concept-id": "G-CONCEPT"}}) == "G-CONCEPT"
    assert _granule_id({"meta": {}, "umm": {"GranuleUR": "G-TOP-UMM"}}) == "G-TOP-UMM"
    assert (
        _granule_id({"meta": {"native-id": "", "GranuleUR": "G-META-CAPS"}})
        == "G-META-CAPS"
    )
    assert _granule_id(SimpleNamespace(GranuleUR="G-CAPS")) == "G-CAPS"
    assert (
        _granule_id(SimpleNamespace(granule_ur="", GranuleUR="G-CAPS-FALLBACK"))
        == "G-CAPS-FALLBACK"
    )

    class BrokenMapping:
        def __getitem__(self, _key):
            raise TypeError("broken")

    with pytest.raises(ValueError, match="missing identifier"):
        _granule_id(BrokenMapping())


def test_urls_ignore_empty_and_prefer_netcdf():
    links = _granule_urls(
        {
            "links": [
                {"href": None},
                {"href": ""},
                {"href": "https://example.test/readme.txt"},
                {"href": "https://example.test/data.nc?token=x"},
            ]
        }
    )
    assert links == [
        "https://example.test/readme.txt",
        "https://example.test/data.nc?token=x",
    ]
    assert _preferred_netcdf_url(links) == links[1]
    assert _preferred_netcdf_url([]) is None
    assert _preferred_netcdf_url(["https://example.test/readme.txt"]) == (
        "https://example.test/readme.txt"
    )
    assert _granule_urls(
        SimpleNamespace(data_links=lambda: [None, "   ", " https://example.test/a.nc "])
    ) == ["https://example.test/a.nc"]
    assert _granule_urls(SimpleNamespace(data_links=[None, "a.nc"])) == ["a.nc"]
    assert _granule_urls(SimpleNamespace()) == []


def test_acquisition_and_revision_precedence():
    granule = {
        "umm": {
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": "2026-07-12T17:00:00Z",
                    "EndingDateTime": "2026-07-12T17:10:00Z",
                }
            }
        },
        "meta": {"revision-date": "2026-07-12T18:00:00Z"},
    }
    assert _acquisition_range(granule) == (
        datetime(2026, 7, 12, 17),
        datetime(2026, 7, 12, 17, 10),
    )
    assert _cmr_revision_at(granule) == datetime(2026, 7, 12, 18)
    fallback = SimpleNamespace(
        time_start="2026-07-12T16:00:00Z",
        time_end="2026-07-12T16:10:00Z",
        updated="2026-07-12T19:00:00Z",
    )
    assert _acquisition_range(fallback)[0] == datetime(2026, 7, 12, 16)
    assert _cmr_revision_at(fallback) == datetime(2026, 7, 12, 19)
    mapping_fallback = {
        "time_start": "2026-07-12T15:00:00Z",
        "time_end": "2026-07-12T15:10:00Z",
        "updated": "2026-07-12T20:00:00Z",
    }
    assert _acquisition_range(mapping_fallback) == (
        datetime(2026, 7, 12, 15),
        datetime(2026, 7, 12, 15, 10),
    )
    assert _cmr_revision_at(mapping_fallback) == datetime(2026, 7, 12, 20)
    single = {
        "umm": {"TemporalExtent": {"SingleDateTime": {"Time": "2026-07-12T15:00:00Z"}}},
        "meta": {"revision_date": "2026-07-12T20:00:00Z"},
    }
    assert _acquisition_range(single) == (
        datetime(2026, 7, 12, 15),
        datetime(2026, 7, 12, 15),
    )
    assert _cmr_revision_at(single) == datetime(2026, 7, 12, 20)


def test_discovery_preserves_nullable_metadata_and_query_window():
    captured = {}

    def search_fn(**kwargs):
        captured.update(kwargs)
        return [
            {
                "meta": {
                    "native-id": "G-SEARCH",
                    "revision-date": "2026-07-12T18:00:00Z",
                },
                "umm": {"TemporalExtent": {"SingleDateTime": "2026-07-12T17:30:00Z"}},
                "links": [{"href": None}, {"href": "https://e.test/g.nc"}],
            },
            {"id": "G-NULL", "links": []},
        ]

    rows = discover_granules(
        lookback_hours=4,
        concept_id="TEST",
        now=datetime(2026, 7, 12, 18),
        search_fn=search_fn,
    )
    assert captured["temporal"] == (
        "2026-07-12T14:00:00Z",
        "2026-07-12T18:00:00Z",
    )
    assert isinstance(rows[0], DiscoveredGranule)
    assert rows[0].acquisition_start == datetime(2026, 7, 12, 17, 30)
    assert rows[0].cmr_revision_at == datetime(2026, 7, 12, 18)
    assert rows[1].acquisition_start is None
    assert rows[1].cmr_revision_at is None
    with pytest.raises(ValueError, match="lookback_hours"):
        discover_granules(lookback_hours=0, search_fn=search_fn)


def test_discovery_explicit_window_overrides_lookback():
    captured = {}

    def search_fn(**kwargs):
        captured.update(kwargs)
        return []

    discover_granules(
        window_start=datetime(2026, 7, 1, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 7, 2, 0, tzinfo=timezone.utc),
        concept_id="TEST",
        search_fn=search_fn,
    )
    assert captured["temporal"] == (
        "2026-07-01T00:00:00Z",
        "2026-07-02T00:00:00Z",
    )


def test_discovery_requires_both_window_bounds():
    with pytest.raises(ValueError, match="window_start and window_end"):
        discover_granules(
            window_start=datetime(2026, 7, 1, 0),
            search_fn=lambda **_kwargs: [],
        )
    with pytest.raises(ValueError, match="window_start and window_end"):
        discover_granules(
            window_end=datetime(2026, 7, 1, 0),
            search_fn=lambda **_kwargs: [],
        )


def test_discovery_uses_public_earthaccess_search_without_login(monkeypatch):
    earthaccess = SimpleNamespace(
        search_data=lambda **_kwargs: [
            SimpleNamespace(granule_ur="G-EA", data_links=["https://ea.test/g.nc"])
        ],
    )
    monkeypatch.setitem(__import__("sys").modules, "earthaccess", earthaccess)
    assert discover_granules(lookback_hours=2)[0].granule_id == "G-EA"
