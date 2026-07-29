from __future__ import annotations

import builtins
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import duckdb
import pytest
import requests
import xarray as xr

from titanskies_pipeline.plumegraph import connectors
from titanskies_pipeline.plumegraph.sources import (
    SourceIngestMetrics,
    SourceRequest,
    SourceSnapshot,
)
from titanskies_pipeline.storage.duckdb.schemas.plumegraph import (
    bootstrap_plumegraph_tables,
)

UTC = timezone.utc


class _Response:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._payload


def _request(connector="hrrr", identity="request", **payload):
    return SourceRequest(
        identity,
        connector,
        "v1",
        "region",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 1, tzinfo=UTC),
        "contract",
        payload,
    )


def _snapshot(request):
    return SourceSnapshot(
        "snapshot",
        request.request_id,
        request.connector,
        "source",
        request.window_end,
        "artifact",
        "a" * 64,
        "etag",
        "b" * 64,
        1,
        request.window_end,
    )


def test_http_retry_matrix(monkeypatch):
    responses = [
        requests.Timeout("timeout"),
        _Response(429, headers={"Retry-After": "3"}),
        _Response(500),
        _Response(200, {"ok": True}, {"etag": "x"}),
    ]

    def get(*_args, **_kwargs):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    sleeps = []
    monkeypatch.setattr(requests, "get", get)
    payload, headers = connectors._http_json(
        "https://example.test",
        params={},
        headers={},
        sleep=sleeps.append,
    )
    assert payload == {"ok": True}
    assert headers["etag"] == "x"
    assert sleeps == [1, 3, 4]

    monkeypatch.setattr(
        requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(requests.Timeout("timeout")),
    )
    with pytest.raises(requests.Timeout):
        connectors._http_json(
            "https://example.test",
            params={},
            headers={},
            sleep=lambda _seconds: None,
        )
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: _Response(503))
    with pytest.raises(requests.HTTPError):
        connectors._http_json(
            "https://example.test",
            params={},
            headers={},
            sleep=lambda _seconds: None,
        )
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: _Response(400))
    with pytest.raises(requests.HTTPError):
        connectors._http_json(
            "https://example.test",
            params={},
            headers={},
            sleep=lambda _seconds: None,
        )


def test_hrrr_array_helpers_close_datasets(monkeypatch):
    data = xr.Dataset({"UGRD": (("projection_y", "projection_x"), [[7.0]])})
    monkeypatch.setattr(xr, "open_zarr", lambda *_args, **_kwargs: data)
    assert (
        connectors._hrrr_value(
            "s3://example.test",
            level="surface",
            variable="UGRD",
            y_index=0,
            x_index=0,
        )
        == 7
    )

    grid = xr.Dataset(
        {
            "latitude": (("y", "x"), [[0.0, 1.0], [2.0, 3.0]]),
            "longitude": (("y", "x"), [[10.0, 11.0], [12.0, 13.0]]),
        }
    )
    opened = []

    def open_grid(path, **_kwargs):
        opened.append(path)
        return grid

    monkeypatch.setattr(xr, "open_zarr", open_grid)
    assert connectors._hrrr_grid_index(
        2.1,
        12.1,
        store_url="s3://example.test/hrrr",
    ) == (1, 0)
    assert opened == ["s3://example.test/hrrr/grid/HRRR_chunk_index.zarr"]


def test_fetch_hrrr_normalizes_hourly_fields(monkeypatch, tmp_path):
    request = _request(
        bbox=[-101, 34, -99, 36],
    )
    monkeypatch.setattr(
        connectors,
        "_hrrr_grid_index",
        lambda *_args, **_kwargs: (2, 3),
    )
    values = {
        "UGRD": 6,
        "VGRD": 2,
        "HPBL": 900,
        "PRES": 98000,
        "TMP": 298,
    }
    monkeypatch.setattr(
        connectors,
        "_hrrr_value",
        lambda _base, *, variable, **_kwargs: values[variable],
    )
    monkeypatch.setattr(
        connectors,
        "write_source_snapshot",
        lambda *_args, **_kwargs: _snapshot(request),
    )
    captured = {}

    def persist(**kwargs):
        captured.update(kwargs)
        return SourceIngestMetrics(0, 0, len(kwargs["meteorology"]), 0)

    monkeypatch.setattr(connectors, "persist_normalized_records", persist)
    assert connectors._fetch_hrrr(request, raw_data_dir=tmp_path, conn=object()) == (
        1,
        1,
    )
    row = captured["meteorology"][0]
    assert row["surface_pressure_hpa"] == 980
    assert row["wind_u_80m"] == 6


def test_fetch_camd_pages_filters_and_requires_key(monkeypatch, tmp_path):
    request = _request(
        "camd",
        facility_ids=["1", "missing"],
    )
    monkeypatch.setattr(
        connectors,
        "get_plumegraph_settings",
        lambda: SimpleNamespace(epa_api_key=None),
    )
    with pytest.raises(RuntimeError, match="EPA_API_KEY"):
        connectors._fetch_camd(request, raw_data_dir=tmp_path, conn=object())

    monkeypatch.setattr(
        connectors,
        "get_plumegraph_settings",
        lambda: SimpleNamespace(epa_api_key="key"),
    )
    pages = [
        (
            {
                "items": [
                    {
                        "facilityId": 1,
                        "unitId": "U1",
                        "date": "2024-01-01",
                        "hour": 0,
                        "noxMass": "",
                        "opTime": 1,
                        "heatInput": 2,
                        "grossLoad": 3,
                        "noxMassMeasureFlg": "M",
                    },
                    "ignored",
                ],
                "pagination": {"totalPages": 2},
            },
            {},
        ),
        (
            {
                "items": [
                    {
                        "facilityId": "missing",
                        "unitId": "U1",
                        "date": "2024-01-01",
                        "hour": 0,
                    }
                ],
                "pagination": {"totalPages": 2},
            },
            {},
        ),
    ]
    monkeypatch.setattr(
        connectors, "_http_json", lambda *_args, **_kwargs: pages.pop(0)
    )
    monkeypatch.setattr(
        connectors,
        "write_source_snapshot",
        lambda *_args, **_kwargs: _snapshot(request),
    )
    captured = {}
    monkeypatch.setattr(
        connectors,
        "persist_normalized_records",
        lambda **kwargs: (
            captured.update(kwargs)
            or SourceIngestMetrics(0, 0, 0, len(kwargs["emissions"]))
        ),
    )
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create schema plumegraph_events_raw;
        create table plumegraph_events_raw.facilities (
            facility_id varchar, timezone varchar,
            utc_standard_offset_minutes integer
        );
        insert into plumegraph_events_raw.facilities
        values ('1', 'America/Chicago', -360);
        """
    )
    assert connectors._fetch_camd(request, raw_data_dir=tmp_path, conn=conn) == (
        1,
        1,
    )
    assert captured["emissions"][0]["nox_mass_lbs"] is None
    conn.close()

    monkeypatch.setattr(
        connectors,
        "_http_json",
        lambda *_args, **_kwargs: ({"wrong": []}, {}),
    )
    with pytest.raises(ValueError, match="unexpected response schema"):
        connectors._fetch_camd(request, raw_data_dir=tmp_path, conn=object())


def test_fetch_harmony_validates_and_normalizes_downloads(
    monkeypatch,
    tmp_path,
):
    request = _request(
        "harmony",
        bbox=[-1, -1, 1, 1],
        concept_id="concept",
        variables=["x"],
    )
    harmony = ModuleType("harmony")

    class Request:
        valid = True

        def __init__(self, **_kwargs):
            pass

        def is_valid(self):
            return self.valid

    class Client:
        def submit(self, _request):
            return "job"

        def download_all(self, _job, *, directory, overwrite):
            assert overwrite
            path = Path(directory) / "subset.nc"
            path.write_bytes(b"netcdf")
            return [SimpleNamespace(result=lambda: str(path))]

    harmony.BBox = lambda *values: values
    harmony.Collection = lambda **values: values
    harmony.Request = Request
    harmony.Client = Client
    monkeypatch.setitem(sys.modules, "harmony", harmony)
    monkeypatch.setattr(
        connectors,
        "read_tempo_l2_netcdf",
        lambda _path: [{"time_gps_seconds": 1, "mirror_step": 0, "xtrack": 0}],
    )
    monkeypatch.setattr(
        connectors,
        "_tempo_source_metadata",
        lambda _path: ("producer-granule", datetime(2024, 1, 2, tzinfo=UTC)),
    )
    monkeypatch.setattr(
        connectors,
        "write_source_snapshot",
        lambda *_args, **_kwargs: _snapshot(request),
    )
    monkeypatch.setattr(
        connectors,
        "normalize_tempo_pixel",
        lambda record, **_kwargs: record,
    )
    monkeypatch.setattr(
        connectors,
        "persist_normalized_records",
        lambda **kwargs: SourceIngestMetrics(0, len(kwargs["pixels"]), 0, 0),
    )
    assert connectors._fetch_harmony(
        request,
        raw_data_dir=tmp_path,
        conn=object(),
    ) == (1, 1)
    Request.valid = False
    with pytest.raises(ValueError, match="rejected"):
        connectors._fetch_harmony(
            request,
            raw_data_dir=tmp_path,
            conn=object(),
        )


def test_tempo_source_metadata_requires_authoritative_utc_revision(tmp_path):
    path = tmp_path / "subset.nc"
    xr.Dataset(
        attrs={
            "granule_id": "TEMPO_GRANULE",
            "date_created": "2024-01-02T03:04:05Z",
        }
    ).to_netcdf(path)
    assert connectors._tempo_source_metadata(path) == (
        "TEMPO_GRANULE",
        datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
    )
    xr.Dataset(attrs={"date_created": "2024-01-02T03:04:05"}).to_netcdf(path)
    with pytest.raises(ValueError, match="include a timezone"):
        connectors._tempo_source_metadata(path)
    xr.Dataset().to_netcdf(path)
    with pytest.raises(ValueError, match="authoritative production"):
        connectors._tempo_source_metadata(path)


def test_fetch_harmony_reports_missing_optional_dependency(monkeypatch, tmp_path):
    request = _request(
        "harmony",
        bbox=[-1, -1, 1, 1],
        concept_id="concept",
        variables=["x"],
    )
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name == "harmony":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    with pytest.raises(RuntimeError, match="extra plumegraph"):
        connectors._fetch_harmony(request, raw_data_dir=tmp_path, conn=object())


def test_connector_batch_commits_successes_and_records_failures(
    monkeypatch,
    tmp_path,
):
    conn = duckdb.connect(":memory:")
    bootstrap_plumegraph_tables(conn)
    for index in range(3):
        request = _request("hrrr", f"r{index}", bbox=[0, 0, 1, 1])
        conn.execute(
            """
            insert into plumegraph_events_ops.source_requests
            (request_id, connector, source_version, analysis_region_id,
             window_start, window_end, request_json, request_contract_version,
             status, attempts, planned_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, 'planned', 0,
                    current_timestamp, current_timestamp)
            """,
            [
                request.request_id,
                request.connector,
                request.source_version,
                request.analysis_region_id,
                request.window_start.replace(tzinfo=None),
                request.window_end.replace(tzinfo=None),
                json.dumps(request.request),
                request.request_contract_version,
            ],
        )

    def fetch(request, **_kwargs):
        if request.request_id == "r1":
            raise RuntimeError("api_key=super-secret")
        return 1, 2

    monkeypatch.setitem(connectors._FETCHERS, "hrrr", fetch)
    monkeypatch.setenv("PLUMEGRAPH_EPA_API_KEY", "super-secret")
    with pytest.raises(connectors.PlumeGraphConnectorError) as caught:
        connectors.sync_source_connector(
            "hrrr",
            raw_data_dir=tmp_path,
            conn=conn,
        )
    assert caught.value.metrics.requests_succeeded == 2
    assert caught.value.metrics.requests_failed == 1
    statuses = dict(
        conn.execute(
            """
            select request_id, status
            from plumegraph_events_ops.source_requests
            """
        ).fetchall()
    )
    assert statuses == {"r0": "success", "r1": "failed", "r2": "success"}
    error = conn.execute(
        """
        select error_message
        from plumegraph_events_ops.source_requests
        where request_id = 'r1'
        """
    ).fetchone()[0]
    assert "super-secret" not in error
    conn.execute(
        """
        update plumegraph_events_ops.source_requests
        set status = 'planned'
        """
    )
    metrics = connectors.sync_source_connector(
        "hrrr",
        max_requests=1,
        raw_data_dir=tmp_path,
        conn=conn,
    )
    assert metrics.requests_succeeded == 1
    with pytest.raises(ValueError, match="Unsupported"):
        connectors.sync_source_connector("unknown", conn=conn)
    conn.close()
