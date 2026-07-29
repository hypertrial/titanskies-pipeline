from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from titanskies_pipeline.riverpulse.hydrocron import (
    HYDROCRON_FIELDS,
    MAX_RESPONSE_BYTES,
    FetchResult,
    HydrocronFetchError,
    HydrocronRequest,
    _retry_after_seconds,
    calendar_year_windows,
    fetch_hydrocron,
    is_no_data_response,
    normalize_utc,
    parse_csv_response,
    parse_utc,
    request_id,
    revision_id,
    sanitize_error,
    stable_observation_id,
)

TESTS_ROOT = Path(__file__).resolve().parents[2]
CASSETTE = TESTS_ROOT / "fixtures" / "cassettes" / "riverpulse_hydrocron.csv"
UTC = timezone.utc


class FakeResponse:
    def __init__(self, status_code: int, body: bytes, headers=None):
        self.status_code = status_code
        self.content = body
        self.headers = headers or {}


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _request() -> HydrocronRequest:
    return HydrocronRequest.create(
        reach_id="RP1001",
        window_start=datetime(2024, 1, 1, tzinfo=UTC),
        window_end=datetime(2025, 1, 1, tzinfo=UTC),
        field_contract_version="riverpulse-v1",
    )


def test_request_contract_and_deterministic_id():
    request = _request()
    params = request.params()
    assert params["feature"] == "Reach"
    assert params["feature_id"] == "RP1001"
    assert params["output"] == "csv"
    assert "geometry" not in params
    assert set(params["fields"].split(",")) == set(HYDROCRON_FIELDS)
    assert request == _request()
    assert request.request_id == request_id(
        collection_name=request.collection_name,
        reach_id=request.reach_id,
        window_start=request.window_start,
        window_end=request.window_end,
        field_contract_version=request.field_contract_version,
    )
    with pytest.raises(ValueError, match="non-empty"):
        HydrocronRequest.create(
            reach_id="x",
            window_start=datetime(2024, 1, 1, tzinfo=UTC),
            window_end=datetime(2024, 1, 1, tzinfo=UTC),
            field_contract_version="v1",
        )


def test_identity_contract_is_stable_and_revision_sensitive():
    observed = datetime(2024, 6, 15, 10, 30, tzinfo=UTC)
    stable = stable_observation_id(
        collection_name="SWOT_L2_HR_RiverSP_reach_D",
        reach_id="RP1001",
        observation_time=observed,
        cycle_id=11,
        pass_id=22,
    )
    assert stable == stable_observation_id(
        collection_name="SWOT_L2_HR_RiverSP_reach_D",
        reach_id="RP1001",
        observation_time=observed,
        cycle_id=11,
        pass_id=22,
    )
    first = revision_id(
        observation_id=stable,
        crid="PIC0",
        granule_id="g1",
        canonical_record='{"wse":"1"}',
    )
    assert first != revision_id(
        observation_id=stable,
        crid="PIC1",
        granule_id="g1",
        canonical_record='{"wse":"1"}',
    )
    assert first != revision_id(
        observation_id=stable,
        crid="PIC0",
        granule_id="g1",
        canonical_record='{"wse":"2"}',
    )


def test_parse_cassette_preserves_revisions_and_normalizes_discharges():
    parsed = parse_csv_response(
        CASSETTE.read_bytes(),
        collected_at=datetime(2024, 7, 8, tzinfo=UTC),
    )
    assert len(parsed) == 3
    assert len({row.values["observation_id"] for row in parsed}) == 2
    assert len({row.values["observation_revision_id"] for row in parsed}) == 3
    assert parsed[0].values["wse"] == 8.42
    assert parsed[1].values["wse_unit"] == "m"
    assert parsed[0].values["slope_unit"] == "m/m"
    assert parsed[0].values["unconstrained_discharge_quality_bits"] == 0
    assert parsed[0].values["constrained_discharge_quality_bits"] == 0
    assert len(parsed[0].discharges) == 14
    constrained = next(
        row
        for row in parsed[0].discharges
        if row["algorithm"] == "c" and row["is_constrained"]
    )
    unconstrained = next(
        row
        for row in parsed[0].discharges
        if row["algorithm"] == "c" and not row["is_constrained"]
    )
    assert constrained["discharge_value"] == 309.0
    assert unconstrained["discharge_value"] == 315.0
    assert constrained["discharge_unit"] == "m3/s"


def test_parse_preserves_hydrocron_units_when_returned():
    lines = CASSETTE.read_text().splitlines()
    lines[0] += ",wse_units,width_units,slope_units,dschg_c_units"
    for index in range(1, len(lines)):
        lines[index] += ",cm,ft,km/km,ft3/s"
    parsed = parse_csv_response(
        ("\n".join(lines) + "\n").encode(),
        collected_at=datetime(2024, 7, 8, tzinfo=UTC),
    )
    assert parsed[0].values["wse_unit"] == "cm"
    assert parsed[0].values["width_unit"] == "ft"
    assert parsed[0].values["slope_unit"] == "km/km"
    assert (
        next(
            row
            for row in parsed[0].discharges
            if row["algorithm"] == "c" and not row["is_constrained"]
        )["discharge_unit"]
        == "ft3/s"
    )


def test_parse_numeric_time_missing_values_and_invalid_rows():
    header = (
        "reach_id,time,cycle_id,pass_id,crid,sword_version,"
        "collection_version,granuleUR,ingest_time,wse,wse_u\n"
    )
    valid = (header + "1,0,1,2,PIC0,17b,D,g,2024-01-01T00:00:00Z,-9999,NaN\n").encode()
    parsed = parse_csv_response(valid, collected_at=datetime.now(UTC))
    assert parsed[0].values["observation_time"] == datetime(2000, 1, 1, tzinfo=UTC)
    assert parsed[0].values["wse"] is None
    assert parsed[0].values["wse_u"] is None
    sentinel_header = header.replace("reach_id,time,", "reach_id,time,time_str,")
    sentinel = (
        sentinel_header
        + "1,60,no_data,1,2,PIC0,17b,D,g,2024-01-01T00:00:00Z,no_data,1\n"
    ).encode()
    sentinel_row = parse_csv_response(sentinel, collected_at=datetime.now(UTC))[
        0
    ].values
    assert sentinel_row["observation_time"] == datetime(2000, 1, 1, 0, 1, tzinfo=UTC)
    assert sentinel_row["wse"] is None

    with pytest.raises(ValueError, match="missing required identity"):
        parse_csv_response(b"reach_id,time\n1,0\n", collected_at=datetime.now(UTC))
    with pytest.raises(ValueError, match="row 2"):
        parse_csv_response(
            (header + "1,0,1.5,2,PIC0,17b,D,g,2024-01-01T00:00:00Z,1,1\n").encode(),
            collected_at=datetime.now(UTC),
        )
    with pytest.raises(ValueError, match="not UTF-8"):
        parse_csv_response(b"\xff\xfe", collected_at=datetime.now(UTC))
    with pytest.raises(ValueError, match="6 MB"):
        parse_csv_response(
            b"x" * (MAX_RESPONSE_BYTES + 1), collected_at=datetime.now(UTC)
        )
    with pytest.raises(ValueError, match="Missing timestamp"):
        parse_utc("")
    with pytest.raises(ValueError, match="numeric value"):
        parse_csv_response(
            (header + "1,0,1,2,PIC0,17b,D,g,2024-01-01T00:00:00Z,bad,1\n").encode(),
            collected_at=datetime.now(UTC),
        )
    for replacement, message in (
        (",,2,PIC0,", "required Hydrocron integer"),
        (",1,2,,", "required Hydrocron text"),
    ):
        row = "1,0,1,2,PIC0,17b,D,g,2024-01-01T00:00:00Z,1,1\n"
        with pytest.raises(ValueError, match=message):
            parse_csv_response(
                (header + row.replace(",1,2,PIC0,", replacement)).encode(),
                collected_at=datetime.now(UTC),
            )


def test_utc_parsing_and_calendar_year_windows():
    assert parse_utc("2024-01-01T01:00:00+01:00") == datetime(2024, 1, 1, tzinfo=UTC)
    assert parse_utc("60") == datetime(2000, 1, 1, 0, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="UTC offset"):
        normalize_utc(datetime(2024, 1, 1))
    windows = calendar_year_windows(
        datetime(2023, 8, 1, tzinfo=UTC),
        datetime(2025, 2, 1, tzinfo=UTC),
    )
    assert windows == [
        (
            datetime(2023, 8, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
        ),
        (
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 1, tzinfo=UTC),
        ),
        (
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 2, 1, tzinfo=UTC),
        ),
    ]
    with pytest.raises(ValueError, match="non-empty"):
        calendar_year_windows(
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 1, tzinfo=UTC),
        )


def test_fetch_success_no_data_and_api_key_header():
    session = FakeSession([FakeResponse(200, b"csv")])
    result = fetch_hydrocron(
        _request(), api_key="secret", session=session, sleep=lambda _: None
    )
    assert result == FetchResult(200, b"csv", 1, False)
    assert session.calls[0][1]["headers"]["x-hydrocron-key"] == "secret"
    assert "x-hydrocron-key" not in session.calls[0][1]["params"]

    body = b'{"message":"No data found for requested feature"}'
    result = fetch_hydrocron(
        _request(),
        session=FakeSession([FakeResponse(400, body)]),
        sleep=lambda _: None,
    )
    assert result.no_data is True
    assert is_no_data_response(body)
    assert not is_no_data_response(b'{"message":"bad field"}')


def test_fetch_retries_429_5xx_and_timeout():
    sleeps = []
    session = FakeSession(
        [
            FakeResponse(429, b"slow", {"Retry-After": "0.5"}),
            FakeResponse(503, b"down", {"Retry-After": "not-a-number"}),
            requests.Timeout("late"),
            FakeResponse(200, b"ok"),
        ]
    )
    result = fetch_hydrocron(_request(), session=session, sleep=sleeps.append)
    assert result.attempts == 4
    assert sleeps == [0.5, 2.0, 4.0]

    missing_retry_after_sleeps = []
    result = fetch_hydrocron(
        _request(),
        session=FakeSession([FakeResponse(429, b"slow"), FakeResponse(200, b"ok")]),
        sleep=missing_retry_after_sleeps.append,
    )
    assert result.attempts == 2
    assert missing_retry_after_sleeps == [1.0]


def test_retry_after_supports_http_dates():
    now = datetime(2025, 1, 1, tzinfo=UTC)
    assert (
        _retry_after_seconds(
            FakeResponse(
                429,
                b"slow",
                {"Retry-After": "Wed, 01 Jan 2025 00:00:05 GMT"},
            ),
            1.0,
            now=now,
        )
        == 5.0
    )
    assert (
        _retry_after_seconds(
            FakeResponse(429, b"slow", {"Retry-After": "Wed, 01 Jan 2025 00:00:05"}),
            1.0,
            now=now,
        )
        == 5.0
    )
    assert (
        _retry_after_seconds(
            FakeResponse(
                429,
                b"slow",
                {"Retry-After": "Tue, 31 Dec 2024 23:59:59 GMT"},
            ),
            1.0,
            now=now,
        )
        == 0.0
    )


@pytest.mark.parametrize("status", [400, 413, 404])
def test_fetch_actionable_client_errors_do_not_retry(status):
    session = FakeSession([FakeResponse(status, b"invalid request")])
    with pytest.raises(HydrocronFetchError) as raised:
        fetch_hydrocron(_request(), session=session, sleep=lambda _: None)
    assert raised.value.attempts == 1
    assert raised.value.status_code == status


def test_fetch_retry_exhaustion_and_response_limit():
    with pytest.raises(HydrocronFetchError, match="exhausted retries") as raised:
        fetch_hydrocron(
            _request(),
            session=FakeSession([requests.Timeout("late")] * 4),
            sleep=lambda _: None,
        )
    assert raised.value.attempts == 4
    with pytest.raises(HydrocronFetchError, match="6 MB"):
        fetch_hydrocron(
            _request(),
            session=FakeSession([FakeResponse(200, b"x" * (MAX_RESPONSE_BYTES + 1))]),
            sleep=lambda _: None,
        )
    with pytest.raises(HydrocronFetchError, match="HTTP 503") as server_error:
        fetch_hydrocron(
            _request(),
            session=FakeSession([FakeResponse(503, b"down")] * 4),
            sleep=lambda _: None,
        )
    assert server_error.value.attempts == 4


def test_sanitize_error_removes_secrets_and_query_strings():
    sanitized = sanitize_error(
        "secret https://example.test/path?token=secret other",
        secrets=("secret", None),
    )
    assert "secret" not in sanitized
    assert "token=" not in sanitized
    assert "[REDACTED]" in sanitized
