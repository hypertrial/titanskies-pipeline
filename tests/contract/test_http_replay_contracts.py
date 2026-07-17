from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from titanskies_pipeline.ingestion.tempo.cmr import discover_granules

pytestmark = pytest.mark.contract

CASSETTE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "cassettes"
    / "tempo_cmr_granules.json"
)


def test_tempo_cmr_granule_search_replay_contract():
    payload = json.loads(CASSETTE.read_text())
    now = datetime.fromisoformat(payload["now"])

    def search_fn(**_kwargs):
        return [
            SimpleNamespace(
                granule_ur=item["granule_ur"],
                data_links=item["data_links"],
                time_start=item.get("time_start"),
            )
            for item in payload["granules"]
        ]

    results = discover_granules(
        lookback_hours=payload["lookback_hours"],
        concept_id=payload["concept_id"],
        now=now,
        search_fn=search_fn,
    )
    assert len(results) == 1
    assert results[0].granule_id == "TEMPO_NO2_G1"
    assert results[0].download_url == "https://example.test/tempo.nc"
