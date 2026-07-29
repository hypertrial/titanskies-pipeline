from __future__ import annotations

import csv

import pytest

from titanskies_pipeline.config import settings_plumegraph as settings


def _contract(tmp_path, **updates):
    with settings.PLUMEGRAPH_CONTRACT_PATH.open(newline="") as handle:
        row = next(csv.DictReader(handle))
    row.update({key: str(value) for key, value in updates.items()})
    path = tmp_path / "contract.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    return path


def test_contract_loads_pinned_policy_and_runtime_env(monkeypatch, tmp_path):
    contract = settings.load_plumegraph_contract()
    assert contract["collection_name"] == "TEMPO_NO2_L2"
    assert contract["wind_levels_m"] == (10, 80)
    monkeypatch.setenv("PLUMEGRAPH_RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PLUMEGRAPH_RELEASE_DIR", str(tmp_path / "release"))
    monkeypatch.setenv("PLUMEGRAPH_COHORT_MANIFEST_PATH", str(tmp_path / "cohort"))
    monkeypatch.setenv("PLUMEGRAPH_DISCOVERY_LOOKBACK_DAYS", "7")
    monkeypatch.setenv("PLUMEGRAPH_RAW_CACHE_RETENTION_DAYS", "9")
    monkeypatch.setenv("PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED", "true")
    monkeypatch.setenv("PLUMEGRAPH_EPA_API_KEY", "secret")
    monkeypatch.setenv("PLUMEGRAPH_HRRR_STORE_URL", "s3://example.test")
    configured = settings.get_plumegraph_settings()
    assert configured.raw_data_dir == tmp_path / "raw"
    assert configured.release_dir == tmp_path / "release"
    assert configured.cohort_manifest_path == tmp_path / "cohort"
    assert configured.discovery_lookback_days == 7
    assert configured.raw_cache_retention_days == 9
    assert configured.schedule_enabled
    assert configured.epa_api_key == "secret"
    assert configured.hrrr_store_url == "s3://example.test"


def test_contract_rejects_shape_source_and_threshold_errors(tmp_path):
    missing = _contract(tmp_path)
    text = missing.read_text().replace(",algorithm_version,", ",")
    missing.write_text(text)
    with pytest.raises(ValueError, match="missing columns"):
        settings.load_plumegraph_contract(missing)

    duplicate = _contract(tmp_path)
    duplicate.write_text(
        duplicate.read_text() + duplicate.read_text().splitlines()[1] + "\n"
    )
    with pytest.raises(ValueError, match="exactly one"):
        settings.load_plumegraph_contract(duplicate)

    with pytest.raises(ValueError, match="invalid numeric"):
        settings.load_plumegraph_contract(_contract(tmp_path, max_cloud_fraction="bad"))
    with pytest.raises(ValueError, match="pinned TEMPO"):
        settings.load_plumegraph_contract(_contract(tmp_path, collection_version="V03"))
    with pytest.raises(ValueError, match="between 0 and 1"):
        settings.load_plumegraph_contract(_contract(tmp_path, max_cloud_fraction=2))
    with pytest.raises(ValueError, match="seed radius"):
        settings.load_plumegraph_contract(_contract(tmp_path, seed_radius_km=101))
    with pytest.raises(ValueError, match="annulus"):
        settings.load_plumegraph_contract(_contract(tmp_path, background_inner_km=100))
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        settings.load_plumegraph_contract(_contract(tmp_path, likely_probability=2))
    with pytest.raises(ValueError, match="attribution weights"):
        settings.load_plumegraph_contract(_contract(tmp_path, trajectory_weight=2))
    with pytest.raises(ValueError, match="positive policy"):
        settings.load_plumegraph_contract(
            _contract(tmp_path, temperature_search_step=0)
        )
    with pytest.raises(ValueError, match="must not be empty"):
        settings.load_plumegraph_contract(_contract(tmp_path, wind_levels_m=""))
