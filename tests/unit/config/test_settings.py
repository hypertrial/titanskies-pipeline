import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from titanskies_pipeline.config._reload_settings import reload_all_settings_modules
from titanskies_pipeline.config.settings_tempo import (
    load_tempo_no2_contract,
    resolve_geo_artifact_path,
)


def test_env_int_invalid_falls_back(monkeypatch, isolated_env):
    monkeypatch.setenv("TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS", "bad")
    settings = reload_all_settings_modules()
    assert settings.TEMPO_NO2_DISCOVERY_LOOKBACK_HOURS == 8


def test_duckdb_path_env_overrides_name(monkeypatch, tmp_path, isolated_env):
    db = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", "ignored.duckdb")
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    settings = reload_all_settings_modules()
    assert settings.DUCKDB_PATH == db.resolve()


def test_invalid_dbt_profiles_dir_falls_back(monkeypatch, tmp_path, isolated_env):
    bad = tmp_path / "profiles"
    bad.mkdir()
    (bad / "profiles.yml").write_text("other: {}\n")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(bad))
    settings = reload_all_settings_modules()
    assert settings.DBT_PROFILES_DIR == settings.BASE_DIR / "dbt" / "profiles"


def test_load_dotenv_branches(monkeypatch, isolated_env):
    mock_load = MagicMock()
    monkeypatch.setattr("dotenv.load_dotenv", mock_load)
    real_exists = Path.exists

    def exists_stub(self):
        if self.name == ".env":
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", exists_stub)
    reload_all_settings_modules()
    assert not mock_load.called

    def exists_true(self):
        if self.name == ".env":
            return True
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", exists_true)
    reload_all_settings_modules()
    assert mock_load.called


def test_tempo_settings_from_env(monkeypatch, isolated_env):
    monkeypatch.setenv("TEMPO_NO2_CMR_CONCEPT_ID", "concept-test")
    monkeypatch.setenv("TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED", "true")
    settings = reload_all_settings_modules()
    assert settings.TEMPO_NO2_CMR_CONCEPT_ID == "concept-test"
    assert settings.TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED is True
    assert settings.TEMPO_NO2_CONTRACT["accepted_quality_flags"] == "0|1"


def test_load_tempo_contract_validates_shape_and_values(tmp_path):
    contract = tmp_path / "contract.csv"
    contract.write_text(
        "contract_key,contract_version,min_region_coverage,stale_hours_warn,stale_hours_error,"
        "anomaly_baseline_days,anomaly_min_baseline_samples,accepted_quality_flags\n"
        "default,0.3.0,0.5,2,4,14,7,0|2\n"
    )
    assert load_tempo_no2_contract(contract) == {
        "contract_version": "0.3.0",
        "min_region_coverage": 0.5,
        "stale_hours_warn": 2,
        "stale_hours_error": 4,
        "anomaly_baseline_days": 14,
        "anomaly_min_baseline_samples": 7,
        "accepted_quality_flags": "0|2",
    }

    contract.write_text("contract_key,min_region_coverage\ndefault,0.5\n")
    with pytest.raises(ValueError, match="missing columns"):
        load_tempo_no2_contract(contract)


def test_load_tempo_contract_rejects_duplicates_and_invalid_values(tmp_path):
    contract = tmp_path / "contract.csv"
    header = (
        "contract_key,contract_version,min_region_coverage,stale_hours_warn,stale_hours_error,"
        "anomaly_baseline_days,anomaly_min_baseline_samples,accepted_quality_flags\n"
    )
    contract.write_text(
        header + "default,0.3.0,0.5,2,4,14,7,0|1\ndefault,0.3.0,0.5,2,4,14,7,0|1\n"
    )
    with pytest.raises(ValueError, match="exactly one"):
        load_tempo_no2_contract(contract)

    contract.write_text(header + "default,0.3.0,bad,2,4,14,7,0|1\n")
    with pytest.raises(ValueError, match="invalid numeric"):
        load_tempo_no2_contract(contract)

    contract.write_text(header + "default,0.3.0,1.5,2,4,14,7,0|1\n")
    with pytest.raises(ValueError, match="between 0 and 1"):
        load_tempo_no2_contract(contract)

    contract.write_text(header + "default,0.3.0,0.5,4,2,14,7,0|1\n")
    with pytest.raises(ValueError, match="thresholds"):
        load_tempo_no2_contract(contract)


def test_resolve_geo_artifact_path(tmp_path):
    path = resolve_geo_artifact_path(tmp_path / "geo" / "registry.parquet")
    assert path.is_absolute()


def test_dbt_cli_argv_uses_active_interpreter():
    from titanskies_pipeline.config.settings_warehouse import dbt_cli_argv

    assert dbt_cli_argv("parse", "--project-dir", "dbt") == [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "parse",
        "--project-dir",
        "dbt",
    ]


def test_resolve_dbt_executable_prefers_venv(monkeypatch, tmp_path):
    from titanskies_pipeline.config.settings_warehouse import resolve_dbt_executable

    fake_python = tmp_path / "bin" / "python3"
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text("")
    fake_dbt = fake_python.with_name("dbt")
    fake_dbt.write_text("")
    monkeypatch.setattr(
        "titanskies_pipeline.config.settings_warehouse.sys.executable", str(fake_python)
    )
    assert resolve_dbt_executable() == str(fake_dbt)


def test_resolve_dbt_executable_fallback(monkeypatch, tmp_path):
    from titanskies_pipeline.config.settings_warehouse import resolve_dbt_executable

    fake_python = tmp_path / "python3"
    fake_python.write_text("")
    monkeypatch.setattr(
        "titanskies_pipeline.config.settings_warehouse.sys.executable", str(fake_python)
    )
    monkeypatch.setattr(
        "titanskies_pipeline.config.settings_warehouse.shutil.which",
        lambda _name: None,
    )
    assert resolve_dbt_executable() == "dbt"


def test_config_init_exports(reload_settings):
    import titanskies_pipeline.config as config_pkg

    assert config_pkg.TEMPO_NO2_CMR_CONCEPT_ID
    assert reload_settings.DUCKDB_PATH


def test_earthdata_lowercase_env_aliases(monkeypatch, isolated_env):
    monkeypatch.delenv("EARTHDATA_USERNAME", raising=False)
    monkeypatch.delenv("EARTHDATA_PASSWORD", raising=False)
    monkeypatch.setenv("earthdata_username", "lower-user")
    monkeypatch.setenv("earthdata_password", "lower-pass")
    reload_all_settings_modules()
    import os

    assert os.environ["EARTHDATA_USERNAME"] == "lower-user"
    assert os.environ["EARTHDATA_PASSWORD"] == "lower-pass"
