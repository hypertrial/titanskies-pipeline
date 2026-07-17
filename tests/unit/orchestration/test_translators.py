from __future__ import annotations

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import AssetKey

from titanskies_pipeline.orchestration.dbt_project import DBT_DAGSTER_GROUP_NAME
from titanskies_pipeline.orchestration.translators import TempoDagsterDbtTranslator


def test_dbt_translator_does_not_override_model_dependencies():
    assert "get_asset_spec" not in TempoDagsterDbtTranslator.__dict__


def test_dbt_translator_enables_source_visibility_settings():
    settings = TempoDagsterDbtTranslator().settings
    assert settings.enable_duplicate_source_asset_keys is True
    assert settings.enable_source_metadata is True
    assert settings.enable_source_tests_as_checks is True


def test_dbt_translator_group_name():
    translator = TempoDagsterDbtTranslator()
    assert translator.get_group_name({"name": "stg_tempo_no2_markets"}) == (
        DBT_DAGSTER_GROUP_NAME
    )


def test_dbt_translator_uses_meta_asset_key_when_present():
    translator = TempoDagsterDbtTranslator()
    props = {
        "name": "ignored",
        "meta": {"dagster": {"asset_key": ["tempo", "no2", "custom", "key"]}},
    }
    assert translator.get_asset_key(props) == AssetKey(
        ["tempo", "no2", "custom", "key"]
    )


def test_dbt_translator_resolves_tempo_model_asset_key():
    translator = TempoDagsterDbtTranslator()
    key = translator.get_asset_key(
        {
            "name": "stg_tempo_no2_granule_inventory",
            "fqn": [
                "titanskies",
                "tempo_no2",
                "staging",
                "stg_tempo_no2_granule_inventory",
            ],
        }
    )
    assert key == AssetKey(["tempo", "no2", "staging", "granule_inventory"])
