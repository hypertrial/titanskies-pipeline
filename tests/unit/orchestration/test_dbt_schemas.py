from __future__ import annotations

from dagster import AssetKey

from titanskies_pipeline.storage.duckdb.schemas import dbt_schemas


def test_resolve_source_slug_tempo_and_fallback():
    assert (
        dbt_schemas.resolve_source_slug(
            {"name": "stg_tempo_no2_granule_inventory"},
            fqn=[
                "titanskies",
                "tempo_no2",
                "staging",
                "stg_tempo_no2_granule_inventory",
            ],
        )
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "int_tempo_no2_region_hourly"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "tempo_no2_region_latest"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "tempo_region_registry"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "other_model"})
        == dbt_schemas.DBT_FALLBACK_SCHEMA
    )


def test_resolve_source_slug_prefers_std_over_nrt_by_name():
    assert (
        dbt_schemas.resolve_source_slug({"name": "stg_tempo_no2_std_region_registry"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2_STD
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "int_tempo_no2_std_region_hourly"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2_STD
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "tempo_no2_std_region_hourly"})
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2_STD
    )


def test_resolve_source_slug_prefers_std_over_nrt_by_fqn():
    assert (
        dbt_schemas.resolve_source_slug(
            {"name": "stg_tempo_no2_std_region_registry"},
            fqn=[
                "titanskies",
                "tempo_no2_std",
                "staging",
                "stg_tempo_no2_std_region_registry",
            ],
        )
        == dbt_schemas.DBT_SOURCE_TEMPO_NO2_STD
    )


def test_dbt_model_asset_key_tempo_std_layers():
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "stg_tempo_no2_std_region_registry"}
    ) == AssetKey(["tempo", "no2_std", "staging", "region_registry"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "int_tempo_no2_std_region_anomalies"}
    ) == AssetKey(["tempo", "no2_std", "intermediate", "region_anomalies"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_std_region_hourly"}
    ) == AssetKey(["tempo", "no2_std", "marts", "region_hourly"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_std_data_quality"}
    ) == AssetKey(["tempo", "no2_std", "observability", "data_quality"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_std_granule_observability"}
    ) == AssetKey(["tempo", "no2_std", "observability", "granule_observability"])


def test_tempo_no2_std_subject_fallback_when_prefix_unmatched():
    assert dbt_schemas._tempo_no2_std_subject("unrelated_model") == "unrelated_model"


def test_dbt_model_asset_key_tempo_layers():
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "stg_tempo_no2_region_registry"}
    ) == AssetKey(["tempo", "no2", "staging", "region_registry"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "int_tempo_no2_region_anomalies"}
    ) == AssetKey(["tempo", "no2", "intermediate", "region_anomalies"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_region_hourly"}
    ) == AssetKey(["tempo", "no2", "marts", "region_hourly"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_data_quality"}
    ) == AssetKey(["tempo", "no2", "observability", "data_quality"])
    assert dbt_schemas.dbt_model_asset_key(
        {"name": "tempo_no2_granule_observability"}
    ) == AssetKey(["tempo", "no2", "observability", "granule_observability"])


def test_dbt_model_asset_key_fallback():
    assert dbt_schemas.dbt_model_asset_key({"name": "other_model"}) == AssetKey(
        "other_model"
    )


def test_modeled_schema_constants():
    assert dbt_schemas.TEMPO_NO2_STAGING_SCHEMA == "tempo_no2_staging"
    assert dbt_schemas.TEMPO_NO2_MARTS_SCHEMA == "tempo_no2_marts"
    assert dbt_schemas.DBT_MODELED_SCHEMAS


def test_std_modeled_schema_constants():
    assert dbt_schemas.TEMPO_NO2_STD_STAGING_SCHEMA == "tempo_no2_std_staging"
    assert dbt_schemas.TEMPO_NO2_STD_MARTS_SCHEMA == "tempo_no2_std_marts"
    assert dbt_schemas.TEMPO_NO2_STD_STAGING_SCHEMA in dbt_schemas.DBT_MODELED_SCHEMAS
    assert dbt_schemas.TEMPO_NO2_STD_MARTS_SCHEMA in dbt_schemas.DBT_MODELED_SCHEMAS
