from dagster import AssetKey

from titanskies_pipeline.storage.duckdb.schemas import dbt_schemas


def test_dbt_model_asset_key_custom_subject():
    key = dbt_schemas.dbt_model_asset_key(
        {"name": "custom_surface"},
        fqn=["titanskies", "tempo_no2", "marts", "custom_surface"],
    )
    assert key == AssetKey(["tempo", "no2", "marts", "custom_surface"])
