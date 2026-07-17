from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"


def test_staging_aggregates_is_source_conformed():
    sql = (
        DBT_ROOT
        / "models"
        / "tempo_no2"
        / "staging"
        / "stg_tempo_no2_region_hour_aggregates.sql"
    ).read_text()
    lowered = sql.lower()
    assert "{{ source('tempo_no2_raw', 'region_hour_aggregates') }}" in lowered
    assert "is_analysis_ready" not in lowered


def test_intermediate_hourly_owns_analysis_ready_contract():
    sql = (
        DBT_ROOT
        / "models"
        / "tempo_no2"
        / "intermediate"
        / "int_tempo_no2_region_hourly.sql"
    ).read_text()
    lowered = sql.lower()
    assert "{{ ref('tempo_no2_contract') }}" in lowered
    assert "is_analysis_ready" in lowered


def test_marts_region_hourly_is_thin_select():
    sql = (
        DBT_ROOT / "models" / "tempo_no2" / "marts" / "tempo_no2_region_hourly.sql"
    ).read_text()
    assert "{{ ref('int_tempo_no2_region_hourly') }}" in sql
