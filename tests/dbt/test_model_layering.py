from pathlib import Path

import pytest

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"

SCOPE_CASES = (
    ("tempo_no2", "tempo_no2", "no2"),
    ("tempo_no2_std", "tempo_no2_std", "no2_std"),
)


@pytest.mark.parametrize("family,prefix,scope_tag", SCOPE_CASES)
def test_staging_aggregates_is_source_conformed(family, prefix, scope_tag):
    sql = (
        DBT_ROOT
        / "models"
        / family
        / "staging"
        / f"stg_{prefix}_region_hour_aggregates.sql"
    ).read_text()
    lowered = sql.lower()
    assert f"{{{{ source('{prefix}_raw', 'region_hour_aggregates') }}}}" in lowered
    assert "is_analysis_ready" not in lowered


@pytest.mark.parametrize("family,prefix,scope_tag", SCOPE_CASES)
def test_intermediate_hourly_owns_analysis_ready_contract(family, prefix, scope_tag):
    sql = (
        DBT_ROOT
        / "models"
        / family
        / "intermediate"
        / f"int_{prefix}_region_hourly.sql"
    ).read_text()
    lowered = sql.lower()
    assert f"{{{{ ref('{prefix}_contract') }}}}" in lowered
    assert "is_analysis_ready" in lowered


@pytest.mark.parametrize("family,prefix,scope_tag", SCOPE_CASES)
def test_marts_region_hourly_is_thin_select(family, prefix, scope_tag):
    sql = (
        DBT_ROOT / "models" / family / "marts" / f"{prefix}_region_hourly.sql"
    ).read_text()
    assert f"{{{{ ref('int_{prefix}_region_hourly') }}}}" in sql
