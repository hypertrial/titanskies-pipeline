import pytest

from titanskies_pipeline.orchestration.scope_registry import (
    SHIPPED_SCOPE_SPECS,
    TEMPO_NO2_SCOPE,
    TEMPO_NO2_STD_SCOPE,
    get_scope_spec,
)


def test_get_scope_spec_unknown():
    with pytest.raises(ValueError, match="Unknown scope"):
        get_scope_spec("bad-scope")


def test_job_for_step_names():
    assert TEMPO_NO2_SCOPE.job_for_step("discovery") == "tempo_no2_granule_discovery"
    assert TEMPO_NO2_SCOPE.job_for_step("ingest") == "tempo_no2_hourly_ingest"
    assert TEMPO_NO2_SCOPE.job_for_step("dbt") == "tempo_no2_dbt_build"
    assert TEMPO_NO2_SCOPE.job_for_step("full") == "tempo_no2_full_pipeline"


def test_iter_scope_specs_filters_by_source():
    from titanskies_pipeline.orchestration.scope_registry import iter_scope_specs

    assert iter_scope_specs(source="tempo") == iter_scope_specs()
    assert iter_scope_specs(source="missing") == ()
    assert TEMPO_NO2_SCOPE.key == "tempo:no2"
    assert TEMPO_NO2_SCOPE.namespace == "tempo_no2"
    assert TEMPO_NO2_SCOPE.supported_steps == ("discovery", "ingest", "dbt", "full")


def test_std_scope_spec_key_and_namespace():
    assert TEMPO_NO2_STD_SCOPE.key == "tempo:no2_std"
    assert TEMPO_NO2_STD_SCOPE.namespace == "tempo_no2_std"
    assert TEMPO_NO2_STD_SCOPE.job_for_step("discovery") == (
        "tempo_no2_std_granule_discovery"
    )
    assert TEMPO_NO2_STD_SCOPE.job_for_step("ingest") == "tempo_no2_std_hourly_ingest"
    assert TEMPO_NO2_STD_SCOPE.job_for_step("dbt") == "tempo_no2_std_dbt_build"
    assert TEMPO_NO2_STD_SCOPE.job_for_step("full") == "tempo_no2_std_full_pipeline"


def test_shipped_scope_specs_include_both_families():
    assert SHIPPED_SCOPE_SPECS == (TEMPO_NO2_SCOPE, TEMPO_NO2_STD_SCOPE)
