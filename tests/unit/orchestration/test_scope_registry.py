from titanskies_pipeline.orchestration.scope_registry import (
    SCOPE_STEPS,
    TEMPO_NO2_SCOPE,
    TEMPO_NO2_STD_SCOPE,
    get_scope_spec,
)


def test_scope_registry_jobs():
    assert TEMPO_NO2_SCOPE.discovery_job_name == "tempo_no2_granule_discovery"
    assert TEMPO_NO2_SCOPE.ingest_job_name == "tempo_no2_hourly_ingest"
    assert TEMPO_NO2_SCOPE.dbt_job_name == "tempo_no2_dbt_build"
    assert TEMPO_NO2_SCOPE.full_job_name == "tempo_no2_full_pipeline"


def test_std_scope_registry_jobs():
    assert TEMPO_NO2_STD_SCOPE.discovery_job_name == "tempo_no2_std_granule_discovery"
    assert TEMPO_NO2_STD_SCOPE.ingest_job_name == "tempo_no2_std_hourly_ingest"
    assert TEMPO_NO2_STD_SCOPE.dbt_job_name == "tempo_no2_std_dbt_build"
    assert TEMPO_NO2_STD_SCOPE.full_job_name == "tempo_no2_std_full_pipeline"
    assert TEMPO_NO2_STD_SCOPE.label == "TEMPO NO2 Standard"
    assert TEMPO_NO2_STD_SCOPE.dbt_select == "+tag:tempo,tag:no2_std"


def test_get_scope_spec_aliases():
    assert get_scope_spec("tempo:no2") is TEMPO_NO2_SCOPE
    assert get_scope_spec("tempo_no2") is TEMPO_NO2_SCOPE
    assert get_scope_spec("tempo:no2_std") is TEMPO_NO2_STD_SCOPE
    assert get_scope_spec("tempo_no2_std") is TEMPO_NO2_STD_SCOPE


def test_scope_steps():
    assert "full" in SCOPE_STEPS
