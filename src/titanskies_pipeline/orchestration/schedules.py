from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from titanskies_pipeline.config.settings_tempo import get_tempo_scope_settings
from titanskies_pipeline.orchestration.config import full_pipeline_run_config
from titanskies_pipeline.orchestration.jobs import SCOPE_JOBS
from titanskies_pipeline.orchestration.scope_registry import (
    SHIPPED_SCOPE_SPECS,
    TEMPO_NO2_SCOPE,
    TEMPO_NO2_STD_SCOPE,
    ScopeSpec,
)


def _build_full_pipeline_schedule(spec: ScopeSpec) -> ScheduleDefinition:
    enabled = get_tempo_scope_settings(spec.scope).schedule_enabled
    return ScheduleDefinition(
        name=spec.schedule_name,
        job=SCOPE_JOBS[spec.full_job_name],
        cron_schedule=spec.schedule_cron,
        run_config=full_pipeline_run_config(spec),
        default_status=(
            DefaultScheduleStatus.RUNNING if enabled else DefaultScheduleStatus.STOPPED
        ),
        description=spec.schedule_description,
    )


_SCOPE_SCHEDULES = {
    spec.schedule_name: _build_full_pipeline_schedule(spec)
    for spec in SHIPPED_SCOPE_SPECS
}

tempo_no2_hourly_pipeline_schedule = _SCOPE_SCHEDULES[TEMPO_NO2_SCOPE.schedule_name]
tempo_no2_std_pipeline_schedule = _SCOPE_SCHEDULES[TEMPO_NO2_STD_SCOPE.schedule_name]

__all__ = ["tempo_no2_hourly_pipeline_schedule", "tempo_no2_std_pipeline_schedule"]
