from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from titanskies_pipeline.config.settings_riverpulse import (
    get_riverpulse_settings,
)
from titanskies_pipeline.config.settings_tempo import get_tempo_scope_settings
from titanskies_pipeline.orchestration.config import (
    full_pipeline_run_config,
    riverpulse_events_full_pipeline_run_config,
)
from titanskies_pipeline.orchestration.jobs import (
    SCOPE_JOBS,
    riverpulse_events_full_pipeline,
)
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

riverpulse_events_pipeline_schedule = ScheduleDefinition(
    name="riverpulse_events_pipeline_schedule",
    job=riverpulse_events_full_pipeline,
    cron_schedule="0 3 * * 0",
    execution_timezone="UTC",
    run_config=riverpulse_events_full_pipeline_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if get_riverpulse_settings().schedule_enabled
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Weekly Version D Hydrocron discovery, observation ingestion, and "
        "RiverPulse publication; network bootstrap is intentionally excluded."
    ),
)

__all__ = [
    "riverpulse_events_pipeline_schedule",
    "tempo_no2_hourly_pipeline_schedule",
    "tempo_no2_std_pipeline_schedule",
]
