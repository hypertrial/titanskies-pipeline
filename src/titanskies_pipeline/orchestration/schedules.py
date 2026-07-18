from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from titanskies_pipeline.config.settings import (
    TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED,
    TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED,
)
from titanskies_pipeline.orchestration.config import (
    tempo_no2_full_pipeline_run_config,
    tempo_no2_std_full_pipeline_run_config,
)
from titanskies_pipeline.orchestration.jobs import (
    tempo_no2_full_pipeline,
    tempo_no2_std_full_pipeline,
)

tempo_no2_hourly_pipeline_schedule = ScheduleDefinition(
    name="tempo_no2_hourly_pipeline_schedule",
    job=tempo_no2_full_pipeline,
    cron_schedule="0 * * * *",
    run_config=tempo_no2_full_pipeline_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly TEMPO NO2 discovery, exact-hour ingestion, and dbt publication. "
        "Controlled by TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED."
    ),
)

tempo_no2_std_pipeline_schedule = ScheduleDefinition(
    name="tempo_no2_std_pipeline_schedule",
    job=tempo_no2_std_full_pipeline,
    cron_schedule="30 * * * *",
    run_config=tempo_no2_std_full_pipeline_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "TEMPO NO2 standard (V04) discovery, exact-hour ingestion, and dbt "
        "publication. Runs on a wider lookback window than NRT because standard "
        "granules settle more slowly. Controlled by "
        "TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED."
    ),
)

__all__ = ["tempo_no2_hourly_pipeline_schedule", "tempo_no2_std_pipeline_schedule"]
