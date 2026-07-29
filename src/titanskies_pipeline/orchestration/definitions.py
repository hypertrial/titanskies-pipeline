from dagster import Definitions
from dagster_dbt import DbtCliResource

from titanskies_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from titanskies_pipeline.orchestration.assets_plumegraph_events import (
    plumegraph_events_analysis_results,
    plumegraph_events_ops_facility_registry,
    plumegraph_events_raw_camd_emissions,
    plumegraph_events_raw_hrrr_snapshots,
    plumegraph_events_raw_source_inventory,
    plumegraph_events_raw_tempo_snapshots,
    plumegraph_events_release,
    plumegraph_events_validation,
)
from titanskies_pipeline.orchestration.assets_riverpulse_events import (
    riverpulse_events_ops_network_registry,
    riverpulse_events_raw_observations,
    riverpulse_events_raw_source_inventory,
)
from titanskies_pipeline.orchestration.assets_tempo_no2 import (
    tempo_no2_ops_region_registry,
    tempo_no2_raw_granule_inventory,
    tempo_no2_raw_region_hour_aggregates,
    tempo_no2_std_ops_region_registry,
    tempo_no2_std_raw_granule_inventory,
    tempo_no2_std_raw_region_hour_aggregates,
    titanskies_dbt,
)
from titanskies_pipeline.orchestration.jobs import (
    plumegraph_events_analysis,
    plumegraph_events_dbt_build,
    plumegraph_events_full_pipeline,
    plumegraph_events_release_build,
    plumegraph_events_source_discovery,
    plumegraph_events_source_ingest,
    riverpulse_events_dbt_build,
    riverpulse_events_full_pipeline,
    riverpulse_events_observation_ingest,
    riverpulse_events_source_discovery,
    tempo_no2_dbt_build,
    tempo_no2_full_pipeline,
    tempo_no2_granule_discovery,
    tempo_no2_hourly_ingest,
    tempo_no2_std_dbt_build,
    tempo_no2_std_full_pipeline,
    tempo_no2_std_granule_discovery,
    tempo_no2_std_hourly_ingest,
)
from titanskies_pipeline.orchestration.jobs import (
    plumegraph_events_validation as plumegraph_events_validation_job,
)
from titanskies_pipeline.orchestration.schedules import (
    plumegraph_events_daily_pipeline_schedule,
    riverpulse_events_pipeline_schedule,
    tempo_no2_hourly_pipeline_schedule,
    tempo_no2_std_pipeline_schedule,
)

defs = Definitions(
    assets=[
        tempo_no2_ops_region_registry,
        tempo_no2_raw_granule_inventory,
        tempo_no2_raw_region_hour_aggregates,
        tempo_no2_std_ops_region_registry,
        tempo_no2_std_raw_granule_inventory,
        tempo_no2_std_raw_region_hour_aggregates,
        titanskies_dbt,
        riverpulse_events_ops_network_registry,
        riverpulse_events_raw_source_inventory,
        riverpulse_events_raw_observations,
        plumegraph_events_ops_facility_registry,
        plumegraph_events_raw_source_inventory,
        plumegraph_events_raw_tempo_snapshots,
        plumegraph_events_raw_hrrr_snapshots,
        plumegraph_events_raw_camd_emissions,
        plumegraph_events_analysis_results,
        plumegraph_events_validation,
        plumegraph_events_release,
    ],
    jobs=[
        tempo_no2_granule_discovery,
        tempo_no2_hourly_ingest,
        tempo_no2_dbt_build,
        tempo_no2_full_pipeline,
        tempo_no2_std_granule_discovery,
        tempo_no2_std_hourly_ingest,
        tempo_no2_std_dbt_build,
        tempo_no2_std_full_pipeline,
        riverpulse_events_source_discovery,
        riverpulse_events_observation_ingest,
        riverpulse_events_dbt_build,
        riverpulse_events_full_pipeline,
        plumegraph_events_source_discovery,
        plumegraph_events_source_ingest,
        plumegraph_events_analysis,
        plumegraph_events_dbt_build,
        plumegraph_events_validation_job,
        plumegraph_events_release_build,
        plumegraph_events_full_pipeline,
    ],
    schedules=[
        tempo_no2_hourly_pipeline_schedule,
        tempo_no2_std_pipeline_schedule,
        riverpulse_events_pipeline_schedule,
        plumegraph_events_daily_pipeline_schedule,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="titanskies",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        ),
    },
)
