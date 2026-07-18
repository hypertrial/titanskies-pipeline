from dagster import Definitions
from dagster_dbt import DbtCliResource

from titanskies_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
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
    tempo_no2_dbt_build,
    tempo_no2_full_pipeline,
    tempo_no2_granule_discovery,
    tempo_no2_hourly_ingest,
    tempo_no2_std_dbt_build,
    tempo_no2_std_full_pipeline,
    tempo_no2_std_granule_discovery,
    tempo_no2_std_hourly_ingest,
)
from titanskies_pipeline.orchestration.schedules import (
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
    ],
    schedules=[tempo_no2_hourly_pipeline_schedule, tempo_no2_std_pipeline_schedule],
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
