select
    grid_row,
    grid_col,
    latitude,
    longitude,
    cell_area_km2,
    observation_time,
    observation_hour,
    no2,
    quality_flag,
    quality_flag_accepted,
    granule_id,
    ingested_at,
    (quality_flag_accepted and no2 is not null) as is_analysis_ready
from {{ ref('stg_tempo_no2_std_grid_latest') }}
