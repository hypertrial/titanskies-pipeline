select
    grid_row,
    grid_col,
    latitude,
    longitude,
    cell_area_km2,
    cast(observation_time as timestamp) as observation_time,
    cast(observation_hour as timestamp) as observation_hour,
    no2,
    quality_flag,
    quality_flag_accepted,
    granule_id,
    ingested_at
from {{ source('tempo_no2_std_raw', 'grid_latest') }}
