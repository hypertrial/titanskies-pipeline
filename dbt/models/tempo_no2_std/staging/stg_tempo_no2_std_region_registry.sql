select
    country_code,
    region_type,
    source_region_id,
    canonical_region_id,
    region_name,
    parent_region_id,
    timezone,
    geometry_version,
    geometry_checksum,
    loaded_at
from {{ source('tempo_no2_std_ops', 'region_registry') }}
