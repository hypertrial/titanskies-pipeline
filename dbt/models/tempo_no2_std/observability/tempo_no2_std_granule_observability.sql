{{ config(materialized='view') }}

select
    granule_id,
    concept_id,
    discovered_at,
    downloaded_at,
    validated_at,
    processed_at,
    acquisition_start,
    acquisition_end,
    cmr_revision_at,
    last_seen_at,
    observation_time,
    observation_hour,
    checksum_sha256,
    file_size_bytes,
    discovery_status,
    download_status,
    validation_status,
    processing_status,
    error_message,
    date_diff('minute', discovered_at, processed_at) as processing_latency_minutes
from {{ ref('stg_tempo_no2_std_granule_inventory') }}
