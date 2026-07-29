{{
  config(
    materialized='incremental',
    unique_key='discharge_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
  )
}}

with current_observations as (
    select *
    from {{ ref('int_riverpulse_events_current_observations') }}
),

current_discharges as (
    select
        discharges.*,
        current_observations.observation_id,
        current_observations.reach_id,
        current_observations.observation_time,
        current_observations.cycle_id,
        current_observations.pass_id,
        current_observations.source_ingest_time,
        current_observations.collected_at,
        current_observations.contract_version,
        sha256(
            current_observations.observation_id
            || '|' || discharges.algorithm
            || '|' || cast(discharges.is_constrained as varchar)
        ) as discharge_id
    from current_observations
    inner join {{ ref('stg_riverpulse_events_discharge_revisions') }} as discharges
        on
            current_observations.observation_revision_id
            = discharges.observation_revision_id
)

select *
from current_discharges
{% if is_incremental() %}
where
    current_discharges.collected_at >= (
        select coalesce(max(collected_at), timestamp '1970-01-01')
        from {{ this }}
    )
    or current_discharges.contract_version != (
        select coalesce(max(contract_version), '')
        from {{ this }}
    )
{% endif %}
