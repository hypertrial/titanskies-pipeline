with contract as (
    select *
    from {{ ref('riverpulse_events_contract') }}
    where contract_key = 'default'
)

select
    discharges.*,
    observations.observation_id,
    observations.reach_id,
    observations.observation_time,
    observations.cycle_id,
    observations.pass_id,
    observations.source_ingest_time,
    observations.collected_at,
    contract.contract_version,
    sha256(
        observations.observation_id
        || '|' || discharges.observation_revision_id
        || '|' || discharges.algorithm
        || '|' || cast(discharges.is_constrained as varchar)
    ) as discharge_revision_id,
    case
        when discharges.is_constrained then 'constrained'
        else 'unconstrained'
    end as discharge_variant,
    coalesce(
        discharges.collection_name = contract.collection_name
        and discharges.collection_version = contract.collection_version
        and discharges.sword_version = contract.sword_version
        and discharges.discharge_quality = contract.accepted_discharge_quality
        and isfinite(discharges.discharge_value)
        and isfinite(discharges.discharge_uncertainty),
        false
    ) as is_discharge_ready
from {{ ref('stg_riverpulse_events_discharge_revisions') }} as discharges
inner join {{ ref('stg_riverpulse_events_observation_revisions') }} as observations
    on discharges.observation_revision_id = observations.observation_revision_id
cross join contract
