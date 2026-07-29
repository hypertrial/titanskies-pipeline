{{
  config(
    materialized='incremental',
    unique_key='observation_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
  )
}}

with contract as (
    select *
    from {{ ref('riverpulse_events_contract') }}
    where contract_key = 'default'
),

ranked as (
    select
        observations.*,
        row_number() over (
            partition by observations.observation_id
            order by
                observations.source_ingest_time desc,
                observations.collected_at desc,
                observations.observation_revision_id desc
        ) as revision_rank
    from {{ ref('stg_riverpulse_events_observation_revisions') }} as observations
),

current_revisions as (
    select * exclude (revision_rank)
    from ranked
    where revision_rank = 1
)

select
    current_revisions.*,
    contract.contract_version,
    contract.field_contract_version
from current_revisions
cross join contract
{% if is_incremental() %}
where
    current_revisions.collected_at >= (
        select coalesce(max(collected_at), timestamp '1970-01-01')
        from {{ this }}
    )
    or contract.contract_version != (
        select coalesce(max(contract_version), '')
        from {{ this }}
    )
{% endif %}
