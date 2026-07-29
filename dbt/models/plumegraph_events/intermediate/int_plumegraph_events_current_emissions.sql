{{
  config(
    materialized='incremental',
    unique_key='emission_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
  )
}}

with contract as (
    select *
    from {{ ref('plumegraph_events_contract') }}
    where contract_key = 'default'
),

ranked as (
    select
        emissions.*,
        row_number() over (
            partition by emissions.emission_id
            order by
                emissions.source_revision_at desc nulls last,
                emissions.collected_at desc,
                emissions.emission_revision_id desc
        ) as revision_rank
    from {{ ref('stg_plumegraph_events_emission_revisions') }} as emissions
    {% if is_incremental() %}
    where
        (select contract_version from contract) != (
            select coalesce(max(contract_version), '')
            from {{ this }}
        )
        or emissions.emission_id in (
            select distinct candidate.emission_id
            from {{ ref('stg_plumegraph_events_emission_revisions') }} as candidate
            left join {{ this }} as current
                on candidate.emission_id = current.emission_id
            where
                current.emission_id is null
                or (
                    coalesce(
                        candidate.source_revision_at,
                        timestamp '0001-01-01'
                    ),
                    candidate.collected_at,
                    candidate.emission_revision_id
                ) > (
                    coalesce(
                        current.source_revision_at,
                        timestamp '0001-01-01'
                    ),
                    current.collected_at,
                    current.emission_revision_id
                )
        )
    {% endif %}
),

current_revisions as (
    select * exclude (revision_rank)
    from ranked
    where revision_rank = 1
)

select
    current_revisions.*,
    contract.contract_version
from current_revisions
cross join contract
