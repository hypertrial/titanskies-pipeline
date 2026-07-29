{{
  config(
    materialized='incremental',
    unique_key='pixel_id',
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
        pixels.*,
        row_number() over (
            partition by pixels.pixel_id
            order by
                pixels.source_revision_at desc nulls last,
                pixels.collected_at desc,
                pixels.pixel_revision_id desc
        ) as revision_rank
    from {{ ref('stg_plumegraph_events_pixel_revisions') }} as pixels
    {% if is_incremental() %}
    where
        (select contract_version from contract) != (
            select coalesce(max(contract_version), '')
            from {{ this }}
        )
        or (select algorithm_version from contract) != (
            select coalesce(max(algorithm_version), '')
            from {{ this }}
        )
        or pixels.pixel_id in (
            select distinct candidate.pixel_id
            from {{ ref('stg_plumegraph_events_pixel_revisions') }} as candidate
            left join {{ this }} as current
                on candidate.pixel_id = current.pixel_id
            where
                current.pixel_id is null
                or (
                    coalesce(
                        candidate.source_revision_at,
                        timestamp '0001-01-01'
                    ),
                    candidate.collected_at,
                    candidate.pixel_revision_id
                ) > (
                    coalesce(
                        current.source_revision_at,
                        timestamp '0001-01-01'
                    ),
                    current.collected_at,
                    current.pixel_revision_id
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
    contract.contract_version,
    contract.algorithm_version
from current_revisions
cross join contract
