with boundary_references as (
    select
        from_reach_id as reach_id,
        to_json(
            list(to_reach_id order by to_reach_id)
            filter (where is_selection_boundary)
        ) as network_boundary_reach_ids
    from {{ ref('stg_riverpulse_events_reach_edges') }}
    group by from_reach_id
),

internal_topology as (
    select
        from_reach_id as reach_id,
        to_json(
            list(to_reach_id order by to_reach_id)
            filter (where not is_selection_boundary)
        ) as selected_neighbor_reach_ids
    from {{ ref('stg_riverpulse_events_reach_edges') }}
    group by from_reach_id
)

select
    reaches.network_version,
    reaches.reach_id,
    reaches.basin_key,
    reaches.river_name,
    reaches.reach_length_m,
    reaches.flow_accumulation,
    reaches.distance_to_outlet_m,
    reaches.geometry_wkb,
    reaches.centroid_longitude,
    reaches.centroid_latitude,
    reaches.is_outlet_anchor,
    reaches.loaded_at,
    4326 as geometry_epsg,
    coalesce(internal_topology.selected_neighbor_reach_ids, '[]') as selected_neighbor_reach_ids,
    coalesce(boundary_references.network_boundary_reach_ids, '[]') as network_boundary_reach_ids
from {{ ref('stg_riverpulse_events_reaches') }} as reaches
left join internal_topology
    on reaches.reach_id = internal_topology.reach_id
left join boundary_references
    on reaches.reach_id = boundary_references.reach_id
