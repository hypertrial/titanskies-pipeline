"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

from titanskies_pipeline.naming import (
    SCOPE_EVENTS,
    SCOPE_NO2,
    SCOPE_NO2_STD,
    SOURCE_PLUMEGRAPH,
    SOURCE_RIVERPULSE,
    SOURCE_TEMPO,
    asset_key,
    schema_name,
)

DBT_SOURCE_TEMPO_NO2: Final = "tempo_no2"
DBT_SOURCE_TEMPO_NO2_STD: Final = "tempo_no2_std"
DBT_SOURCE_RIVERPULSE_EVENTS: Final = "riverpulse_events"
DBT_SOURCE_PLUMEGRAPH_EVENTS: Final = "plumegraph_events"

TEMPO_NO2_STAGING_SCHEMA: Final = schema_name(SOURCE_TEMPO, SCOPE_NO2, "staging")
TEMPO_NO2_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2, "intermediate"
)
TEMPO_NO2_MARTS_SCHEMA: Final = schema_name(SOURCE_TEMPO, SCOPE_NO2, "marts")
TEMPO_NO2_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2, "observability"
)

TEMPO_NO2_STD_STAGING_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2_STD, "staging"
)
TEMPO_NO2_STD_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2_STD, "intermediate"
)
TEMPO_NO2_STD_MARTS_SCHEMA: Final = schema_name(SOURCE_TEMPO, SCOPE_NO2_STD, "marts")
TEMPO_NO2_STD_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2_STD, "observability"
)
RIVERPULSE_EVENTS_STAGING_SCHEMA: Final = schema_name(
    SOURCE_RIVERPULSE, SCOPE_EVENTS, "staging"
)
RIVERPULSE_EVENTS_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_RIVERPULSE, SCOPE_EVENTS, "intermediate"
)
RIVERPULSE_EVENTS_MARTS_SCHEMA: Final = schema_name(
    SOURCE_RIVERPULSE, SCOPE_EVENTS, "marts"
)
RIVERPULSE_EVENTS_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_RIVERPULSE, SCOPE_EVENTS, "observability"
)
PLUMEGRAPH_EVENTS_STAGING_SCHEMA: Final = schema_name(
    SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "staging"
)
PLUMEGRAPH_EVENTS_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "intermediate"
)
PLUMEGRAPH_EVENTS_MARTS_SCHEMA: Final = schema_name(
    SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "marts"
)
PLUMEGRAPH_EVENTS_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "observability"
)

DBT_FALLBACK_SCHEMA: Final = "dbt"

TEMPO_NO2_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "tempo_no2_data_quality",
    "tempo_no2_granule_observability",
)

TEMPO_NO2_STD_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "tempo_no2_std_data_quality",
    "tempo_no2_std_granule_observability",
)
RIVERPULSE_EVENTS_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "riverpulse_events_request_health",
    "riverpulse_events_scientific_quality_issues",
)
PLUMEGRAPH_EVENTS_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "plumegraph_events_benchmark_metrics",
    "plumegraph_events_calibration_state",
    "plumegraph_events_data_quality_issues",
    "plumegraph_events_partition_completeness",
    "plumegraph_events_release_integrity",
    "plumegraph_events_request_health",
    "plumegraph_events_source_revisions",
)

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    TEMPO_NO2_STAGING_SCHEMA,
    TEMPO_NO2_INTERMEDIATE_SCHEMA,
    TEMPO_NO2_MARTS_SCHEMA,
    TEMPO_NO2_OBSERVABILITY_SCHEMA,
    TEMPO_NO2_STD_STAGING_SCHEMA,
    TEMPO_NO2_STD_INTERMEDIATE_SCHEMA,
    TEMPO_NO2_STD_MARTS_SCHEMA,
    TEMPO_NO2_STD_OBSERVABILITY_SCHEMA,
    RIVERPULSE_EVENTS_STAGING_SCHEMA,
    RIVERPULSE_EVENTS_INTERMEDIATE_SCHEMA,
    RIVERPULSE_EVENTS_MARTS_SCHEMA,
    RIVERPULSE_EVENTS_OBSERVABILITY_SCHEMA,
    PLUMEGRAPH_EVENTS_STAGING_SCHEMA,
    PLUMEGRAPH_EVENTS_INTERMEDIATE_SCHEMA,
    PLUMEGRAPH_EVENTS_MARTS_SCHEMA,
    PLUMEGRAPH_EVENTS_OBSERVABILITY_SCHEMA,
)


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    path_fqn = list(fqn or props.get("fqn") or [])
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_PLUMEGRAPH_EVENTS:
        return DBT_SOURCE_PLUMEGRAPH_EVENTS
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_RIVERPULSE_EVENTS:
        return DBT_SOURCE_RIVERPULSE_EVENTS
    # Standard-scope folder is a longer, more specific prefix of the NRT folder
    # name, so it must be checked first to avoid the NRT branch shadowing it.
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_TEMPO_NO2_STD:
        return DBT_SOURCE_TEMPO_NO2_STD
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_TEMPO_NO2:
        return DBT_SOURCE_TEMPO_NO2
    name = str(props.get("name") or "")
    if name.startswith(
        (
            "stg_plumegraph_events_",
            "int_plumegraph_events_",
            "plumegraph_events_",
        )
    ):
        return DBT_SOURCE_PLUMEGRAPH_EVENTS
    if name.startswith(
        (
            "stg_riverpulse_events_",
            "int_riverpulse_events_",
            "riverpulse_events_",
        )
    ):
        return DBT_SOURCE_RIVERPULSE_EVENTS
    if name.startswith(
        (
            "stg_tempo_no2_std_",
            "int_tempo_no2_std_",
            "tempo_no2_std_",
        )
    ):
        return DBT_SOURCE_TEMPO_NO2_STD
    if name.startswith(
        (
            "stg_tempo_no2_",
            "int_tempo_no2_",
            "tempo_no2_",
            "tempo_region_registry",
        )
    ):
        return DBT_SOURCE_TEMPO_NO2
    return DBT_FALLBACK_SCHEMA


def _tempo_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
    observability_models: Sequence[str],
    staging_prefix: str,
    intermediate_prefix: str,
) -> str:
    path_fqn = list(fqn or (props or {}).get("fqn") or [])
    for segment in path_fqn:
        if segment in {"staging", "intermediate", "marts", "observability"}:
            return segment
    if model_name.startswith(staging_prefix):
        return "staging"
    if model_name.startswith(intermediate_prefix):
        return "intermediate"
    if model_name in observability_models:
        return "observability"
    return "marts"


def _tempo_no2_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _tempo_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=TEMPO_NO2_OBSERVABILITY_MODELS,
        staging_prefix="stg_tempo_no2_",
        intermediate_prefix="int_tempo_no2_",
    )


def _tempo_no2_std_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _tempo_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=TEMPO_NO2_STD_OBSERVABILITY_MODELS,
        staging_prefix="stg_tempo_no2_std_",
        intermediate_prefix="int_tempo_no2_std_",
    )


def _tempo_no2_subject(model_name: str) -> str:
    for prefix in ("stg_tempo_no2_", "int_tempo_no2_", "tempo_no2_", "tempo_"):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def _tempo_no2_std_subject(model_name: str) -> str:
    for prefix in ("stg_tempo_no2_std_", "int_tempo_no2_std_", "tempo_no2_std_"):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def _riverpulse_events_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _tempo_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=RIVERPULSE_EVENTS_OBSERVABILITY_MODELS,
        staging_prefix="stg_riverpulse_events_",
        intermediate_prefix="int_riverpulse_events_",
    )


def _riverpulse_events_subject(model_name: str) -> str:
    for prefix in (
        "stg_riverpulse_events_",
        "int_riverpulse_events_",
        "riverpulse_events_",
    ):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def _plumegraph_events_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _tempo_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=PLUMEGRAPH_EVENTS_OBSERVABILITY_MODELS,
        staging_prefix="stg_plumegraph_events_",
        intermediate_prefix="int_plumegraph_events_",
    )


def _plumegraph_events_subject(model_name: str) -> str:
    for prefix in (
        "stg_plumegraph_events_",
        "int_plumegraph_events_",
        "plumegraph_events_",
    ):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def dbt_model_asset_key(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    source = resolve_source_slug(props, fqn=fqn)
    name = str(props.get("name") or "")
    if source == DBT_SOURCE_PLUMEGRAPH_EVENTS:
        return asset_key(
            SOURCE_PLUMEGRAPH,
            SCOPE_EVENTS,
            _plumegraph_events_layer(name, props, fqn=fqn),
            _plumegraph_events_subject(name),
        )
    if source == DBT_SOURCE_RIVERPULSE_EVENTS:
        return asset_key(
            SOURCE_RIVERPULSE,
            SCOPE_EVENTS,
            _riverpulse_events_layer(name, props, fqn=fqn),
            _riverpulse_events_subject(name),
        )
    if source == DBT_SOURCE_TEMPO_NO2_STD:
        return asset_key(
            SOURCE_TEMPO,
            SCOPE_NO2_STD,
            _tempo_no2_std_layer(name, props, fqn=fqn),
            _tempo_no2_std_subject(name),
        )
    if source == DBT_SOURCE_TEMPO_NO2:
        return asset_key(
            SOURCE_TEMPO,
            SCOPE_NO2,
            _tempo_no2_layer(name, props, fqn=fqn),
            _tempo_no2_subject(name),
        )
    return AssetKey(name)


__all__ = [
    "DBT_FALLBACK_SCHEMA",
    "DBT_MODELED_SCHEMAS",
    "DBT_SOURCE_PLUMEGRAPH_EVENTS",
    "DBT_SOURCE_TEMPO_NO2",
    "DBT_SOURCE_TEMPO_NO2_STD",
    "DBT_SOURCE_RIVERPULSE_EVENTS",
    "PLUMEGRAPH_EVENTS_INTERMEDIATE_SCHEMA",
    "PLUMEGRAPH_EVENTS_MARTS_SCHEMA",
    "PLUMEGRAPH_EVENTS_OBSERVABILITY_SCHEMA",
    "PLUMEGRAPH_EVENTS_STAGING_SCHEMA",
    "RIVERPULSE_EVENTS_INTERMEDIATE_SCHEMA",
    "RIVERPULSE_EVENTS_MARTS_SCHEMA",
    "RIVERPULSE_EVENTS_OBSERVABILITY_SCHEMA",
    "RIVERPULSE_EVENTS_STAGING_SCHEMA",
    "TEMPO_NO2_INTERMEDIATE_SCHEMA",
    "TEMPO_NO2_MARTS_SCHEMA",
    "TEMPO_NO2_OBSERVABILITY_SCHEMA",
    "TEMPO_NO2_STAGING_SCHEMA",
    "TEMPO_NO2_STD_INTERMEDIATE_SCHEMA",
    "TEMPO_NO2_STD_MARTS_SCHEMA",
    "TEMPO_NO2_STD_OBSERVABILITY_SCHEMA",
    "TEMPO_NO2_STD_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "resolve_source_slug",
]
