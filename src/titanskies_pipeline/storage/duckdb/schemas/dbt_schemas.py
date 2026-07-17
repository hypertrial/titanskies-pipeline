"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

from titanskies_pipeline.naming import SCOPE_NO2, SOURCE_TEMPO, asset_key, schema_name

DBT_SOURCE_TEMPO_NO2: Final = "tempo_no2"

TEMPO_NO2_STAGING_SCHEMA: Final = schema_name(SOURCE_TEMPO, SCOPE_NO2, "staging")
TEMPO_NO2_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2, "intermediate"
)
TEMPO_NO2_MARTS_SCHEMA: Final = schema_name(SOURCE_TEMPO, SCOPE_NO2, "marts")
TEMPO_NO2_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_TEMPO, SCOPE_NO2, "observability"
)
DBT_FALLBACK_SCHEMA: Final = "dbt"

TEMPO_NO2_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "tempo_no2_data_quality",
    "tempo_no2_granule_observability",
)

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    TEMPO_NO2_STAGING_SCHEMA,
    TEMPO_NO2_INTERMEDIATE_SCHEMA,
    TEMPO_NO2_MARTS_SCHEMA,
    TEMPO_NO2_OBSERVABILITY_SCHEMA,
)


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    path_fqn = list(fqn or props.get("fqn") or [])
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_TEMPO_NO2:
        return DBT_SOURCE_TEMPO_NO2
    name = str(props.get("name") or "")
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


def _tempo_no2_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    path_fqn = list(fqn or (props or {}).get("fqn") or [])
    for segment in path_fqn:
        if segment in {"staging", "intermediate", "marts", "observability"}:
            return segment
    if model_name.startswith("stg_tempo_no2_"):
        return "staging"
    if model_name.startswith("int_tempo_no2_"):
        return "intermediate"
    if model_name in TEMPO_NO2_OBSERVABILITY_MODELS:
        return "observability"
    return "marts"


def _tempo_no2_subject(model_name: str) -> str:
    for prefix in ("stg_tempo_no2_", "int_tempo_no2_", "tempo_no2_", "tempo_"):
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
    "DBT_SOURCE_TEMPO_NO2",
    "TEMPO_NO2_INTERMEDIATE_SCHEMA",
    "TEMPO_NO2_MARTS_SCHEMA",
    "TEMPO_NO2_OBSERVABILITY_SCHEMA",
    "TEMPO_NO2_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "resolve_source_slug",
]
