"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

from titanskies_pipeline.naming import (
    SCOPE_EVENTS,
    SCOPE_NO2,
    SCOPE_NO2_STD,
    SOURCE_PLUMEGRAPH,
    SOURCE_RIVERPULSE,
    SOURCE_TEMPO,
    schema_name,
)

TITANSKIES_OPS_SCHEMA = "titanskies_ops"
TEMPO_NO2_RAW_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "raw")
TEMPO_NO2_OPS_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "ops")
TEMPO_NO2_STD_RAW_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2_STD, "raw")
TEMPO_NO2_STD_OPS_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2_STD, "ops")
RIVERPULSE_EVENTS_RAW_SCHEMA = schema_name(SOURCE_RIVERPULSE, SCOPE_EVENTS, "raw")
RIVERPULSE_EVENTS_OPS_SCHEMA = schema_name(SOURCE_RIVERPULSE, SCOPE_EVENTS, "ops")
PLUMEGRAPH_EVENTS_RAW_SCHEMA = schema_name(SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "raw")
PLUMEGRAPH_EVENTS_OPS_SCHEMA = schema_name(SOURCE_PLUMEGRAPH, SCOPE_EVENTS, "ops")

_RAW_SCHEMAS_BY_SCOPE = {
    SCOPE_NO2: TEMPO_NO2_RAW_SCHEMA,
    SCOPE_NO2_STD: TEMPO_NO2_STD_RAW_SCHEMA,
}
_OPS_SCHEMAS_BY_SCOPE = {
    SCOPE_NO2: TEMPO_NO2_OPS_SCHEMA,
    SCOPE_NO2_STD: TEMPO_NO2_STD_OPS_SCHEMA,
}
_HOUR_REVISION_SEQUENCES_BY_SCOPE = {
    SCOPE_NO2: "tempo_no2_hour_revision",
    SCOPE_NO2_STD: "tempo_no2_std_hour_revision",
}


def _known_scopes(mapping: dict[str, str]) -> str:
    return ", ".join(sorted(mapping))


def tempo_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def warehouse_ops_tbl(name: str) -> str:
    return tempo_q(TITANSKIES_OPS_SCHEMA, name)


def riverpulse_raw_tbl(name: str) -> str:
    return tempo_q(RIVERPULSE_EVENTS_RAW_SCHEMA, name)


def riverpulse_ops_tbl(name: str) -> str:
    return tempo_q(RIVERPULSE_EVENTS_OPS_SCHEMA, name)


def plumegraph_raw_tbl(name: str) -> str:
    return tempo_q(PLUMEGRAPH_EVENTS_RAW_SCHEMA, name)


def plumegraph_ops_tbl(name: str) -> str:
    return tempo_q(PLUMEGRAPH_EVENTS_OPS_SCHEMA, name)


def tempo_raw_tbl(name: str, *, scope: str = SCOPE_NO2) -> str:
    try:
        schema = _RAW_SCHEMAS_BY_SCOPE[scope]
    except KeyError as exc:
        raise ValueError(
            f"Unknown TEMPO scope {scope!r}; expected one of: "
            f"{_known_scopes(_RAW_SCHEMAS_BY_SCOPE)}"
        ) from exc
    return tempo_q(schema, name)


def tempo_ops_tbl(name: str, *, scope: str = SCOPE_NO2) -> str:
    try:
        schema = _OPS_SCHEMAS_BY_SCOPE[scope]
    except KeyError as exc:
        raise ValueError(
            f"Unknown TEMPO scope {scope!r}; expected one of: "
            f"{_known_scopes(_OPS_SCHEMAS_BY_SCOPE)}"
        ) from exc
    return tempo_q(schema, name)


def hour_revision_sequence(*, scope: str = SCOPE_NO2) -> str:
    try:
        return _HOUR_REVISION_SEQUENCES_BY_SCOPE[scope]
    except KeyError as exc:
        raise ValueError(
            f"Unknown TEMPO scope {scope!r}; expected one of: "
            f"{_known_scopes(_HOUR_REVISION_SEQUENCES_BY_SCOPE)}"
        ) from exc


__all__ = [
    "RIVERPULSE_EVENTS_OPS_SCHEMA",
    "RIVERPULSE_EVENTS_RAW_SCHEMA",
    "PLUMEGRAPH_EVENTS_OPS_SCHEMA",
    "PLUMEGRAPH_EVENTS_RAW_SCHEMA",
    "TEMPO_NO2_OPS_SCHEMA",
    "TEMPO_NO2_RAW_SCHEMA",
    "TEMPO_NO2_STD_OPS_SCHEMA",
    "TEMPO_NO2_STD_RAW_SCHEMA",
    "TITANSKIES_OPS_SCHEMA",
    "hour_revision_sequence",
    "riverpulse_ops_tbl",
    "riverpulse_raw_tbl",
    "plumegraph_ops_tbl",
    "plumegraph_raw_tbl",
    "tempo_ops_tbl",
    "tempo_q",
    "tempo_raw_tbl",
    "warehouse_ops_tbl",
]
