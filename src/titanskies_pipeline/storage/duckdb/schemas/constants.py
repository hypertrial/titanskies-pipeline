"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

from titanskies_pipeline.naming import (
    SCOPE_NO2,
    SCOPE_NO2_STD,
    SOURCE_TEMPO,
    schema_name,
)

TEMPO_NO2_RAW_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "raw")
TEMPO_NO2_OPS_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "ops")
TEMPO_NO2_STD_RAW_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2_STD, "raw")
TEMPO_NO2_STD_OPS_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2_STD, "ops")

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
    "TEMPO_NO2_OPS_SCHEMA",
    "TEMPO_NO2_RAW_SCHEMA",
    "TEMPO_NO2_STD_OPS_SCHEMA",
    "TEMPO_NO2_STD_RAW_SCHEMA",
    "hour_revision_sequence",
    "tempo_ops_tbl",
    "tempo_q",
    "tempo_raw_tbl",
]
