"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

from titanskies_pipeline.naming import SCOPE_NO2, SOURCE_TEMPO, schema_name

TEMPO_NO2_RAW_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "raw")
TEMPO_NO2_OPS_SCHEMA = schema_name(SOURCE_TEMPO, SCOPE_NO2, "ops")


def tempo_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def tempo_raw_tbl(name: str) -> str:
    return tempo_q(TEMPO_NO2_RAW_SCHEMA, name)


def tempo_ops_tbl(name: str) -> str:
    return tempo_q(TEMPO_NO2_OPS_SCHEMA, name)


__all__ = [
    "TEMPO_NO2_OPS_SCHEMA",
    "TEMPO_NO2_RAW_SCHEMA",
    "tempo_ops_tbl",
    "tempo_q",
    "tempo_raw_tbl",
]
