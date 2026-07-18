"""Shared source/scope naming helpers."""

from __future__ import annotations

from dagster import AssetKey

SOURCE_TEMPO = "tempo"
SCOPE_NO2 = "no2"
SCOPE_NO2_STD = "no2_std"


def flat_name(source: str, scope: str, *parts: str) -> str:
    return "_".join((source, scope, *parts))


def schema_name(source: str, scope: str, layer: str) -> str:
    return flat_name(source, scope, layer)


def asset_key(source: str, scope: str, layer: str, *parts: str) -> AssetKey:
    return AssetKey([source, scope, layer, *parts])


TEMPO_NO2 = flat_name(SOURCE_TEMPO, SCOPE_NO2)
TEMPO_NO2_STD = flat_name(SOURCE_TEMPO, SCOPE_NO2_STD)

__all__ = [
    "SCOPE_NO2",
    "SCOPE_NO2_STD",
    "SOURCE_TEMPO",
    "TEMPO_NO2",
    "TEMPO_NO2_STD",
    "asset_key",
    "flat_name",
    "schema_name",
]
