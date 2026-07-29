"""Shared source/scope naming helpers."""

from __future__ import annotations

from dagster import AssetKey

SOURCE_TEMPO = "tempo"
SOURCE_RIVERPULSE = "riverpulse"
SOURCE_PLUMEGRAPH = "plumegraph"
SOURCE_SUN2025 = "sun2025"
SOURCE_ANDREADIS2025 = "andreadis2025"
SCOPE_NO2 = "no2"
SCOPE_NO2_STD = "no2_std"
SCOPE_EVENTS = "events"
SCOPE_REPRO = "repro"


def flat_name(source: str, scope: str, *parts: str) -> str:
    return "_".join((source, scope, *parts))


def schema_name(source: str, scope: str, layer: str) -> str:
    return flat_name(source, scope, layer)


def asset_key(source: str, scope: str, layer: str, *parts: str) -> AssetKey:
    return AssetKey([source, scope, layer, *parts])


TEMPO_NO2 = flat_name(SOURCE_TEMPO, SCOPE_NO2)
TEMPO_NO2_STD = flat_name(SOURCE_TEMPO, SCOPE_NO2_STD)
RIVERPULSE_EVENTS = flat_name(SOURCE_RIVERPULSE, SCOPE_EVENTS)
PLUMEGRAPH_EVENTS = flat_name(SOURCE_PLUMEGRAPH, SCOPE_EVENTS)
SUN2025_REPRO = flat_name(SOURCE_SUN2025, SCOPE_REPRO)
ANDREADIS2025_REPRO = flat_name(SOURCE_ANDREADIS2025, SCOPE_REPRO)

__all__ = [
    "ANDREADIS2025_REPRO",
    "RIVERPULSE_EVENTS",
    "PLUMEGRAPH_EVENTS",
    "SCOPE_EVENTS",
    "SCOPE_NO2",
    "SCOPE_NO2_STD",
    "SCOPE_REPRO",
    "SOURCE_ANDREADIS2025",
    "SOURCE_RIVERPULSE",
    "SOURCE_PLUMEGRAPH",
    "SOURCE_SUN2025",
    "SOURCE_TEMPO",
    "SUN2025_REPRO",
    "TEMPO_NO2",
    "TEMPO_NO2_STD",
    "asset_key",
    "flat_name",
    "schema_name",
]
