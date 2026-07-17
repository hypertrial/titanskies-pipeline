"""Ordered settings reload for tests."""

from __future__ import annotations

import importlib
from types import ModuleType

_SETTINGS_CHAIN: tuple[str, ...] = (
    "titanskies_pipeline.config.settings_warehouse",
    "titanskies_pipeline.config.settings_tempo",
    "titanskies_pipeline.config.settings",
)


def reload_all_settings_modules() -> ModuleType:
    """Reload settings submodules then the barrel; return the refreshed settings module."""
    out: ModuleType | None = None
    for name in _SETTINGS_CHAIN:
        out = importlib.reload(importlib.import_module(name))
    assert out is not None
    return out


reload_settings = reload_all_settings_modules

__all__ = ["reload_all_settings_modules", "reload_settings"]
