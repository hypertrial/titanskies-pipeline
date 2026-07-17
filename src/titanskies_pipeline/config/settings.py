"""Barrel re-export of all settings."""

from titanskies_pipeline.config.settings_tempo import *  # noqa: F403
from titanskies_pipeline.config.settings_warehouse import *  # noqa: F403

__all__ = [
    *[
        name
        for name in dir()
        if not name.startswith("_") and name not in {"annotations"}
    ],
]
