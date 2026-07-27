"""Shared filesystem paths for TEMPO granule raw NetCDF files."""

from __future__ import annotations

from pathlib import Path

from titanskies_pipeline.config.settings_tempo import get_tempo_scope_settings
from titanskies_pipeline.naming import SCOPE_NO2


def granule_raw_path(granule_id: str, *, scope: str = SCOPE_NO2) -> Path:
    """Return the on-disk NetCDF path for a granule under the scope raw directory."""
    name = Path(granule_id).name
    if not name.endswith(".nc"):
        name = f"{granule_id.replace('/', '_')}.nc"
    return get_tempo_scope_settings(scope).raw_data_dir / name


__all__ = ["granule_raw_path"]
