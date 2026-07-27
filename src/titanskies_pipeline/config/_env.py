"""Pure environment parsing helpers."""

from __future__ import annotations

import os
from datetime import date


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a float") from exc


def _optional_env_str(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return raw.strip()


def _env_date(name: str, default: str) -> date:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return date.fromisoformat(default)
    try:
        return date.fromisoformat(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name} must be an ISO date (YYYY-MM-DD)"
        ) from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


__all__ = [
    "_env_bool",
    "_env_date",
    "_env_float",
    "_env_int",
    "_optional_env_str",
]
