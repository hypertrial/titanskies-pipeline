"""UTC timestamp parsing for Dagster discovery-window config."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 timestamp to a naive UTC datetime.

    Accepts a trailing ``Z`` and aware offsets; naive values are treated as UTC.
    """
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


__all__ = ["parse_iso_utc"]
