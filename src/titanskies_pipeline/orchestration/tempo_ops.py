"""Stable facade for orchestration tests."""

from titanskies_pipeline.ingestion.tempo.sync import (
    process_pending_granules,
    require_registered_geography,
    sync_granule_discovery,
    sync_region_registry,
)

__all__ = [
    "process_pending_granules",
    "require_registered_geography",
    "sync_granule_discovery",
    "sync_region_registry",
]
