"""Explicit Dagster assets for the PlumeGraph events lane."""

from dataclasses import asdict
from datetime import date
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from titanskies_pipeline.config.settings_plumegraph import (
    get_plumegraph_settings,
)
from titanskies_pipeline.orchestration.config import (
    PlumeGraphAnalysisConfig,
    PlumeGraphDiscoveryConfig,
    PlumeGraphFacilityRegistryConfig,
    PlumeGraphIngestConfig,
    PlumeGraphReleaseConfig,
    PlumeGraphValidationConfig,
)
from titanskies_pipeline.orchestration.timestamps import parse_iso_utc
from titanskies_pipeline.plumegraph.analysis import run_pending_analysis
from titanskies_pipeline.plumegraph.connectors import sync_source_connector
from titanskies_pipeline.plumegraph.release import build_release
from titanskies_pipeline.plumegraph.sources import (
    persist_cohort,
    plan_source_requests,
)
from titanskies_pipeline.plumegraph.validation import (
    load_benchmark,
    run_validation,
)

PLUMEGRAPH_EVENTS_OPS_FACILITY_REGISTRY = AssetKey(
    ["plumegraph", "events", "ops", "facility_registry"]
)
PLUMEGRAPH_EVENTS_RAW_SOURCE_INVENTORY = AssetKey(
    ["plumegraph", "events", "raw", "source_inventory"]
)
PLUMEGRAPH_EVENTS_RAW_TEMPO_SNAPSHOTS = AssetKey(
    ["plumegraph", "events", "raw", "tempo_snapshots"]
)
PLUMEGRAPH_EVENTS_RAW_HRRR_SNAPSHOTS = AssetKey(
    ["plumegraph", "events", "raw", "hrrr_snapshots"]
)
PLUMEGRAPH_EVENTS_RAW_CAMD_EMISSIONS = AssetKey(
    ["plumegraph", "events", "raw", "camd_emissions"]
)
PLUMEGRAPH_EVENTS_ANALYSIS_RESULTS = AssetKey(
    ["plumegraph", "events", "intermediate", "analysis_results"]
)
PLUMEGRAPH_EVENTS_VALIDATION = AssetKey(
    ["plumegraph", "events", "observability", "validation"]
)
PLUMEGRAPH_EVENTS_RELEASE = AssetKey(
    ["plumegraph", "events", "releases", "evidence_ledger"]
)


@asset(
    key=PLUMEGRAPH_EVENTS_OPS_FACILITY_REGISTRY,
    group_name="ingestion",
)
def plumegraph_events_ops_facility_registry(
    context: AssetExecutionContext,
    config: PlumeGraphFacilityRegistryConfig,
) -> MaterializeResult:
    settings = get_plumegraph_settings()
    path = (
        Path(config.manifest_path).resolve()
        if config.manifest_path
        else settings.cohort_manifest_path
    )
    metrics = persist_cohort(path, require_approved=not config.allow_synthetic)
    context.log.info("Registered PlumeGraph facility cohort: %s", metrics)
    return MaterializeResult(metadata=metrics)


@asset(
    key=PLUMEGRAPH_EVENTS_RAW_SOURCE_INVENTORY,
    deps=[PLUMEGRAPH_EVENTS_OPS_FACILITY_REGISTRY],
    group_name="ingestion",
)
def plumegraph_events_raw_source_inventory(
    context: AssetExecutionContext,
    config: PlumeGraphDiscoveryConfig,
) -> MaterializeResult:
    start = parse_iso_utc(config.window_start_utc) if config.window_start_utc else None
    end = parse_iso_utc(config.window_end_utc) if config.window_end_utc else None
    metrics = plan_source_requests(
        window_start=start,
        window_end=end,
        backfill=config.backfill,
    )
    metadata = asdict(metrics)
    context.log.info("Planned PlumeGraph source requests: %s", metadata)
    return MaterializeResult(metadata=metadata)


def _source_asset(
    *,
    connector: str,
    key: AssetKey,
):
    @asset(
        key=key,
        deps=[PLUMEGRAPH_EVENTS_RAW_SOURCE_INVENTORY],
        group_name="ingestion",
    )
    def _asset(
        context: AssetExecutionContext,
        config: PlumeGraphIngestConfig,
    ) -> MaterializeResult:
        metrics = sync_source_connector(
            connector,
            max_requests=config.max_requests,
            raw_data_dir=(
                Path(config.raw_data_dir).resolve() if config.raw_data_dir else None
            ),
        )
        metadata = asdict(metrics)
        context.log.info("Ingested PlumeGraph %s source: %s", connector, metadata)
        return MaterializeResult(metadata=metadata)

    return _asset


plumegraph_events_raw_tempo_snapshots = _source_asset(
    connector="harmony",
    key=PLUMEGRAPH_EVENTS_RAW_TEMPO_SNAPSHOTS,
)
plumegraph_events_raw_hrrr_snapshots = _source_asset(
    connector="hrrr",
    key=PLUMEGRAPH_EVENTS_RAW_HRRR_SNAPSHOTS,
)
plumegraph_events_raw_camd_emissions = _source_asset(
    connector="camd",
    key=PLUMEGRAPH_EVENTS_RAW_CAMD_EMISSIONS,
)


@asset(
    key=PLUMEGRAPH_EVENTS_ANALYSIS_RESULTS,
    deps=[
        PLUMEGRAPH_EVENTS_RAW_TEMPO_SNAPSHOTS,
        PLUMEGRAPH_EVENTS_RAW_HRRR_SNAPSHOTS,
        PLUMEGRAPH_EVENTS_RAW_CAMD_EMISSIONS,
    ],
    group_name="analytics",
)
def plumegraph_events_analysis_results(
    context: AssetExecutionContext,
    config: PlumeGraphAnalysisConfig,
) -> MaterializeResult:
    partition_dates = (
        [date.fromisoformat(value) for value in config.partition_dates]
        if config.partition_dates
        else None
    )
    metrics = run_pending_analysis(partition_dates=partition_dates)
    metadata = asdict(metrics)
    metadata["generation_ids"] = list(metrics.generation_ids)
    context.log.info("Analyzed PlumeGraph region-day partitions: %s", metadata)
    return MaterializeResult(metadata=metadata)


@asset(
    key=PLUMEGRAPH_EVENTS_VALIDATION,
    deps=[
        PLUMEGRAPH_EVENTS_ANALYSIS_RESULTS,
        AssetKey(["plumegraph", "events", "marts", "episodes"]),
        AssetKey(["plumegraph", "events", "marts", "candidate_sources"]),
    ],
    group_name="analytics",
)
def plumegraph_events_validation(
    context: AssetExecutionContext,
    config: PlumeGraphValidationConfig,
) -> MaterializeResult:
    if config.benchmark_path:
        load_benchmark(
            Path(config.benchmark_path).resolve(),
            allow_incomplete=config.allow_incomplete,
        )
    metrics = run_validation(
        config.benchmark_version,
        allow_incomplete=config.allow_incomplete,
    )
    metadata = asdict(metrics)
    context.log.info("Validated PlumeGraph evidence ledger: %s", metadata)
    return MaterializeResult(metadata=metadata)


@asset(
    key=PLUMEGRAPH_EVENTS_RELEASE,
    deps=[
        PLUMEGRAPH_EVENTS_VALIDATION,
        AssetKey(["plumegraph", "events", "marts", "provenance"]),
        AssetKey(["plumegraph", "events", "marts", "evidence_pixels"]),
    ],
    group_name="analytics",
)
def plumegraph_events_release(
    context: AssetExecutionContext,
    config: PlumeGraphReleaseConfig,
) -> MaterializeResult:
    metrics = build_release(
        release_version=config.release_version,
        output_dir=Path(config.output_dir).resolve() if config.output_dir else None,
        validation_run_id=config.validation_run_id,
        require_passed_validation=config.require_passed_validation,
    )
    metadata = {
        **asdict(metrics),
        "release_path": str(metrics.release_path),
    }
    context.log.info("Built immutable PlumeGraph release: %s", metadata)
    return MaterializeResult(metadata=metadata)


__all__ = [
    "PLUMEGRAPH_EVENTS_ANALYSIS_RESULTS",
    "PLUMEGRAPH_EVENTS_OPS_FACILITY_REGISTRY",
    "PLUMEGRAPH_EVENTS_RAW_CAMD_EMISSIONS",
    "PLUMEGRAPH_EVENTS_RAW_HRRR_SNAPSHOTS",
    "PLUMEGRAPH_EVENTS_RAW_SOURCE_INVENTORY",
    "PLUMEGRAPH_EVENTS_RAW_TEMPO_SNAPSHOTS",
    "PLUMEGRAPH_EVENTS_RELEASE",
    "PLUMEGRAPH_EVENTS_VALIDATION",
    "plumegraph_events_analysis_results",
    "plumegraph_events_ops_facility_registry",
    "plumegraph_events_raw_camd_emissions",
    "plumegraph_events_raw_hrrr_snapshots",
    "plumegraph_events_raw_source_inventory",
    "plumegraph_events_raw_tempo_snapshots",
    "plumegraph_events_release",
    "plumegraph_events_validation",
]
