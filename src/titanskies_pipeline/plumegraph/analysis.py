"""Revision-safe PlumeGraph partition analysis and atomic generation promotion."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Sequence

from titanskies_pipeline.config.settings_plumegraph import get_plumegraph_settings
from titanskies_pipeline.plumegraph.identity import (
    analysis_partition_identity,
    canonical_json,
    episode_revision_identity,
    sha256_identity,
)
from titanskies_pipeline.plumegraph.science import (
    CandidateInput,
    PixelRecord,
    classify_candidates,
    detect_plumes,
    estimate_emissions,
    xy_km,
)
from titanskies_pipeline.storage.duckdb.schemas.constants import (
    plumegraph_ops_tbl,
    plumegraph_raw_tbl,
)


@dataclass(frozen=True)
class AnalysisMetrics:
    partitions_succeeded: int
    partitions_failed: int
    episodes_inserted: int
    generation_ids: tuple[str, ...]


class PlumeGraphAnalysisError(RuntimeError):
    def __init__(self, metrics: AnalysisMetrics, failed_partitions: Sequence[str]):
        super().__init__(
            f"{len(failed_partitions)} PlumeGraph partition(s) failed after "
            "successful siblings were committed"
        )
        self.metrics = metrics
        self.failed_partitions = tuple(failed_partitions)


@dataclass
class _Component:
    target_ids: set[str]
    observation_time: datetime
    detection: object
    geometry: object
    wind_u_10m: float
    wind_v_10m: float
    wind_u_80m: float
    wind_v_80m: float


@dataclass(frozen=True)
class _PriorEpisode:
    episode_revision_id: str
    plume_id: str
    geometries: dict[datetime, object]


@dataclass(frozen=True)
class _TrackingEdge:
    edge_id: str
    from_component_id: str
    to_component_id: str
    from_time: datetime
    to_time: datetime
    gap_hours: float
    geometry_iou: float
    advection_residual_km: float
    concentration_ratio: float


def _db_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _geometry_modules():
    from shapely.ops import unary_union
    from shapely.wkb import dumps, loads

    return unary_union, dumps, loads


def _current_pixels(
    connection,
    region_id: str,
    partition_date: date,
    *,
    overlap_hours: int,
) -> list[PixelRecord]:
    start = datetime.combine(partition_date, datetime.min.time()) - timedelta(
        hours=overlap_hours
    )
    end = start + timedelta(days=1, hours=overlap_hours * 2)
    rows = connection.execute(
        f"""
        SELECT
            pixel_revision_id, mirror_step, xtrack, observation_time,
            latitude, longitude, pixel_area_km2, no2_vertical_column,
            no2_uncertainty, quality_flag, cloud_fraction, geometry_wkb,
            collection_version, source_snapshot_id
        FROM {plumegraph_raw_tbl("retrieval_pixel_revisions")}
        WHERE analysis_region_id = ?
          AND observation_time >= ?
          AND observation_time < ?
        QUALIFY row_number() OVER (
            PARTITION BY pixel_id
            ORDER BY source_revision_at DESC NULLS LAST,
                     collected_at DESC,
                     pixel_revision_id DESC
        ) = 1
        ORDER BY observation_time, mirror_step, xtrack, pixel_revision_id
        """,
        [region_id, start, end],
    ).fetchall()
    result: list[PixelRecord] = []
    for row in rows:
        if any(row[index] is None for index in (4, 5, 6, 7, 8, 9, 10)):
            continue
        result.append(
            PixelRecord(
                pixel_revision_id=str(row[0]),
                mirror_step=int(row[1]),
                xtrack=int(row[2]),
                observation_time=_aware(row[3]),
                latitude=float(row[4]),
                longitude=float(row[5]),
                area_km2=float(row[6]),
                no2_vertical_column=float(row[7]),
                no2_uncertainty=float(row[8]),
                quality_flag=int(row[9]),
                cloud_fraction=float(row[10]),
                geometry_wkb=bytes(row[11] or b""),
                collection_version=str(row[12]),
                source_snapshot_id=str(row[13]),
            )
        )
    return result


def _current_emissions(connection) -> dict[tuple[str, datetime], float | None]:
    rows = connection.execute(
        f"""
        SELECT facility_id, observation_start_utc, sum(nox_mass_lbs)
        FROM (
            SELECT *
            FROM {plumegraph_raw_tbl("hourly_emission_revisions")}
            QUALIFY row_number() OVER (
                PARTITION BY emission_id
                ORDER BY source_revision_at DESC NULLS LAST,
                         collected_at DESC,
                         emission_revision_id DESC
            ) = 1
        )
        GROUP BY facility_id, observation_start_utc
        """
    ).fetchall()
    return {
        (str(row[0]), _aware(row[1])): None if row[2] is None else float(row[2])
        for row in rows
    }


def _interpolate_meteorology(
    rows: Sequence[tuple[object, ...]],
    observation_time: datetime,
    max_bracket_minutes: int,
) -> dict[str, float] | None:
    before = [row for row in rows if _aware(row[0]) <= observation_time]
    after = [row for row in rows if _aware(row[0]) >= observation_time]
    if not before or not after:
        return None
    lower = max(before, key=lambda row: _aware(row[0]))
    upper = min(after, key=lambda row: _aware(row[0]))
    maximum_gap = timedelta(minutes=max_bracket_minutes)
    if (
        observation_time - _aware(lower[0]) > maximum_gap
        or _aware(upper[0]) - observation_time > maximum_gap
    ):
        return None
    values = ("u10", "v10", "u80", "v80", "pbl", "pressure", "temperature")
    if lower[0] == upper[0]:
        return {
            name: float(lower[index])
            for index, name in enumerate(values, start=1)
            if lower[index] is not None
        }
    fraction = (observation_time - _aware(lower[0])).total_seconds() / (
        _aware(upper[0]) - _aware(lower[0])
    ).total_seconds()
    result: dict[str, float] = {}
    for index, name in enumerate(values, start=1):
        if lower[index] is None or upper[index] is None:
            continue
        result[name] = float(lower[index]) + fraction * (
            float(upper[index]) - float(lower[index])
        )
    return result


def _wind_sensitivity_variants(
    rows: Sequence[tuple[object, ...]],
    track: Sequence[_Component],
) -> dict[str, float]:
    start_time = min(component.observation_time for component in track)
    end_time = max(component.observation_time for component in track)
    variants = {
        "10m": sum(
            math.hypot(component.wind_u_10m, component.wind_v_10m)
            for component in track
        )
        / len(track),
        "80m": sum(
            math.hypot(component.wind_u_80m, component.wind_v_80m)
            for component in track
        )
        / len(track),
    }
    lower_rows = [row for row in rows if _aware(row[0]) <= start_time]
    upper_rows = [row for row in rows if _aware(row[0]) >= end_time]
    for label, row in (
        ("previous", max(lower_rows, key=lambda item: _aware(item[0]))),
        ("next", min(upper_rows, key=lambda item: _aware(item[0]))),
    ):
        variants[f"10m_{label}"] = math.hypot(float(row[1]), float(row[2]))
        variants[f"80m_{label}"] = math.hypot(float(row[3]), float(row[4]))
    return variants


def _current_meteorology(
    connection,
    analysis_region_id: str,
    start: datetime,
    end: datetime,
) -> list[tuple[object, ...]]:
    return connection.execute(
        f"""
        SELECT meteorology.valid_time,
               meteorology.wind_u_10m,
               meteorology.wind_v_10m,
               meteorology.wind_u_80m,
               meteorology.wind_v_80m,
               meteorology.pbl_height_m,
               meteorology.surface_pressure_hpa,
               meteorology.temperature_2m_k
        FROM {plumegraph_raw_tbl("meteorology_observations")} AS meteorology
        INNER JOIN {plumegraph_ops_tbl("source_snapshots")} AS snapshots
            ON meteorology.source_snapshot_id = snapshots.snapshot_id
        WHERE meteorology.analysis_region_id = ?
          AND meteorology.valid_time >= ?
          AND meteorology.valid_time < ?
        QUALIFY row_number() OVER (
            PARTITION BY meteorology.analysis_region_id,
                         meteorology.valid_time,
                         meteorology.latitude,
                         meteorology.longitude
            ORDER BY snapshots.source_revision_at DESC NULLS LAST,
                     snapshots.collected_at DESC,
                     meteorology.meteorology_revision_id DESC
        ) = 1
        ORDER BY meteorology.valid_time
        """,
        [analysis_region_id, start, end],
    ).fetchall()


def _jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    return len(left & right) / len(union) if union else 1.0


def _deduplicate_components(
    components: list[_Component],
    minimum_jaccard: float,
) -> list[_Component]:
    deduplicated: list[_Component] = []
    for component in components:
        match = next(
            (
                existing
                for existing in deduplicated
                if existing.observation_time == component.observation_time
                and _jaccard(
                    set(existing.detection.plume_pixel_ids),
                    set(component.detection.plume_pixel_ids),
                )
                >= minimum_jaccard
            ),
            None,
        )
        if match is None:
            deduplicated.append(component)
        else:
            match.target_ids.update(component.target_ids)
    return deduplicated


def _component_tracks(
    components: Sequence[_Component],
    contract: Mapping[str, object],
) -> list[list[_Component]]:
    remaining = set(range(len(components)))
    tracks: list[list[_Component]] = []
    while remaining:
        root = min(remaining)
        track_indices = {root}
        frontier = [root]
        remaining.remove(root)
        while frontier:
            current_index = frontier.pop()
            current = components[current_index]
            for candidate_index in sorted(remaining):
                candidate = components[candidate_index]
                if _tracking_edge(current, candidate, contract) is not None:
                    track_indices.add(candidate_index)
                    frontier.append(candidate_index)
            remaining -= track_indices
        tracks.append(
            sorted(
                (components[index] for index in track_indices),
                key=lambda item: item.observation_time,
            )
        )
    return tracks


def _component_identity(component: _Component) -> str:
    return sha256_identity(
        component.observation_time.isoformat(),
        canonical_json(sorted(component.detection.plume_pixel_ids)),
        canonical_json(sorted(component.target_ids)),
    )


def _tracking_edge(
    left: _Component,
    right: _Component,
    contract: Mapping[str, object],
) -> _TrackingEdge | None:
    before, after = sorted((left, right), key=lambda item: item.observation_time)
    gap_hours = (
        after.observation_time - before.observation_time
    ).total_seconds() / 3600
    if gap_hours == 0 or gap_hours > float(contract["max_tracking_gap_hours"]):
        return None
    intersection = before.geometry.intersection(after.geometry).area
    union = before.geometry.union(after.geometry).area
    iou = intersection / union if union else 0.0
    actual_x_km, actual_y_km = xy_km(
        after.geometry.centroid.y,
        after.geometry.centroid.x,
        before.geometry.centroid.y,
        before.geometry.centroid.x,
    )
    residual_km = math.hypot(
        actual_x_km - before.wind_u_80m * gap_hours * 3.6,
        actual_y_km - before.wind_v_80m * gap_hours * 3.6,
    )
    enhancements = (before.detection.enhancement, after.detection.enhancement)
    continuity = (
        min(float(value) for value in enhancements)
        / max(float(value) for value in enhancements)
        if all(value is not None and float(value) > 0 for value in enhancements)
        else 0.0
    )
    if continuity < float(contract["tracking_concentration_ratio_min"]) or (
        iou <= 0 and residual_km > float(contract["tracking_advection_residual_km_max"])
    ):
        return None
    from_component_id = _component_identity(before)
    to_component_id = _component_identity(after)
    return _TrackingEdge(
        edge_id=sha256_identity(from_component_id, to_component_id),
        from_component_id=from_component_id,
        to_component_id=to_component_id,
        from_time=before.observation_time,
        to_time=after.observation_time,
        gap_hours=gap_hours,
        geometry_iou=iou,
        advection_residual_km=residual_km,
        concentration_ratio=continuity,
    )


def _track_edges(
    track: Sequence[_Component],
    contract: Mapping[str, object],
) -> list[_TrackingEdge]:
    edges = [
        edge
        for index, left in enumerate(track)
        for right in track[index + 1 :]
        if (edge := _tracking_edge(left, right, contract)) is not None
    ]
    return sorted(edges, key=lambda edge: edge.edge_id)


def _prior_episodes(
    connection,
    analysis_region_id: str,
    partition_date: date,
) -> list[_PriorEpisode]:
    current = connection.execute(
        f"""
        SELECT analysis_run_id
        FROM {plumegraph_ops_tbl("current_generations")}
        WHERE analysis_region_id = ?
          AND partition_date IN (?, ?)
        ORDER BY partition_date DESC
        LIMIT 1
        """,
        [analysis_region_id, partition_date, partition_date - timedelta(days=1)],
    ).fetchone()
    if not current:
        return []
    _, _, loads = _geometry_modules()
    rows = connection.execute(
        f"""
        SELECT episodes.episode_revision_id, episodes.plume_id,
               geometries.observation_time, geometries.geometry_wkb
        FROM {plumegraph_raw_tbl("episode_revisions")} AS episodes
        INNER JOIN {plumegraph_raw_tbl("episode_geometries")} AS geometries
            USING (episode_revision_id)
        WHERE episodes.analysis_run_id = ?
        ORDER BY episodes.episode_revision_id, geometries.observation_time
        """,
        [str(current[0])],
    ).fetchall()
    grouped: dict[tuple[str, str], dict[datetime, object]] = {}
    for revision_id, plume_id, observation_time, geometry_wkb in rows:
        grouped.setdefault((str(revision_id), str(plume_id)), {})[
            _aware(observation_time)
        ] = loads(bytes(geometry_wkb))
    return [
        _PriorEpisode(revision_id, plume_id, geometries)
        for (revision_id, plume_id), geometries in grouped.items()
    ]


def _lineage_assignments(
    prior: Sequence[_PriorEpisode],
    tracks: Sequence[Sequence[_Component]],
    contract: Mapping[str, object],
) -> tuple[dict[int, str], dict[int, list[tuple[str, str, float, float]]]]:
    matches: dict[tuple[int, int], tuple[float, float]] = {}
    for new_index, track in enumerate(tracks):
        new_geometries = {
            component.observation_time: component.geometry for component in track
        }
        new_times = set(new_geometries)
        for prior_index, previous in enumerate(prior):
            prior_times = set(previous.geometries)
            common = sorted(new_times & prior_times)
            temporal_overlap = len(common) / max(
                1,
                min(len(new_times), len(prior_times)),
            )
            if not common or temporal_overlap < float(
                contract["lineage_temporal_overlap_min"]
            ):
                continue
            ious: list[float] = []
            for timestamp in common:
                before = previous.geometries[timestamp]
                after = new_geometries[timestamp]
                union = before.union(after).area
                ious.append(before.intersection(after).area / union if union else 0.0)
            mean_iou = sum(ious) / len(ious)
            if mean_iou >= float(contract["lineage_mean_iou_min"]):
                matches[(new_index, prior_index)] = (temporal_overlap, mean_iou)
    by_new: dict[int, list[int]] = {}
    by_prior: dict[int, list[int]] = {}
    for new_index, prior_index in matches:
        by_new.setdefault(new_index, []).append(prior_index)
        by_prior.setdefault(prior_index, []).append(new_index)
    inherited: dict[int, str] = {}
    edges: dict[int, list[tuple[str, str, float, float]]] = {}
    for (new_index, prior_index), (temporal_overlap, mean_iou) in matches.items():
        if len(by_new[new_index]) == 1 and len(by_prior[prior_index]) == 1:
            relation = "supersedes"
            inherited[new_index] = prior[prior_index].plume_id
        elif len(by_new[new_index]) > 1:
            relation = "merged_from"
        else:
            relation = "split_from"
        edges.setdefault(new_index, []).append(
            (
                prior[prior_index].episode_revision_id,
                relation,
                temporal_overlap,
                mean_iou,
            )
        )
    return inherited, edges


def _facilities(connection, region_id: str) -> list[dict[str, object]]:
    facility_ids_row = connection.execute(
        f"""
        SELECT facility_ids_json
        FROM {plumegraph_raw_tbl("analysis_regions")}
        WHERE analysis_region_id = ?
        """,
        [region_id],
    ).fetchone()
    if not facility_ids_row:
        raise ValueError(f"Unknown PlumeGraph analysis region {region_id}")
    facility_ids = json.loads(str(facility_ids_row[0]))
    placeholders = ", ".join("?" for _ in facility_ids)
    rows = connection.execute(
        f"""
        SELECT facility_id, latitude, longitude, annual_nox_tons, is_cohort
        FROM {plumegraph_raw_tbl("facilities")}
        WHERE facility_id IN ({placeholders})
        ORDER BY facility_id
        """,
        facility_ids,
    ).fetchall()
    return [
        {
            "facility_id": str(row[0]),
            "latitude": float(row[1]),
            "longitude": float(row[2]),
            "annual_nox_tons": float(row[3] or 0),
            "is_cohort": bool(row[4]),
        }
        for row in rows
    ]


def _input_manifest(
    connection,
    region_id: str,
    partition_date: date,
    *,
    overlap_hours: int,
) -> tuple[str, list[str]]:
    start = datetime.combine(partition_date, datetime.min.time()) - timedelta(
        hours=overlap_hours
    )
    end = start + timedelta(days=1, hours=overlap_hours * 2)
    rows = connection.execute(
        f"""
        SELECT DISTINCT snapshots.snapshot_id, snapshots.content_sha256
        FROM {plumegraph_ops_tbl("source_snapshots")} AS snapshots
        INNER JOIN {plumegraph_ops_tbl("source_requests")} AS requests
            USING (request_id)
        WHERE requests.analysis_region_id = ?
          AND requests.window_start < ?
          AND requests.window_end > ?
        ORDER BY snapshots.snapshot_id
        """,
        [region_id, end, start],
    ).fetchall()
    snapshot_ids = [str(row[0]) for row in rows]
    return (
        sha256_identity(canonical_json([(str(row[0]), str(row[1])) for row in rows])),
        snapshot_ids,
    )


def _latest_calibration(connection) -> tuple[float | None, float | None]:
    row = connection.execute(
        f"""
        SELECT expected_calibration_error, metrics_json
        FROM {plumegraph_ops_tbl("validation_runs")}
        WHERE split_name = 'held_out'
        ORDER BY completed_at DESC, validation_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None, None
    metrics = json.loads(str(row[1]))
    temperature = metrics.get("temperature")
    return (
        None if temperature is None else float(temperature),
        None if row[0] is None else float(row[0]),
    )


def _episode_scientific_result(
    track: Sequence[_Component],
    candidates: Sequence[object],
) -> dict[str, object]:
    return {
        "components": [
            {
                "time": component.observation_time,
                "pixels": component.detection.plume_pixel_ids,
                "background": component.detection.background,
                "status": component.detection.status,
            }
            for component in track
        ],
        "candidates": [asdict(candidate) for candidate in candidates],
    }


def _track_is_analysis_ready(track: Sequence[_Component]) -> bool:
    return len(track) > 1 and all(component.detection.is_ready for component in track)


def run_analysis_partition(
    analysis_region_id: str,
    partition_date: date,
    *,
    conn=None,
) -> tuple[str, int]:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    settings = get_plumegraph_settings()
    contract = settings.contract
    with _use_conn(conn) as connection:
        overlap_hours = int(contract["partition_overlap_hours"])
        input_manifest_sha, snapshot_ids = _input_manifest(
            connection,
            analysis_region_id,
            partition_date,
            overlap_hours=overlap_hours,
        )
        temperature, calibration_ece = _latest_calibration(connection)
        calibration_manifest_sha = sha256_identity(
            canonical_json(
                {
                    "temperature": temperature,
                    "expected_calibration_error": calibration_ece,
                }
            )
        )
        analysis_input_manifest_sha = sha256_identity(
            canonical_json([input_manifest_sha, calibration_manifest_sha])
        )
        run_id = analysis_partition_identity(
            analysis_region_id,
            partition_date,
            str(contract["contract_version"]),
            str(contract["algorithm_version"]),
            [input_manifest_sha, calibration_manifest_sha],
        )
        completed = connection.execute(
            f"""
            SELECT status, episode_count
            FROM {plumegraph_ops_tbl("analysis_runs")}
            WHERE analysis_run_id = ?
            """,
            [run_id],
        ).fetchone()
        if completed and str(completed[0]) == "success":
            return run_id, int(completed[1])
        now = _db_time(datetime.now(timezone.utc))
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {plumegraph_ops_tbl("analysis_runs")}
            VALUES (?, ?, ?, ?, ?, ?, 'running', 0, ?, NULL, NULL)
            """,
            [
                run_id,
                analysis_region_id,
                partition_date,
                analysis_input_manifest_sha,
                contract["contract_version"],
                contract["algorithm_version"],
                now,
            ],
        )
        try:
            facilities = _facilities(connection, analysis_region_id)
            pixels = _current_pixels(
                connection,
                analysis_region_id,
                partition_date,
                overlap_hours=overlap_hours,
            )
            emissions = _current_emissions(connection)
            met_rows = _current_meteorology(
                connection,
                analysis_region_id,
                datetime.combine(partition_date, datetime.min.time())
                - timedelta(hours=overlap_hours),
                datetime.combine(partition_date, datetime.min.time())
                + timedelta(days=1, hours=overlap_hours),
            )
            unary_union, _, loads = _geometry_modules()
            by_time: dict[datetime, list[PixelRecord]] = {}
            for pixel in pixels:
                by_time.setdefault(pixel.observation_time, []).append(pixel)
            components: list[_Component] = []
            for observation_time, scene_pixels in sorted(by_time.items()):
                meteorology = _interpolate_meteorology(
                    met_rows,
                    observation_time,
                    int(contract["meteorology_max_bracket_minutes"]),
                )
                for facility in facilities:
                    if not facility["is_cohort"]:
                        continue
                    if meteorology is None or not all(
                        name in meteorology for name in ("u10", "v10", "u80", "v80")
                    ):
                        detections = detect_plumes(
                            scene_pixels,
                            target_latitude=float(facility["latitude"]),
                            target_longitude=float(facility["longitude"]),
                            wind_u_ms=float("nan"),
                            wind_v_ms=float("nan"),
                            contract=contract,
                        )
                    else:
                        detections = detect_plumes(
                            scene_pixels,
                            target_latitude=float(facility["latitude"]),
                            target_longitude=float(facility["longitude"]),
                            wind_u_ms=meteorology["u80"],
                            wind_v_ms=meteorology["v80"],
                            contract=contract,
                        )
                    for detection in detections:
                        if not detection.plume_pixel_ids:
                            continue
                        pixel_map = {
                            pixel.pixel_revision_id: pixel for pixel in scene_pixels
                        }
                        geometry = unary_union(
                            [
                                loads(pixel_map[pixel_id].geometry_wkb)
                                for pixel_id in detection.plume_pixel_ids
                            ]
                        )
                        components.append(
                            _Component(
                                target_ids={str(facility["facility_id"])},
                                observation_time=observation_time,
                                detection=detection,
                                geometry=geometry,
                                wind_u_10m=(
                                    float(meteorology["u10"])
                                    if meteorology is not None
                                    else float("nan")
                                ),
                                wind_v_10m=(
                                    float(meteorology["v10"])
                                    if meteorology is not None
                                    else float("nan")
                                ),
                                wind_u_80m=(
                                    float(meteorology["u80"])
                                    if meteorology is not None
                                    else float("nan")
                                ),
                                wind_v_80m=(
                                    float(meteorology["v80"])
                                    if meteorology is not None
                                    else float("nan")
                                ),
                            )
                        )
            tracks = _component_tracks(
                _deduplicate_components(
                    components,
                    float(contract["dedup_jaccard_min"]),
                ),
                contract,
            )
            inherited_plume_ids, lineage_edges = _lineage_assignments(
                _prior_episodes(connection, analysis_region_id, partition_date),
                tracks,
                contract,
            )
            connection.begin()
            episode_count = 0
            try:
                pixel_map = {pixel.pixel_revision_id: pixel for pixel in pixels}
                for track_index, track in enumerate(tracks):
                    episode_count += 1
                    geometry = unary_union([component.geometry for component in track])
                    centroid = geometry.centroid
                    start_time = min(item.observation_time for item in track)
                    end_time = max(item.observation_time for item in track)
                    average_u = sum(item.wind_u_80m for item in track) / len(track)
                    average_v = sum(item.wind_v_80m for item in track) / len(track)
                    candidate_inputs: list[CandidateInput] = []
                    for facility in facilities:
                        dx = (centroid.x - float(facility["longitude"])) * 111
                        dy = (centroid.y - float(facility["latitude"])) * 111
                        distance = math.hypot(dx, dy)
                        vector_length = max(distance, 1e-9)
                        wind_length = max(math.hypot(average_u, average_v), 1e-9)
                        alignment = max(
                            0.0,
                            min(
                                1.0,
                                (dx * average_u + dy * average_v)
                                / (vector_length * wind_length),
                            ),
                        )
                        hour = start_time.replace(minute=0, second=0, microsecond=0)
                        candidate_inputs.append(
                            CandidateInput(
                                facility_id=str(facility["facility_id"]),
                                trajectory_alignment=alignment,
                                concurrent_nox_lbs=emissions.get(
                                    (str(facility["facility_id"]), hour)
                                ),
                                distance_km=distance,
                                annual_nox_tons=float(facility["annual_nox_tons"]),
                                is_cohort=bool(facility["is_cohort"]),
                            )
                        )
                    candidates = classify_candidates(
                        candidate_inputs,
                        contract=contract,
                        temperature=temperature,
                        calibration_ece=calibration_ece,
                    )
                    episode_is_ready = _track_is_analysis_ready(track)
                    if not episode_is_ready:
                        candidates = [
                            replace(
                                candidate,
                                probability=None,
                                classification="insufficient_evidence",
                                is_probability_ready=False,
                            )
                            for candidate in candidates
                        ]
                    scientific_result = _episode_scientific_result(track, candidates)
                    all_pixel_ids = sorted(
                        {
                            pixel_id
                            for component in track
                            for pixel_id in component.detection.plume_pixel_ids
                        }
                    )
                    plume_id = inherited_plume_ids.get(
                        track_index,
                        sha256_identity(
                            analysis_region_id,
                            canonical_json(all_pixel_ids),
                        ),
                    )
                    tracking_edges = _track_edges(track, contract)
                    revision_id = episode_revision_identity(
                        run_id,
                        [candidate.facility_id for candidate in candidates],
                        [edge.edge_id for edge in tracking_edges],
                        all_pixel_ids,
                        scientific_result,
                    )
                    classification = (
                        candidates[0].classification
                        if candidates
                        else "insufficient_evidence"
                    )
                    backgrounds = [
                        float(component.detection.background)
                        for component in track
                        if component.detection.background is not None
                    ]
                    enhancements = [
                        float(component.detection.enhancement)
                        for component in track
                        if component.detection.enhancement is not None
                    ]
                    connection.execute(
                        f"""
                        INSERT OR IGNORE INTO
                        {plumegraph_raw_tbl("episode_revisions")}
                        VALUES (?, ?, ?, 'NO2', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?)
                        """,
                        [
                            revision_id,
                            plume_id,
                            run_id,
                            _db_time(start_time),
                            _db_time(end_time),
                            len(track),
                            len(all_pixel_ids),
                            len(
                                {
                                    pixel_id
                                    for component in track
                                    for pixel_id in component.detection.background_pixel_ids
                                }
                            ),
                            sum(enhancements) if enhancements else None,
                            (
                                sum(backgrounds) / len(backgrounds)
                                if backgrounds
                                else None
                            ),
                            (
                                (end_time - start_time).total_seconds() / 3600
                                if end_time > start_time
                                else 0.0
                            ),
                            classification,
                            (
                                "analysis_ready"
                                if episode_is_ready
                                else "insufficient_evidence"
                            ),
                            episode_is_ready,
                            contract["contract_version"],
                            contract["algorithm_version"],
                            now,
                        ],
                    )
                    for edge in tracking_edges:
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("episode_tracking_edges")}
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                revision_id,
                                edge.edge_id,
                                edge.from_component_id,
                                edge.to_component_id,
                                _db_time(edge.from_time),
                                _db_time(edge.to_time),
                                edge.gap_hours,
                                edge.geometry_iou,
                                edge.advection_residual_km,
                                edge.concentration_ratio,
                            ],
                        )
                    for (
                        prior_revision_id,
                        relation_type,
                        temporal_overlap,
                        mean_iou,
                    ) in lineage_edges.get(track_index, []):
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("episode_lineage")}
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            [
                                prior_revision_id,
                                revision_id,
                                relation_type,
                                temporal_overlap,
                                mean_iou,
                            ],
                        )
                    _, dumps, _ = _geometry_modules()
                    for component in track:
                        component_centroid = component.geometry.centroid
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("episode_geometries")}
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                revision_id,
                                _db_time(component.observation_time),
                                dumps(
                                    component.geometry,
                                    hex=False,
                                    big_endian=False,
                                ),
                                component_centroid.y,
                                component_centroid.x,
                                component.wind_u_80m,
                                component.wind_v_80m,
                            ],
                        )
                    for target_id in sorted(
                        {
                            target
                            for component in track
                            for target in component.target_ids
                        }
                    ):
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("episode_target_links")}
                            VALUES (?, ?)
                            """,
                            [revision_id, target_id],
                        )
                    for candidate in candidates:
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("candidate_source_revisions")}
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                revision_id,
                                candidate.facility_id,
                                candidate.rank,
                                candidate.trajectory_score,
                                candidate.emissions_score,
                                candidate.distance_score,
                                candidate.annual_prior_score,
                                candidate.attribution_score,
                                candidate.probability,
                                candidate.classification,
                                candidate.distance_km,
                                candidate.is_cohort,
                                candidate.is_probability_ready,
                            ],
                        )
                    background = (
                        sum(backgrounds) / len(backgrounds) if backgrounds else 0.0
                    )
                    plume_pixels = [pixel_map[pixel_id] for pixel_id in all_pixel_ids]
                    estimates = estimate_emissions(
                        plume_pixels,
                        background_molecules_cm2=background,
                        wind_speeds_ms=_wind_sensitivity_variants(met_rows, track),
                        plume_length_km=max(
                            1.0,
                            math.hypot(
                                geometry.bounds[2] - geometry.bounds[0],
                                geometry.bounds[3] - geometry.bounds[1],
                            )
                            * 111,
                        ),
                        contract=contract,
                    )
                    for estimate in estimates:
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("emission_estimate_revisions")}
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                revision_id,
                                estimate.variant_id,
                                estimate.wind_variant,
                                estimate.wind_speed_ms,
                                estimate.lifetime_hours,
                                estimate.no2_nox_ratio,
                                estimate.no2_flux_kg_h,
                                estimate.nox_flux_kg_h,
                                estimate.retrieval_uncertainty_kg_h,
                                estimate.background_uncertainty_kg_h,
                                estimate.wind_uncertainty_kg_h,
                                estimate.geometry_uncertainty_kg_h,
                                estimate.is_central,
                            ],
                        )
                    roles: dict[str, tuple[str, str | None, float | None]] = {}
                    for component in track:
                        for pixel_id in component.detection.background_pixel_ids:
                            roles.setdefault(pixel_id, ("background", None, None))
                        for pixel_id in component.detection.rejected_pixel_ids:
                            roles.setdefault(
                                pixel_id,
                                ("rejected", "science_readiness", None),
                            )
                        for pixel_id in component.detection.plume_pixel_ids:
                            pixel = pixel_map[pixel_id]
                            roles[pixel_id] = (
                                "plume",
                                None,
                                pixel.no2_vertical_column
                                - float(component.detection.background or 0),
                            )
                    for pixel_id, (role, reason, enhancement) in sorted(roles.items()):
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("episode_pixel_links")}
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            [revision_id, pixel_id, role, reason, enhancement],
                        )
                    for snapshot_id in sorted(snapshot_ids):
                        source_type = str(
                            connection.execute(
                                f"""
                            SELECT connector
                            FROM {plumegraph_ops_tbl("source_snapshots")}
                            WHERE snapshot_id = ?
                            """,
                                [snapshot_id],
                            ).fetchone()[0]
                        )
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO
                            {plumegraph_raw_tbl("provenance_links")}
                            VALUES (?, ?, ?, ?)
                            """,
                            [revision_id, source_type, snapshot_id, run_id],
                        )
                connection.execute(
                    f"""
                    UPDATE {plumegraph_ops_tbl("analysis_runs")}
                    SET status = 'success', episode_count = ?,
                        completed_at = ?, error_message = NULL
                    WHERE analysis_run_id = ?
                    """,
                    [episode_count, now, run_id],
                )
                connection.execute(
                    f"""
                    INSERT OR REPLACE INTO
                    {plumegraph_ops_tbl("current_generations")}
                    VALUES (?, ?, ?, ?)
                    """,
                    [analysis_region_id, partition_date, run_id, now],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        except Exception as exc:
            connection.execute(
                f"""
                UPDATE {plumegraph_ops_tbl("analysis_runs")}
                SET status = 'failed', completed_at = ?, error_message = ?
                WHERE analysis_run_id = ?
                """,
                [
                    _db_time(datetime.now(timezone.utc)),
                    str(exc)[:2000],
                    run_id,
                ],
            )
            raise
    return run_id, episode_count


def run_pending_analysis(
    *,
    partition_dates: Sequence[date] | None = None,
    conn=None,
) -> AnalysisMetrics:
    from titanskies_pipeline.storage.duckdb.connection import _use_conn

    with _use_conn(conn) as connection:
        regions = [
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT analysis_region_id
                FROM {plumegraph_raw_tbl("analysis_regions")}
                ORDER BY analysis_region_id
                """
            ).fetchall()
        ]
        dates = sorted(set(partition_dates or ()))
        if not dates:
            dates = [
                row[0]
                for row in connection.execute(
                    f"""
                    SELECT DISTINCT CAST(observation_time AS DATE)
                    FROM {plumegraph_raw_tbl("retrieval_pixel_revisions")}
                    ORDER BY 1
                    """
                ).fetchall()
            ]
        succeeded = 0
        episodes = 0
        run_ids: list[str] = []
        failed: list[str] = []
        for region_id in regions:
            for partition_date in dates:
                label = f"{region_id}:{partition_date.isoformat()}"
                try:
                    run_id, count = run_analysis_partition(
                        region_id,
                        partition_date,
                        conn=connection,
                    )
                except Exception:
                    failed.append(label)
                    continue
                succeeded += 1
                episodes += count
                run_ids.append(run_id)
    metrics = AnalysisMetrics(
        partitions_succeeded=succeeded,
        partitions_failed=len(failed),
        episodes_inserted=episodes,
        generation_ids=tuple(run_ids),
    )
    if failed:
        raise PlumeGraphAnalysisError(metrics, failed)
    return metrics


__all__ = [
    "AnalysisMetrics",
    "PlumeGraphAnalysisError",
    "run_analysis_partition",
    "run_pending_analysis",
]
