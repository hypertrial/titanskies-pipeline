"""Deterministic PlumeGraph baseline science.

The functions in this module are intentionally dependency-light and consume a
validated contract mapping. Geometry construction and persistence live outside
the numerical kernel.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Sequence

AVOGADRO = 6.02214076e23
NO2_KG_PER_MOL = 0.0460055
EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class PixelRecord:
    pixel_revision_id: str
    mirror_step: int
    xtrack: int
    observation_time: datetime
    latitude: float
    longitude: float
    area_km2: float
    no2_vertical_column: float
    no2_uncertainty: float
    quality_flag: int
    cloud_fraction: float
    geometry_wkb: bytes
    collection_version: str
    source_snapshot_id: str


@dataclass(frozen=True)
class DetectionResult:
    plume_pixel_ids: tuple[str, ...]
    background_pixel_ids: tuple[str, ...]
    rejected_pixel_ids: tuple[str, ...]
    background: float | None
    background_mad: float | None
    enhancement: float | None
    is_ready: bool
    status: str


@dataclass(frozen=True)
class CandidateInput:
    facility_id: str
    trajectory_alignment: float
    concurrent_nox_lbs: float | None
    distance_km: float
    annual_nox_tons: float
    is_cohort: bool


@dataclass(frozen=True)
class CandidateResult:
    facility_id: str
    rank: int
    trajectory_score: float
    emissions_score: float | None
    distance_score: float
    annual_prior_score: float
    attribution_score: float
    probability: float | None
    classification: str
    distance_km: float
    is_cohort: bool
    is_probability_ready: bool


@dataclass(frozen=True)
class EmissionEstimate:
    variant_id: str
    wind_variant: str
    wind_speed_ms: float
    lifetime_hours: float
    no2_nox_ratio: float
    no2_flux_kg_h: float
    nox_flux_kg_h: float
    retrieval_uncertainty_kg_h: float
    background_uncertainty_kg_h: float
    wind_uncertainty_kg_h: float
    geometry_uncertainty_kg_h: float
    is_central: bool


def is_pixel_ready(pixel: PixelRecord, contract: Mapping[str, object]) -> bool:
    return (
        pixel.quality_flag == int(contract["quality_flag_good"])
        and math.isfinite(pixel.cloud_fraction)
        and pixel.cloud_fraction < float(contract["max_cloud_fraction"])
        and math.isfinite(pixel.no2_vertical_column)
        and math.isfinite(pixel.no2_uncertainty)
        and pixel.no2_uncertainty > 0
        and math.isfinite(pixel.area_km2)
        and pixel.area_km2 > 0
        and bool(pixel.geometry_wkb)
        and pixel.collection_version == str(contract["collection_version"])
        and bool(pixel.source_snapshot_id)
    )


def xy_km(
    latitude: float,
    longitude: float,
    origin_latitude: float,
    origin_longitude: float,
) -> tuple[float, float]:
    latitude_radians = math.radians((latitude + origin_latitude) / 2)
    x = (
        math.radians(longitude - origin_longitude)
        * math.cos(latitude_radians)
        * EARTH_RADIUS_KM
    )
    y = math.radians(latitude - origin_latitude) * EARTH_RADIUS_KM
    return x, y


def _median_mad(values: Sequence[float]) -> tuple[float, float]:
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return median, mad


def _connected_components(
    pixels: Sequence[PixelRecord],
    eligible_ids: set[str],
) -> list[set[str]]:
    coordinates = {
        (pixel.mirror_step, pixel.xtrack): pixel.pixel_revision_id
        for pixel in pixels
        if pixel.pixel_revision_id in eligible_ids
    }
    unvisited = set(eligible_ids)
    components: list[set[str]] = []
    while unvisited:
        root = min(unvisited)
        component = {root}
        frontier = [root]
        unvisited.remove(root)
        by_id = {pixel.pixel_revision_id: pixel for pixel in pixels}
        while frontier:
            current = by_id[frontier.pop()]
            for coordinate in (
                (current.mirror_step - 1, current.xtrack),
                (current.mirror_step + 1, current.xtrack),
                (current.mirror_step, current.xtrack - 1),
                (current.mirror_step, current.xtrack + 1),
            ):
                neighbor = coordinates.get(coordinate)
                if neighbor in unvisited:
                    unvisited.remove(neighbor)
                    component.add(neighbor)
                    frontier.append(neighbor)
        components.append(component)
    return components


def detect_plumes(
    pixels: Sequence[PixelRecord],
    *,
    target_latitude: float,
    target_longitude: float,
    wind_u_ms: float,
    wind_v_ms: float,
    contract: Mapping[str, object],
) -> list[DetectionResult]:
    ready = [pixel for pixel in pixels if is_pixel_ready(pixel, contract)]
    rejected = tuple(
        sorted(pixel.pixel_revision_id for pixel in pixels if pixel not in ready)
    )
    wind_speed = math.hypot(wind_u_ms, wind_v_ms)
    if not ready or not math.isfinite(wind_speed) or wind_speed == 0:
        return [
            DetectionResult(
                (),
                (),
                rejected,
                None,
                None,
                None,
                False,
                "meteorology_incomplete" if ready else "no_eligible_pixels",
            )
        ]
    wind_x = wind_u_ms / wind_speed
    wind_y = wind_v_ms / wind_speed
    positioned: list[tuple[PixelRecord, float, float]] = []
    for pixel in ready:
        x, y = xy_km(
            pixel.latitude,
            pixel.longitude,
            target_latitude,
            target_longitude,
        )
        positioned.append((pixel, math.hypot(x, y), x * wind_x + y * wind_y))
    background_pixels = [
        pixel
        for pixel, distance, along_wind in positioned
        if float(contract["background_inner_km"])
        <= distance
        <= float(contract["background_outer_km"])
        and along_wind < 0
    ]
    background_ids = tuple(
        sorted(pixel.pixel_revision_id for pixel in background_pixels)
    )
    if len(background_pixels) < int(contract["min_background_pixels"]):
        return [
            DetectionResult(
                (),
                background_ids,
                rejected,
                None,
                None,
                None,
                False,
                "insufficient_background",
            )
        ]
    background, mad = _median_mad(
        [pixel.no2_vertical_column for pixel in background_pixels]
    )
    seeds: set[str] = set()
    growth: set[str] = set()
    for pixel, distance, _ in positioned:
        enhancement = pixel.no2_vertical_column - background
        combined_uncertainty = math.hypot(pixel.no2_uncertainty, mad)
        if distance <= float(contract["aoi_radius_km"]) and enhancement > 0:
            growth.add(pixel.pixel_revision_id)
        if (
            distance <= float(contract["seed_radius_km"])
            and enhancement > float(contract["mad_multiplier"]) * mad
            and enhancement
            > float(contract["uncertainty_multiplier"]) * combined_uncertainty
        ):
            seeds.add(pixel.pixel_revision_id)
    if not seeds:
        return [
            DetectionResult(
                (),
                background_ids,
                rejected,
                background,
                mad,
                None,
                False,
                "no_detection",
            )
        ]
    components = _connected_components(ready, growth)
    detections: list[DetectionResult] = []
    by_id = {pixel.pixel_revision_id: pixel for pixel in ready}
    for component in components:
        if component.isdisjoint(seeds):
            continue
        is_ready = len(component) >= int(contract["min_component_pixels"])
        enhancement = sum(
            by_id[pixel_id].no2_vertical_column - background for pixel_id in component
        )
        detections.append(
            DetectionResult(
                tuple(sorted(component)),
                background_ids,
                rejected,
                background,
                mad,
                enhancement,
                is_ready,
                "analysis_ready" if is_ready else "insufficient_component",
            )
        )
    return sorted(detections, key=lambda item: item.plume_pixel_ids)


def _normalized(values: Sequence[float]) -> list[float]:
    low = min(values)
    high = max(values)
    if math.isclose(low, high):
        return [1.0 for _ in values]
    return [(value - low) / (high - low) for value in values]


def _softmax(values: Sequence[float], temperature: float) -> list[float]:
    if not math.isfinite(temperature) or temperature <= 0:
        raise ValueError("Calibration temperature must be positive and finite")
    scaled = [value / temperature for value in values]
    maximum = max(scaled)
    exponentials = [math.exp(value - maximum) for value in scaled]
    total = sum(exponentials)
    return [value / total for value in exponentials]


def classify_candidates(
    candidates: Sequence[CandidateInput],
    *,
    contract: Mapping[str, object],
    temperature: float | None,
    calibration_ece: float | None,
) -> list[CandidateResult]:
    if not candidates:
        return []
    trajectories = [
        max(0.0, min(1.0, item.trajectory_alignment)) for item in candidates
    ]
    distance_scores = [
        math.exp(-max(0.0, item.distance_km) / float(contract["distance_decay_km"]))
        for item in candidates
    ]
    annual_scores = _normalized([max(0.0, item.annual_nox_tons) for item in candidates])
    emission_values = [
        max(0.0, item.concurrent_nox_lbs)
        for item in candidates
        if item.concurrent_nox_lbs is not None
    ]
    emission_scores = (
        _normalized(emission_values)
        if len(emission_values) == len(candidates)
        else None
    )
    scores = [
        float(contract["trajectory_weight"]) * trajectories[index]
        + float(contract["concurrent_emissions_weight"])
        * (emission_scores[index] if emission_scores is not None else 0.0)
        + float(contract["distance_weight"]) * distance_scores[index]
        + float(contract["annual_emissions_weight"]) * annual_scores[index]
        for index in range(len(candidates))
    ]
    probability_ready = (
        emission_scores is not None
        and temperature is not None
        and calibration_ece is not None
        and calibration_ece <= float(contract["calibration_ece_max"])
    )
    probabilities = _softmax(scores, temperature) if probability_ready else None
    ordered = sorted(
        range(len(candidates)),
        key=lambda index: (-scores[index], candidates[index].facility_id),
    )
    result: list[CandidateResult] = []
    for rank, index in enumerate(ordered, start=1):
        probability = probabilities[index] if probabilities is not None else None
        lead = (
            probability - sorted(probabilities, reverse=True)[1]
            if probabilities is not None and rank == 1 and len(probabilities) > 1
            else probability
        )
        classification = "insufficient_evidence"
        if probability is not None:
            if (
                rank == 1
                and probability >= float(contract["likely_probability"])
                and (lead or 0.0) >= float(contract["likely_margin"])
            ):
                classification = "likely"
            elif probability >= float(contract["plausible_probability"]):
                classification = "plausible"
            if (
                len(probabilities) > 1
                and max(probabilities) - probability
                <= float(contract["ambiguous_margin"])
                and classification != "likely"
            ):
                classification = "ambiguous"
        candidate = candidates[index]
        result.append(
            CandidateResult(
                facility_id=candidate.facility_id,
                rank=rank,
                trajectory_score=trajectories[index],
                emissions_score=(
                    emission_scores[index] if emission_scores is not None else None
                ),
                distance_score=distance_scores[index],
                annual_prior_score=annual_scores[index],
                attribution_score=scores[index],
                probability=probability,
                classification=classification,
                distance_km=candidate.distance_km,
                is_cohort=candidate.is_cohort,
                is_probability_ready=probability_ready,
            )
        )
    return result


def expected_calibration_error(
    probabilities: Sequence[float],
    outcomes: Sequence[bool],
    *,
    bins: int = 10,
) -> float:
    if len(probabilities) != len(outcomes) or not probabilities:
        raise ValueError("Probabilities and outcomes must be non-empty and aligned")
    if bins < 1:
        raise ValueError("ECE requires at least one bin")
    if any(not 0 <= probability <= 1 for probability in probabilities):
        raise ValueError("Probabilities must be in [0, 1]")
    error = 0.0
    for index in range(bins):
        lower = index / bins
        upper = (index + 1) / bins
        members = [
            member
            for member, probability in enumerate(probabilities)
            if lower <= probability <= upper
            and (probability < upper or index == bins - 1)
        ]
        if members:
            confidence = statistics.fmean(probabilities[member] for member in members)
            accuracy = statistics.fmean(float(outcomes[member]) for member in members)
            error += len(members) / len(probabilities) * abs(confidence - accuracy)
    return error


def estimate_emissions(
    plume_pixels: Sequence[PixelRecord],
    *,
    background_molecules_cm2: float,
    wind_speeds_ms: Mapping[str, float],
    plume_length_km: float,
    contract: Mapping[str, object],
) -> list[EmissionEstimate]:
    if not plume_pixels:
        return []
    if not math.isfinite(plume_length_km) or plume_length_km <= 0:
        raise ValueError("Plume length must be positive and finite")
    mass_kg = sum(
        max(0.0, pixel.no2_vertical_column - background_molecules_cm2)
        * pixel.area_km2
        * 1e10
        / AVOGADRO
        * NO2_KG_PER_MOL
        for pixel in plume_pixels
    )
    retrieval_fraction = math.sqrt(
        sum(pixel.no2_uncertainty**2 for pixel in plume_pixels)
    ) / max(
        sum(
            max(0.0, pixel.no2_vertical_column - background_molecules_cm2)
            for pixel in plume_pixels
        ),
        1e-12,
    )
    estimates: list[EmissionEstimate] = []
    for wind_variant, wind_speed in sorted(wind_speeds_ms.items()):
        if not math.isfinite(wind_speed) or wind_speed <= 0:
            continue
        for lifetime in contract["chemical_lifetimes_hours"]:
            for ratio in contract["no2_nox_ratios"]:
                residence_hours = plume_length_km / (wind_speed * 3.6)
                chemical_correction = math.exp(residence_hours / float(lifetime))
                no2_flux = mass_kg / residence_hours * chemical_correction
                nox_flux = no2_flux / float(ratio)
                variant_id = (
                    f"{wind_variant}-tau{int(lifetime)}-ratio{float(ratio):.1f}"
                )
                is_central = (
                    wind_variant == f"{int(contract['central_wind_level_m'])}m"
                    and int(lifetime) == int(contract["central_lifetime_hours"])
                    and math.isclose(
                        float(ratio),
                        float(contract["central_no2_nox_ratio"]),
                    )
                )
                estimates.append(
                    EmissionEstimate(
                        variant_id=variant_id,
                        wind_variant=wind_variant,
                        wind_speed_ms=wind_speed,
                        lifetime_hours=float(lifetime),
                        no2_nox_ratio=float(ratio),
                        no2_flux_kg_h=no2_flux,
                        nox_flux_kg_h=nox_flux,
                        retrieval_uncertainty_kg_h=no2_flux * retrieval_fraction,
                        background_uncertainty_kg_h=no2_flux
                        * float(contract["background_uncertainty_fraction"]),
                        wind_uncertainty_kg_h=no2_flux
                        * float(contract["wind_uncertainty_fraction"]),
                        geometry_uncertainty_kg_h=no2_flux
                        * float(contract["geometry_uncertainty_fraction"]),
                        is_central=is_central,
                    )
                )
    return estimates


__all__ = [
    "CandidateInput",
    "CandidateResult",
    "DetectionResult",
    "EmissionEstimate",
    "PixelRecord",
    "classify_candidates",
    "detect_plumes",
    "estimate_emissions",
    "expected_calibration_error",
    "is_pixel_ready",
    "xy_km",
]
