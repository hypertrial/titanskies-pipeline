"""Columnar area-weighted aggregation from TEMPO cells to regions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from titanskies_pipeline.geography.tempo_grid import (
    GRID_VERSION,
    validate_grid_coordinates,
)
from titanskies_pipeline.ingestion.tempo.netcdf import (
    NetcdfGrid,
    accepted_quality_flags,
    quality_mask,
    weighted_stats,
)


@dataclass(frozen=True)
class RegionHourAggregate:
    observation_hour: str
    canonical_region_id: str
    country_code: str
    region_type: str
    no2_mean: float | None
    no2_median: float | None
    no2_p90: float | None
    valid_pixel_count: int
    total_pixel_count: int
    valid_area_km2: float
    total_area_km2: float
    coverage_fraction: float
    quality_flag_accepted: bool
    source_granule_count: int
    all_granules_validated: bool
    geometry_version: str


@dataclass(frozen=True)
class RegionWeights:
    rows: np.ndarray
    cols: np.ndarray
    overlap_area_km2: np.ndarray
    region_ids: tuple[str, ...]
    offsets: np.ndarray
    grid_version: str

    @classmethod
    def from_rows(
        cls,
        rows: list[dict[str, object]],
        *,
        grid_version: str = "synthetic",
    ) -> "RegionWeights":
        table = (
            pa.Table.from_pylist(rows)
            if rows
            else pa.table(
                {
                    "grid_row": pa.array([], type=pa.int32()),
                    "grid_col": pa.array([], type=pa.int32()),
                    "canonical_region_id": pa.array([], type=pa.string()),
                    "overlap_weight": pa.array([], type=pa.float64()),
                }
            )
        )
        return cls.from_table(table, grid_version=grid_version)

    @classmethod
    def from_table(
        cls,
        table: pa.Table,
        *,
        grid_version: str,
        sorted_input: bool = False,
    ) -> "RegionWeights":
        required = {"grid_row", "grid_col", "canonical_region_id", "overlap_weight"}
        missing = required - set(table.column_names)
        if missing:
            raise ValueError(
                f"Grid weights missing columns: {', '.join(sorted(missing))}"
            )
        table = table.select(sorted(required))
        if not sorted_input:
            table = table.sort_by(
                [
                    ("canonical_region_id", "ascending"),
                    ("grid_row", "ascending"),
                    ("grid_col", "ascending"),
                ]
            )
        encoded = pc.dictionary_encode(table["canonical_region_id"].combine_chunks())
        codes = encoded.indices.to_numpy(zero_copy_only=False)
        if codes.size and np.any(codes[1:] < codes[:-1]):
            raise ValueError("Grid weights must be sorted by canonical_region_id")
        starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
        offsets = np.r_[starts, codes.size].astype(np.int64, copy=False)
        region_ids = tuple(str(value.as_py()) for value in encoded.dictionary)
        return cls(
            rows=table["grid_row"].combine_chunks().to_numpy(zero_copy_only=False),
            cols=table["grid_col"].combine_chunks().to_numpy(zero_copy_only=False),
            overlap_area_km2=table["overlap_weight"]
            .combine_chunks()
            .to_numpy(zero_copy_only=False),
            region_ids=region_ids,
            offsets=offsets,
            grid_version=grid_version,
        )


def load_region_weights(path: Path) -> RegionWeights:
    parquet = pq.ParquetFile(path)
    metadata = parquet.schema_arrow.metadata or {}
    grid_version = metadata.get(b"grid_version", b"synthetic").decode()
    return RegionWeights.from_table(
        parquet.read(), grid_version=grid_version, sorted_input=True
    )


def aggregate_region_hour(
    *,
    grids: list[tuple[str, NetcdfGrid]],
    weights: RegionWeights,
    region_meta: Mapping[str, tuple[str, str]],
    geometry_version: str,
    accepted_flags: str,
) -> list[RegionHourAggregate]:
    """Pool exact cell observations across every validated scan in one UTC hour."""
    if not grids:
        raise ValueError("At least one granule grid is required")
    observation_hours = {grid.observation_hour for _granule_id, grid in grids}
    if len(observation_hours) != 1:
        raise ValueError("All pooled granules must share one observation hour")
    for _granule_id, grid in grids:
        if grid.no2.shape != grid.quality.shape:
            raise ValueError("NO2 and quality grid shapes must match")
        if weights.grid_version == GRID_VERSION:
            validate_grid_coordinates(grid.lat, grid.lon)
        if weights.rows.size and (
            int(weights.rows.max()) >= grid.no2.shape[0]
            or int(weights.cols.max()) >= grid.no2.shape[1]
        ):
            raise ValueError("Grid-region weights exceed NetCDF grid dimensions")

    accepted = accepted_quality_flags(accepted_flags)
    accepted_masks = [
        quality_mask(grid.quality, accepted) & np.isfinite(grid.no2)
        for _granule_id, grid in grids
    ]
    result: list[RegionHourAggregate] = []
    for index, region_id in enumerate(weights.region_ids):
        start, end = weights.offsets[index : index + 2]
        rows = weights.rows[start:end]
        cols = weights.cols[start:end]
        area = weights.overlap_area_km2[start:end].astype(float, copy=False)
        values_parts: list[np.ndarray] = []
        areas_parts: list[np.ndarray] = []
        valid_count = 0
        valid_area = 0.0
        for (_granule_id, grid), accepted_mask in zip(
            grids, accepted_masks, strict=True
        ):
            valid = accepted_mask[rows, cols]
            values_parts.append(grid.no2[rows[valid], cols[valid]])
            areas_parts.append(area[valid])
            valid_count += int(valid.sum())
            valid_area += float(area[valid].sum())
        values = np.concatenate(values_parts) if values_parts else np.array([])
        valid_weights = np.concatenate(areas_parts) if areas_parts else np.array([])
        stats = weighted_stats(values, valid_weights)
        country_code, region_type = region_meta[region_id]
        total_count = int(end - start) * len(grids)
        total_area = float(area.sum()) * len(grids)

        def nullable(name: str) -> float | None:
            value = float(stats[name])
            return value if np.isfinite(value) else None

        result.append(
            RegionHourAggregate(
                observation_hour=next(iter(observation_hours)),
                canonical_region_id=region_id,
                country_code=country_code,
                region_type=region_type,
                no2_mean=nullable("mean"),
                no2_median=nullable("median"),
                no2_p90=nullable("p90"),
                valid_pixel_count=valid_count,
                total_pixel_count=total_count,
                valid_area_km2=valid_area,
                total_area_km2=total_area,
                coverage_fraction=valid_area / total_area if total_area else 0.0,
                quality_flag_accepted=valid_count > 0,
                source_granule_count=len(grids),
                all_granules_validated=True,
                geometry_version=geometry_version,
            )
        )
    return result


def supported_grid_mask(
    weights: RegionWeights,
    region_meta: Mapping[str, tuple[str, str]],
    shape: tuple[int, int],
) -> np.ndarray:
    mask = np.zeros(shape, dtype=bool)
    for index, region_id in enumerate(weights.region_ids):
        if region_meta[region_id][1] != "country":
            continue
        start, end = weights.offsets[index : index + 2]
        mask[weights.rows[start:end], weights.cols[start:end]] = True
    return mask


__all__ = [
    "RegionHourAggregate",
    "RegionWeights",
    "aggregate_region_hour",
    "load_region_weights",
    "supported_grid_mask",
]
