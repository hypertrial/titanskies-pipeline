"""TEMPO grid-region weight generation and Parquet writers."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from titanskies_pipeline.geography.normalize import geo_modules
from titanskies_pipeline.geography.registry import WEIGHT_COLUMNS
from titanskies_pipeline.geography.tempo_grid import TEMPO_GRID


def iter_region_weight_tables(
    regions,
    *,
    geometry_version: str,
    row_chunk_size: int = 64,
):
    if row_chunk_size < 1:
        raise ValueError("row_chunk_size must be positive")
    _gpd, shapely, Transformer = geo_modules()
    project = Transformer.from_crs(4326, 6933, always_xy=True)
    half = TEMPO_GRID.step_degrees / 2
    for region in regions.sort_values("canonical_region_id").itertuples():
        region_geometry = region.geometry
        shapely.prepare(region_geometry)
        minx, miny, maxx, maxy = region_geometry.bounds
        row_start = max(
            0,
            int(
                np.ceil(
                    (miny - half - TEMPO_GRID.latitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        row_end = min(
            TEMPO_GRID.rows - 1,
            int(
                np.floor(
                    (maxy + half - TEMPO_GRID.latitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        col_start = max(
            0,
            int(
                np.ceil(
                    (minx - half - TEMPO_GRID.longitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        col_end = min(
            TEMPO_GRID.cols - 1,
            int(
                np.floor(
                    (maxx + half - TEMPO_GRID.longitude_start) / TEMPO_GRID.step_degrees
                )
            ),
        )
        if row_start > row_end or col_start > col_end:
            continue
        for chunk_start in range(row_start, row_end + 1, row_chunk_size):
            chunk_end = min(row_end + 1, chunk_start + row_chunk_size)
            rows = np.repeat(np.arange(chunk_start, chunk_end), col_end - col_start + 1)
            cols = np.tile(np.arange(col_start, col_end + 1), chunk_end - chunk_start)
            lat = TEMPO_GRID.latitude_start + rows * TEMPO_GRID.step_degrees
            lon = TEMPO_GRID.longitude_start + cols * TEMPO_GRID.step_degrees
            cells = shapely.box(lon - half, lat - half, lon + half, lat + half)
            intersects = shapely.intersects(region_geometry, cells)
            if not np.any(intersects):
                continue
            selected_cells = cells[intersects]
            contained = shapely.contains(region_geometry, selected_cells)
            area = np.empty(selected_cells.size, dtype=float)
            if np.any(contained):
                projected_cells = shapely.transform(
                    selected_cells[contained], project.transform, interleaved=False
                )
                area[contained] = shapely.area(projected_cells) / 1_000_000.0
            boundary = ~contained
            clipped = shapely.intersection(selected_cells[boundary], region_geometry)
            projected_clips = shapely.transform(
                clipped, project.transform, interleaved=False
            )
            area[boundary] = shapely.area(projected_clips) / 1_000_000.0
            keep = area > 0
            if not np.any(keep):
                continue
            selected_rows = rows[intersects][keep]
            selected_cols = cols[intersects][keep]
            yield pa.table(
                {
                    "grid_row": pa.array(selected_rows, type=pa.int32()),
                    "grid_col": pa.array(selected_cols, type=pa.int32()),
                    "canonical_region_id": [region.canonical_region_id]
                    * int(keep.sum()),
                    "overlap_weight": area[keep],
                    "geometry_version": [geometry_version] * int(keep.sum()),
                }
            ).select(WEIGHT_COLUMNS)


def atomic_parquet(table: pa.Table, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, temporary, compression="zstd", version="2.6")
        if pq.ParquetFile(temporary).metadata.num_rows != table.num_rows:
            raise ValueError(f"Parquet validation failed for {destination.name}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_weights(
    tables: Iterable[pa.Table],
    destination: Path,
    *,
    metadata: Mapping[bytes, bytes],
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    writer = None
    count = 0
    try:
        for table in tables:
            table = table.replace_schema_metadata(metadata)
            writer = writer or pq.ParquetWriter(
                temporary, table.schema, compression="zstd", version="2.6"
            )
            writer.write_table(table)
            count += table.num_rows
        if writer is None:
            raise ValueError("Production geography produced no grid weights")
        writer.close()
        writer = None
        parquet = pq.ParquetFile(temporary)
        if parquet.metadata.num_rows != count:
            raise ValueError("Grid-weight Parquet row-count validation failed")
        os.replace(temporary, destination)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
    return count


__all__ = [
    "atomic_parquet",
    "atomic_weights",
    "iter_region_weight_tables",
]
