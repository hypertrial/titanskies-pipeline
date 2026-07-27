"""Administrative boundary normalization and timezone assignment."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

from titanskies_pipeline.geography.acquire import find_file, safe_extract
from titanskies_pipeline.geography.registry import REGISTRY_COLUMNS

CANADA_PROVINCES = {
    "10": "NL",
    "11": "PE",
    "12": "NS",
    "13": "NB",
    "24": "QC",
    "35": "ON",
    "46": "MB",
    "47": "SK",
    "48": "AB",
    "59": "BC",
    "60": "YT",
    "61": "NT",
    "62": "NU",
}

MEXICO_STATES = {
    "01": "AGU",
    "02": "BCN",
    "03": "BCS",
    "04": "CAM",
    "05": "COA",
    "06": "COL",
    "07": "CHP",
    "08": "CHH",
    "09": "CMX",
    "10": "DUR",
    "11": "GUA",
    "12": "GRO",
    "13": "HID",
    "14": "JAL",
    "15": "MEX",
    "16": "MIC",
    "17": "MOR",
    "18": "NAY",
    "19": "NLE",
    "20": "OAX",
    "21": "PUE",
    "22": "QUE",
    "23": "ROO",
    "24": "SLP",
    "25": "SIN",
    "26": "SON",
    "27": "TAB",
    "28": "TAM",
    "29": "TLA",
    "30": "VER",
    "31": "YUC",
    "32": "ZAC",
}

COUNTRY_NAMES = {
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
}


def geo_modules():
    try:
        import geopandas as gpd
        import shapely
        from pyproj import Transformer
    except ImportError as exc:  # pragma: no cover - exercised without geo extra
        raise RuntimeError(
            "Install geography dependencies: uv sync --locked --extra geo"
        ) from exc
    return gpd, shapely, Transformer


def require_columns(frame, columns: set[str], provider: str) -> None:
    missing = columns - set(frame.columns)
    if missing:
        raise ValueError(
            f"{provider} boundary is missing required fields: "
            f"{', '.join(sorted(missing))}"
        )


def repair_dissolve(frame, *, by: str):
    _gpd, shapely, _transformer = geo_modules()
    normalized = frame.to_crs(4326).copy()
    normalized.geometry = shapely.make_valid(normalized.geometry.array)
    normalized = normalized[~normalized.geometry.is_empty & normalized.geometry.notna()]
    normalized = normalized.dissolve(by=by, as_index=False, sort=True)
    normalized.geometry = shapely.make_valid(normalized.geometry.array)
    if normalized.empty or not normalized.geometry.is_valid.all():
        raise ValueError(f"Invalid geometry remained after dissolving by {by}")
    return normalized


def canonical_frame(frame, rows: Iterable[dict[str, Any]]):
    gpd, _shapely, _transformer = geo_modules()
    metadata = list(rows)
    result = gpd.GeoDataFrame(metadata, geometry=frame.geometry.array, crs=frame.crs)
    return repair_dissolve(result, by="canonical_region_id")


def provider_regions(paths: Mapping[str, Path], source_cache: Path):
    gpd, _shapely, _transformer = geo_modules()
    import pandas as pd

    extracted = {
        key: safe_extract(path, source_cache / "extracted" / key)
        for key, path in paths.items()
    }

    us_states = gpd.read_file(find_file(extracted["us_states_2025"], ".shp"))
    us_counties = gpd.read_file(find_file(extracted["us_counties_2025"], ".shp"))
    require_columns(us_states, {"STATEFP", "STUSPS", "NAME"}, "US states")
    require_columns(
        us_counties, {"STATEFP", "COUNTYFP", "GEOID", "NAME"}, "US counties"
    )
    state_by_fips = dict(zip(us_states["STATEFP"], us_states["STUSPS"], strict=True))
    us_first = canonical_frame(
        us_states,
        (
            {
                "country_code": "US",
                "region_type": "state",
                "source_region_id": str(row.STATEFP),
                "canonical_region_id": f"US-{row.STUSPS}",
                "region_name": str(row.NAME),
                "parent_region_id": "US",
            }
            for row in us_states.itertuples()
        ),
    )
    us_finest = canonical_frame(
        us_counties,
        (
            {
                "country_code": "US",
                "region_type": "county",
                "source_region_id": str(row.GEOID),
                "canonical_region_id": (
                    f"US-{state_by_fips[str(row.STATEFP)]}-{str(row.COUNTYFP).zfill(3)}"
                ),
                "region_name": str(row.NAME),
                "parent_region_id": f"US-{state_by_fips[str(row.STATEFP)]}",
            }
            for row in us_counties.itertuples()
        ),
    )

    canada = gpd.read_file(find_file(extracted["canada_csd_2025"], ".shp"))
    require_columns(canada, {"PRUID", "CSDUID", "CSDNAME", "PRNAME"}, "Canadian CSD")
    ca_finest = canonical_frame(
        canada,
        (
            {
                "country_code": "CA",
                "region_type": "census_subdivision",
                "source_region_id": str(row.CSDUID),
                "canonical_region_id": (
                    f"CA-{CANADA_PROVINCES[str(row.PRUID)]}-{row.CSDUID}"
                ),
                "region_name": str(row.CSDNAME),
                "parent_region_id": f"CA-{CANADA_PROVINCES[str(row.PRUID)]}",
                "first_name": str(row.PRNAME),
            }
            for row in canada.itertuples()
        ),
    )
    ca_first = derive_parents(ca_finest, country="CA", region_type="province")

    mexico_root = extracted["mexico_geostatistical_2025"]
    mexico_states = gpd.read_file(find_file(mexico_root, "00ent.shp"))
    mexico_municipalities = gpd.read_file(find_file(mexico_root, "00mun.shp"))
    require_columns(mexico_states, {"CVE_ENT", "NOMGEO"}, "Mexican AGEE")
    require_columns(
        mexico_municipalities,
        {"CVE_ENT", "CVE_MUN", "NOMGEO"},
        "Mexican AGEM",
    )
    mx_first = canonical_frame(
        mexico_states,
        (
            {
                "country_code": "MX",
                "region_type": "state",
                "source_region_id": str(row.CVE_ENT).zfill(2),
                "canonical_region_id": (
                    f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}"
                ),
                "region_name": str(row.NOMGEO),
                "parent_region_id": "MX",
            }
            for row in mexico_states.itertuples()
        ),
    )
    mx_finest = canonical_frame(
        mexico_municipalities,
        (
            {
                "country_code": "MX",
                "region_type": "municipality",
                "source_region_id": (
                    f"{str(row.CVE_ENT).zfill(2)}{str(row.CVE_MUN).zfill(3)}"
                ),
                "canonical_region_id": (
                    f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}-"
                    f"{str(row.CVE_MUN).zfill(3)}"
                ),
                "region_name": str(row.NOMGEO),
                "parent_region_id": (f"MX-{MEXICO_STATES[str(row.CVE_ENT).zfill(2)]}"),
            }
            for row in mexico_municipalities.itertuples()
        ),
    )

    frames = [us_first, us_finest, ca_first, ca_finest, mx_first, mx_finest]
    countries = derive_countries([us_first, ca_first, mx_first])
    regions = gpd.GeoDataFrame(
        pd.concat([countries, *frames], ignore_index=True), crs=4326
    )
    timezone_path = find_file(extracted["land_timezones_2026b"], ".geojson", ".json")
    timezones = gpd.read_file(timezone_path).to_crs(4326)
    require_columns(timezones, {"tzid"}, "timezone")
    return regions.sort_values("canonical_region_id").reset_index(drop=True), timezones


def derive_parents(finest, *, country: str, region_type: str):
    gpd, _shapely, _transformer = geo_modules()
    rows = []
    for parent_id, group in finest.groupby("parent_region_id", sort=True):
        rows.append(
            {
                "country_code": country,
                "region_type": region_type,
                "source_region_id": parent_id.split("-")[-1],
                "canonical_region_id": parent_id,
                "region_name": str(group.iloc[0].get("first_name", parent_id)),
                "parent_region_id": country,
                "geometry": group.geometry.union_all(),
            }
        )
    return repair_dissolve(gpd.GeoDataFrame(rows, crs=4326), by="canonical_region_id")


def derive_countries(first_frames):
    gpd, _shapely, _transformer = geo_modules()
    rows = []
    for frame in first_frames:
        country = str(frame.iloc[0].country_code)
        rows.append(
            {
                "country_code": country,
                "region_type": "country",
                "source_region_id": country,
                "canonical_region_id": country,
                "region_name": COUNTRY_NAMES[country],
                "parent_region_id": None,
                "geometry": frame.geometry.union_all(),
            }
        )
    return gpd.GeoDataFrame(rows, crs=4326)


def assign_dominant_timezones(regions, timezones):
    _gpd, shapely, Transformer = geo_modules()
    project = Transformer.from_crs(4326, 6933, always_xy=True)
    timezone_geometries = timezones.geometry.array
    timezone_ids = timezones["tzid"].astype(str).to_numpy()
    index = timezones.sindex
    assignments: list[str] = []
    for region in regions.geometry.array:
        candidates = list(index.query(region, predicate="intersects"))
        if not candidates:
            raise ValueError("A region has no intersecting IANA land timezone")
        intersections = shapely.intersection(timezone_geometries[candidates], region)
        projected = shapely.transform(
            intersections, project.transform, interleaved=False
        )
        areas = shapely.area(projected)
        assignments.append(str(timezone_ids[candidates[int(np.argmax(areas))]]))
    return assignments


def geometry_checksum(geometry) -> str:
    _gpd, shapely, _transformer = geo_modules()
    return hashlib.sha256(shapely.to_wkb(geometry, hex=False)).hexdigest()


def region_registry_table(regions, *, geometry_version: str) -> pa.Table:
    records = []
    for row in regions.itertuples():
        records.append(
            {
                "country_code": row.country_code,
                "region_type": row.region_type,
                "source_region_id": row.source_region_id,
                "canonical_region_id": row.canonical_region_id,
                "region_name": row.region_name,
                "parent_region_id": row.parent_region_id,
                "timezone": row.timezone,
                "geometry_version": geometry_version,
                "geometry_checksum": geometry_checksum(row.geometry),
            }
        )
    return pa.Table.from_pylist(records).select(REGISTRY_COLUMNS)


__all__ = [
    "CANADA_PROVINCES",
    "COUNTRY_NAMES",
    "MEXICO_STATES",
    "assign_dominant_timezones",
    "canonical_frame",
    "derive_countries",
    "derive_parents",
    "geo_modules",
    "geometry_checksum",
    "provider_regions",
    "region_registry_table",
    "repair_dissolve",
    "require_columns",
]
