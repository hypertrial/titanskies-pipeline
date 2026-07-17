from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scripts.build_region_artifacts import build_artifacts
from shapely.geometry import LineString, MultiPolygon, box

import titanskies_pipeline.geography.build as geo_build
from titanskies_pipeline.geography.build import (
    CANADA_PROVINCES,
    MEXICO_STATES,
    _atomic_parquet,
    _atomic_weights,
    _find_file,
    _repair_dissolve,
    _safe_extract,
    acquire_source,
    assign_dominant_timezones,
    build_production_artifacts,
    iter_region_weight_tables,
    load_source_manifest,
    publish_artifact_generation,
)


def test_committed_source_manifest_is_complete():
    root = Path(__file__).resolve().parents[3]
    manifest = load_source_manifest(root / "config" / "geography_sources.json")
    assert manifest["geometry_version"] == "v0.3-geo-2025-tz-2026b"
    assert {source["id"] for source in manifest["sources"]} == {
        "us_states_2025",
        "us_counties_2025",
        "canada_csd_2025",
        "mexico_geostatistical_2025",
        "land_timezones_2026b",
    }


def test_offline_source_cache_missing_and_checksum_rejection(tmp_path):
    source = {
        "id": "tiny",
        "filename": "tiny.zip",
        "url": "https://example.test/tiny.zip",
        "sha256": hashlib.sha256(b"valid").hexdigest(),
    }
    with pytest.raises(FileNotFoundError, match="not cached"):
        acquire_source(source, source_cache=tmp_path, offline=True)
    (tmp_path / "tiny.zip").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="failed checksum"):
        acquire_source(source, source_cache=tmp_path, offline=True)


def test_synthetic_artifacts_are_deterministic_and_atomic(tmp_path):
    first = build_artifacts(tmp_path / "first", use_synthetic=True)
    second = build_artifacts(tmp_path / "second", use_synthetic=True)
    assert first["registry_checksum"] == second["registry_checksum"]
    assert first["weights_checksum"] == second["weights_checksum"]
    assert first["region_count"] == 9
    assert first["weight_count"] == 9
    assert not list(tmp_path.rglob("*.tmp"))


def test_canonical_subdivision_code_maps_cover_examples():
    assert CANADA_PROVINCES["35"] == "ON"
    assert MEXICO_STATES["09"] == "CMX"


def test_invalid_geometry_is_repaired_and_dissolved():
    geopandas = pytest.importorskip("geopandas")
    shapely = pytest.importorskip("shapely")
    bowtie = shapely.Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    frame = geopandas.GeoDataFrame(
        {"canonical_region_id": ["US-X"], "geometry": [bowtie]}, crs=4326
    )

    repaired = _repair_dissolve(frame, by="canonical_region_id")

    assert len(repaired) == 1
    assert repaired.geometry.is_valid.all()


def test_dominant_timezone_uses_largest_equal_area_intersection():
    geopandas = pytest.importorskip("geopandas")
    shapely = pytest.importorskip("shapely")
    regions = geopandas.GeoDataFrame(
        {"geometry": [shapely.box(-100, 30, -96, 32)]}, crs=4326
    )
    timezones = geopandas.GeoDataFrame(
        {
            "tzid": ["America/Test_West", "America/Test_East"],
            "geometry": [
                shapely.box(-100, 30, -97, 32),
                shapely.box(-97, 30, -96, 32),
            ],
        },
        crs=4326,
    )

    assert assign_dominant_timezones(regions, timezones) == ["America/Test_West"]


def test_tiny_exact_cell_overlap_is_equal_area_square_kilometres():
    geopandas = pytest.importorskip("geopandas")
    shapely = pytest.importorskip("shapely")
    pyproj = pytest.importorskip("pyproj")
    region = shapely.box(-168.0, 14.0, -167.98, 14.02)
    regions = geopandas.GeoDataFrame(
        {"canonical_region_id": ["MX-TINY"], "geometry": [region]}, crs=4326
    )

    rows = list(
        iter_region_weight_tables(regions, geometry_version="tiny-v1", row_chunk_size=1)
    )
    records = [record for table in rows for record in table.to_pylist()]
    projected = shapely.transform(
        region,
        pyproj.Transformer.from_crs(4326, 6933, always_xy=True).transform,
        interleaved=False,
    )

    assert len(records) == 1
    assert records[0]["grid_row"] == 0
    assert records[0]["grid_col"] == 0
    assert records[0]["overlap_weight"] == pytest.approx(
        shapely.area(projected) / 1_000_000.0, rel=1e-12
    )

    half_region = shapely.box(-168.0, 14.0, -167.99, 14.02)
    half = geopandas.GeoDataFrame(
        {"canonical_region_id": ["MX-HALF"], "geometry": [half_region]}, crs=4326
    )
    half_record = list(
        iter_region_weight_tables(half, geometry_version="tiny-v1", row_chunk_size=1)
    )[0].to_pylist()[0]
    half_projected = shapely.transform(
        half_region,
        pyproj.Transformer.from_crs(4326, 6933, always_xy=True).transform,
        interleaved=False,
    )
    assert half_record["overlap_weight"] == pytest.approx(
        shapely.area(half_projected) / 1_000_000.0, rel=1e-12
    )


def _zip_boundary(root: Path, name: str, rows: dict[str, list[str]], geometry) -> Path:
    directory = root / name
    directory.mkdir()
    path = directory / f"{name}.shp"
    gpd.GeoDataFrame(rows, geometry=[geometry], crs=4326).to_file(path)
    archive = root / f"{name}.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        for member in sorted(directory.iterdir()):
            zipped.write(member, member.name)
    return archive


def _tiny_production_sources(root: Path) -> tuple[Path, Path]:
    source_cache = root / "cache"
    source_cache.mkdir()
    geometries = [
        box(-106.00, 35.00, -105.99, 35.01),
        box(-105.98, 35.00, -105.97, 35.01),
        box(-105.96, 35.00, -105.95, 35.01),
    ]
    archives = {
        "us_states_2025": _zip_boundary(
            root,
            "us_states_2025",
            {"STATEFP": ["06"], "STUSPS": ["CA"], "NAME": ["California"]},
            geometries[0],
        ),
        "us_counties_2025": _zip_boundary(
            root,
            "us_counties_2025",
            {
                "STATEFP": ["06"],
                "COUNTYFP": ["037"],
                "GEOID": ["06037"],
                "NAME": ["Los Angeles"],
            },
            geometries[0],
        ),
        "canada_csd_2025": _zip_boundary(
            root,
            "canada_csd_2025",
            {
                "PRUID": ["35"],
                "CSDUID": ["3520005"],
                "CSDNAME": ["Toronto"],
                "PRNAME": ["Ontario"],
            },
            geometries[1],
        ),
    }
    mexico_dir = root / "mexico"
    mexico_dir.mkdir()
    gpd.GeoDataFrame(
        {"CVE_ENT": ["09"], "NOMGEO": ["Ciudad de Mexico"]},
        geometry=[geometries[2]],
        crs=4326,
    ).to_file(mexico_dir / "tiny00ent.shp")
    gpd.GeoDataFrame(
        {"CVE_ENT": ["09"], "CVE_MUN": ["002"], "NOMGEO": ["Azcapotzalco"]},
        geometry=[geometries[2]],
        crs=4326,
    ).to_file(mexico_dir / "tiny00mun.shp")
    mexico = root / "mexico_geostatistical_2025.zip"
    with zipfile.ZipFile(mexico, "w") as zipped:
        for member in sorted(mexico_dir.iterdir()):
            zipped.write(member, member.name)
    archives["mexico_geostatistical_2025"] = mexico

    timezone_geojson = root / "timezones.geojson"
    gpd.GeoDataFrame(
        {"tzid": ["America/Denver"]},
        geometry=[box(-107, 34, -105, 36)],
        crs=4326,
    ).to_file(timezone_geojson, driver="GeoJSON")
    timezone_archive = root / "land_timezones_2026b.zip"
    with zipfile.ZipFile(timezone_archive, "w") as zipped:
        zipped.write(timezone_geojson, timezone_geojson.name)
    archives["land_timezones_2026b"] = timezone_archive

    sources = []
    for source_id, archive in archives.items():
        destination = source_cache / archive.name
        destination.write_bytes(archive.read_bytes())
        sources.append(
            {
                "id": source_id,
                "version": "tiny-v1",
                "url": f"https://example.test/{archive.name}",
                "filename": archive.name,
                "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                "attribution": "test",
                "license": "test",
            }
        )
    manifest = root / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "geometry_version": "tiny-production-v1",
                "sources": sources,
            }
        )
    )
    return manifest, source_cache


def test_offline_production_build_normalizes_hierarchy_and_reuses_extraction(tmp_path):
    manifest, source_cache = _tiny_production_sources(tmp_path)

    result = build_production_artifacts(
        output_dir=tmp_path / "artifacts",
        source_cache=source_cache,
        manifest_path=manifest,
        offline=True,
    )
    registry = pd.read_parquet(result["registry_path"])

    assert result["artifact_mode"] == "production"
    assert set(registry["canonical_region_id"]) == {
        "US",
        "US-CA",
        "US-CA-037",
        "CA",
        "CA-ON",
        "CA-ON-3520005",
        "MX",
        "MX-CMX",
        "MX-CMX-002",
    }
    assert set(registry["timezone"]) == {"America/Denver"}
    extracted = source_cache / "extracted" / "us_states_2025"
    extracted_generation = next(extracted.iterdir())
    assert _safe_extract(source_cache / "us_states_2025.zip", extracted) == (
        extracted_generation
    )


def test_safe_extract_rejects_path_traversal_and_find_file_missing(tmp_path):
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("../escape.txt", "nope")
    with pytest.raises(ValueError, match="Unsafe path"):
        _safe_extract(archive, tmp_path / "extracted")
    assert not (tmp_path / "escape.txt").exists()
    with pytest.raises(FileNotFoundError, match="None of"):
        _find_file(tmp_path, ".shp")


def test_safe_extract_tolerates_concurrent_identical_extraction(tmp_path, monkeypatch):
    archive = tmp_path / "safe.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("safe.txt", "ok")
    root = tmp_path / "extracted"
    destination = root / hashlib.sha256(archive.read_bytes()).hexdigest()
    destination.mkdir(parents=True)
    real_replace = geo_build.os.replace

    def concurrent_replace(source, target):
        if Path(target) == destination:
            raise FileExistsError("published concurrently")
        return real_replace(source, target)

    monkeypatch.setattr(geo_build.os, "replace", concurrent_replace)
    assert _safe_extract(archive, root) == destination
    assert not [path for path in root.iterdir() if path.name.startswith(".")]


def test_production_provider_field_rejection(tmp_path):
    manifest, source_cache = _tiny_production_sources(tmp_path)
    county_archive = source_cache / "us_counties_2025.zip"
    with zipfile.ZipFile(county_archive, "w") as zipped:
        bad = tmp_path / "bad-counties"
        bad.mkdir()
        gpd.GeoDataFrame(
            {"STATEFP": ["06"], "COUNTYFP": ["037"], "NAME": ["Los Angeles"]},
            geometry=[box(-106, 35, -105.99, 35.01)],
            crs=4326,
        ).to_file(bad / "counties.shp")
        for member in sorted(bad.iterdir()):
            zipped.write(member, member.name)
    payload = json.loads(manifest.read_text())
    for source in payload["sources"]:
        if source["id"] == "us_counties_2025":
            source["sha256"] = hashlib.sha256(county_archive.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="US counties.*GEOID"):
        build_production_artifacts(
            output_dir=tmp_path / "artifacts",
            source_cache=source_cache,
            manifest_path=manifest,
            offline=True,
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "incomplete"),
        (
            {
                "manifest_version": "1",
                "geometry_version": "test",
                "sources": [{"id": "broken"}],
            },
            "source broken is incomplete",
        ),
        (
            {
                "manifest_version": "1",
                "geometry_version": "test",
                "sources": [
                    {
                        "id": "broken",
                        "version": "1",
                        "url": "https://example.test",
                        "filename": "broken.zip",
                        "sha256": "short",
                        "attribution": "test",
                        "license": "test",
                    }
                ],
            },
            "invalid SHA-256",
        ),
    ],
)
def test_source_manifest_validation(tmp_path, payload, message):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=message):
        load_source_manifest(path)


class _Response:
    def __init__(self, content: bytes):
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, _size):
        return [b"", self.content]


def test_source_download_is_atomic_and_checksum_verified(tmp_path, monkeypatch):
    content = b"downloaded"
    source = {
        "id": "tiny",
        "filename": "tiny.zip",
        "url": "https://example.test/tiny.zip",
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    monkeypatch.setattr(geo_build.requests, "get", lambda *_a, **_k: _Response(content))
    downloaded = acquire_source(source, source_cache=tmp_path, offline=False)
    assert downloaded.read_bytes() == content
    assert acquire_source(source, source_cache=tmp_path, offline=True) == downloaded

    downloaded.write_bytes(b"stale")
    source["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="Downloaded.*failed checksum"):
        acquire_source(source, source_cache=tmp_path, offline=False)
    assert not list(tmp_path.glob("*.tmp"))


def test_geometry_and_weight_rejection_branches():
    empty = gpd.GeoDataFrame(
        {"canonical_region_id": ["US-X"]}, geometry=[None], crs=4326
    )
    with pytest.raises(ValueError, match="Invalid geometry remained"):
        _repair_dissolve(empty, by="canonical_region_id")

    regions = gpd.GeoDataFrame(
        {"canonical_region_id": ["US-X"]},
        geometry=[box(-106, 35, -105.99, 35.01)],
        crs=4326,
    )
    timezones = gpd.GeoDataFrame(
        {"tzid": ["America/Denver"]}, geometry=[box(-80, 30, -79, 31)], crs=4326
    )
    with pytest.raises(ValueError, match="no intersecting"):
        assign_dominant_timezones(regions, timezones)
    with pytest.raises(ValueError, match="row_chunk_size"):
        list(iter_region_weight_tables(regions, geometry_version="x", row_chunk_size=0))

    outside = gpd.GeoDataFrame(
        {"canonical_region_id": ["OUT"]}, geometry=[box(0, 0, 1, 1)], crs=4326
    )
    assert list(iter_region_weight_tables(outside, geometry_version="x")) == []

    separated = gpd.GeoDataFrame(
        {"canonical_region_id": ["GAP"]},
        geometry=[
            MultiPolygon(
                [
                    box(-106, 35, -105.999, 35.001),
                    box(-106, 35.08, -105.999, 35.081),
                ]
            )
        ],
        crs=4326,
    )
    assert list(
        iter_region_weight_tables(separated, geometry_version="x", row_chunk_size=1)
    )

    line = gpd.GeoDataFrame(
        {"canonical_region_id": ["LINE"]},
        geometry=[LineString([(-106, 35), (-105.98, 35)])],
        crs=4326,
    )
    assert list(iter_region_weight_tables(line, geometry_version="x")) == []


def test_atomic_weights_rejects_empty_input(tmp_path):
    with pytest.raises(ValueError, match="no grid weights"):
        _atomic_weights([], tmp_path / "weights.parquet", metadata={})
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("writer_kind", ["parquet", "weights"])
def test_atomic_parquet_writers_reject_row_count_mismatch(
    tmp_path, monkeypatch, writer_kind
):
    table = pa.table(
        {
            "grid_row": [0],
            "grid_col": [0],
            "canonical_region_id": ["US"],
            "overlap_weight": [1.0],
            "geometry_version": ["v1"],
        }
    )
    real_parquet_file = geo_build.pq.ParquetFile

    class WrongCount:
        def __init__(self, path):
            self._delegate = real_parquet_file(path)
            self.metadata = type("Metadata", (), {"num_rows": 2})()

    monkeypatch.setattr(geo_build.pq, "ParquetFile", WrongCount)
    if writer_kind == "parquet":
        with pytest.raises(ValueError, match="Parquet validation failed"):
            _atomic_parquet(table, tmp_path / "artifact.parquet")
    else:
        with pytest.raises(ValueError, match="row-count validation failed"):
            _atomic_weights([table], tmp_path / "weights.parquet", metadata={})
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_weights_closes_writer_after_write_failure(tmp_path, monkeypatch):
    closed = []

    class BrokenWriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def write_table(self, _table):
            raise OSError("disk full")

        def close(self):
            closed.append(True)

    monkeypatch.setattr(geo_build.pq, "ParquetWriter", BrokenWriter)
    with pytest.raises(OSError, match="disk full"):
        _atomic_weights(
            [
                pa.table(
                    {
                        "grid_row": [0],
                        "grid_col": [0],
                        "canonical_region_id": ["US"],
                        "overlap_weight": [1.0],
                        "geometry_version": ["v1"],
                    }
                )
            ],
            tmp_path / "weights.parquet",
            metadata={},
        )
    assert closed == [True]


def test_generation_publication_rejects_invalid_inputs_and_corruption(tmp_path):
    registry = pa.Table.from_pylist(
        [
            {
                "country_code": "US",
                "region_type": "country",
                "source_region_id": "US",
                "canonical_region_id": "US",
                "region_name": "United States",
                "parent_region_id": None,
                "timezone": "America/Denver",
                "geometry_version": "v1",
                "geometry_checksum": "a" * 64,
            }
        ]
    )
    weights = pa.Table.from_pylist(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
                "geometry_version": "v1",
            }
        ]
    )

    def write_sources(root, registry_table=registry, weights_table=weights):
        root.mkdir(parents=True)
        registry_path = root / "registry.parquet"
        weights_path = root / "weights.parquet"
        metadata = {
            b"grid_version": b"grid-v1",
            b"geometry_version": b"v1",
            b"source_manifest_sha256": b"a" * 64,
        }
        pq.write_table(registry_table.replace_schema_metadata(metadata), registry_path)
        pq.write_table(weights_table.replace_schema_metadata(metadata), weights_path)
        return registry_path, weights_path

    common = {
        "output_dir": tmp_path / "geo",
        "artifact_mode": "synthetic",
        "geometry_version": "v1",
        "grid_version": "grid-v1",
        "source_manifest_sha256": "a" * 64,
        "region_count": 1,
        "weight_count": 1,
    }
    registry_path, weights_path = write_sources(tmp_path / "unknown")
    with pytest.raises(ValueError, match="Unknown geography artifact mode"):
        publish_artifact_generation(
            **{**common, "artifact_mode": "unknown"},
            registry_source=registry_path,
            weights_source=weights_path,
        )

    registry_path, weights_path = write_sources(
        tmp_path / "schema", pa.table({"canonical_region_id": ["US"]}), weights
    )
    with pytest.raises(ValueError, match="schema validation"):
        publish_artifact_generation(
            **common, registry_source=registry_path, weights_source=weights_path
        )

    registry_path, weights_path = write_sources(tmp_path / "count")
    with pytest.raises(ValueError, match="row-count validation"):
        publish_artifact_generation(
            **{**common, "region_count": 0},
            registry_source=registry_path,
            weights_source=weights_path,
        )

    registry_path, weights_path = write_sources(tmp_path / "metadata")
    with pytest.raises(ValueError, match="metadata validation"):
        publish_artifact_generation(
            **{**common, "grid_version": "wrong-grid"},
            registry_source=registry_path,
            weights_source=weights_path,
        )

    unsorted_registry = pa.concat_tables([registry, registry])
    unsorted_weights = pa.concat_tables([weights, weights])
    registry_path, weights_path = write_sources(
        tmp_path / "unsorted", unsorted_registry, unsorted_weights
    )
    with pytest.raises(ValueError, match="canonically sorted"):
        publish_artifact_generation(
            **{**common, "region_count": 2, "weight_count": 2},
            registry_source=registry_path,
            weights_source=weights_path,
        )

    registry_path, weights_path = write_sources(tmp_path / "valid")
    first = publish_artifact_generation(
        **common, registry_source=registry_path, weights_source=weights_path
    )

    registry_path, weights_path = write_sources(tmp_path / "reuse")
    reused = publish_artifact_generation(
        **common, registry_source=registry_path, weights_source=weights_path
    )
    assert reused["build_id"] == first["build_id"]
    assert not registry_path.exists()
    assert not weights_path.exists()

    Path(first["registry_path"]).write_bytes(b"corrupt")
    registry_path, weights_path = write_sources(tmp_path / "retry")
    with pytest.raises(ValueError, match="generation.*corrupt"):
        publish_artifact_generation(
            **common, registry_source=registry_path, weights_source=weights_path
        )


@pytest.mark.parametrize(
    ("failed_call", "message"),
    [(3, "Registry checksum changed"), (4, "Weight checksum changed")],
)
def test_generation_rejects_checksum_change_during_publication(
    tmp_path, monkeypatch, failed_call, message
):
    registry = pa.Table.from_pylist(
        [
            {
                "country_code": "US",
                "region_type": "country",
                "source_region_id": "US",
                "canonical_region_id": "US",
                "region_name": "United States",
                "parent_region_id": None,
                "timezone": "America/Denver",
                "geometry_version": "v1",
                "geometry_checksum": "a" * 64,
            }
        ]
    )
    weights = pa.Table.from_pylist(
        [
            {
                "grid_row": 0,
                "grid_col": 0,
                "canonical_region_id": "US",
                "overlap_weight": 1.0,
                "geometry_version": "v1",
            }
        ]
    )
    source = tmp_path / "source"
    source.mkdir()
    registry_path = source / "registry.parquet"
    weights_path = source / "weights.parquet"
    metadata = {
        b"grid_version": b"grid-v1",
        b"geometry_version": b"v1",
        b"source_manifest_sha256": b"a" * 64,
    }
    pq.write_table(registry.replace_schema_metadata(metadata), registry_path)
    pq.write_table(weights.replace_schema_metadata(metadata), weights_path)
    real_checksum = geo_build.sha256_file
    calls = 0

    def changing_checksum(path):
        nonlocal calls
        calls += 1
        return "changed" if calls == failed_call else real_checksum(path)

    monkeypatch.setattr(geo_build, "sha256_file", changing_checksum)
    with pytest.raises(ValueError, match=message):
        publish_artifact_generation(
            output_dir=tmp_path / "geo",
            registry_source=registry_path,
            weights_source=weights_path,
            artifact_mode="synthetic",
            geometry_version="v1",
            grid_version="grid-v1",
            source_manifest_sha256="a" * 64,
            region_count=1,
            weight_count=1,
        )
    assert not list((tmp_path / "geo" / "generations").glob(".*.tmp"))
