from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from titanskies_pipeline.riverpulse import network
from titanskies_pipeline.riverpulse.network import (
    NetworkArtifacts,
    acquire_sword_archive,
    build_production_network,
    load_network_artifacts,
    load_sword_source_manifest,
    persist_network_artifacts,
    publish_network_generation,
    select_mainstem,
    synthetic_network_rows,
)


class FakeGeometry:
    def __init__(self, x: float, y: float):
        self.wkb = b"wkb-" + str((x, y)).encode()
        self.centroid = SimpleNamespace(x=x, y=y)


class FakeDownload:
    def __init__(self, body: bytes):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def raise_for_status(self):
        return None

    def iter_content(self, _size):
        yield self.body


def _records(river: str = "Sacramento River"):
    return [
        {
            "reach_id": "A",
            "river_name": river,
            "end_reach": 2,
            "dist_out": 0,
            "facc": 300,
            "rch_id_up": ["B", "C"],
            "rch_id_dn": [],
            "geometry": FakeGeometry(1, 1),
            "reach_len": 10,
        },
        {
            "reach_id": "B",
            "river_name": river,
            "dist_out": 10,
            "facc": 200,
            "rch_id_up": ["D"],
            "rch_id_dn": ["A"],
            "geometry": FakeGeometry(2, 2),
            "reach_len": 10,
        },
        {
            "reach_id": "C",
            "river_name": river,
            "dist_out": 10,
            "facc": 100,
            "rch_id_up": [],
            "rch_id_dn": ["A"],
            "geometry": FakeGeometry(3, 3),
            "reach_len": 10,
        },
        {
            "reach_id": "D",
            "river_name": river,
            "dist_out": 20,
            "facc": 150,
            "rch_id_up": [],
            "rch_id_dn": ["B"],
            "geometry": FakeGeometry(4, 4),
            "reach_len": 10,
        },
    ]


def _publish(tmp_path: Path) -> NetworkArtifacts:
    reaches, edges, anchors = synthetic_network_rows()
    return publish_network_generation(
        output_dir=tmp_path / "network",
        reaches=reaches,
        edges=edges,
        artifact_mode="synthetic",
        source_manifest_sha256=hashlib.sha256(b"synthetic").hexdigest(),
        resolved_anchors=anchors,
    )


def test_select_mainstem_uses_largest_flow_branch_and_records_boundary():
    reaches, edges, anchor = select_mainstem(
        _records(),
        basin_key="sacramento",
        aliases=["Sacramento"],
        max_reaches=3,
    )
    assert anchor == "A"
    assert [row["reach_id"] for row in reaches] == ["A", "B", "D"]
    assert any(
        edge["from_reach_id"] == "A"
        and edge["to_reach_id"] == "C"
        and edge["is_selection_boundary"]
        for edge in edges
    )
    explicit = select_mainstem(
        _records(),
        basin_key="sacramento",
        aliases=["Sacramento"],
        max_reaches=1,
        anchor_reach_id="B",
    )
    assert explicit[2] == "B"
    zero_flow = _records()
    zero_flow[1]["facc"] = 0
    zero_flow[2]["facc"] = None
    assert [
        row["reach_id"]
        for row in select_mainstem(
            zero_flow,
            basin_key="sacramento",
            aliases=["Sacramento"],
            max_reaches=2,
        )[0]
    ] == ["A", "B"]
    unrelated = _records() + _records("Rhine")
    for index, row in enumerate(unrelated[4:], start=1):
        row["reach_id"] = f"RH{index}"
    with pytest.raises(ValueError, match="does not match pilot"):
        select_mainstem(
            unrelated,
            basin_key="sacramento",
            aliases=["Sacramento"],
            anchor_reach_id="RH1",
        )


def test_select_mainstem_rejects_invalid_topology_and_selection():
    with pytest.raises(ValueError, match="between 1 and 100"):
        select_mainstem(
            _records(), basin_key="x", aliases=["Sacramento"], max_reaches=101
        )
    with pytest.raises(ValueError, match="Unknown reviewed outlet"):
        select_mainstem(
            _records(),
            basin_key="x",
            aliases=["Sacramento"],
            anchor_reach_id="missing",
        )
    with pytest.raises(ValueError, match="No SWORD reaches"):
        select_mainstem(_records(), basin_key="x", aliases=["Rhine"])
    duplicate = _records() + [_records()[0]]
    with pytest.raises(ValueError, match="duplicate or missing"):
        select_mainstem(duplicate, basin_key="x", aliases=["Sacramento"])
    inconsistent = _records()
    inconsistent[1]["rch_id_dn"] = []
    with pytest.raises(ValueError, match="reciprocal"):
        select_mainstem(
            inconsistent, basin_key="x", aliases=["Sacramento"], max_reaches=3
        )
    self_edge = _records()
    self_edge[0]["rch_id_up"] = ["A"]
    with pytest.raises(ValueError, match="self-edge"):
        select_mainstem(self_edge, basin_key="x", aliases=["Sacramento"])


def test_publish_load_and_reuse_immutable_generation(tmp_path):
    artifacts = _publish(tmp_path)
    loaded = load_network_artifacts(artifacts.manifest_path, allow_synthetic=True)
    assert loaded.reach_count == 9
    assert loaded.edge_count == 15
    assert set(loaded.resolved_anchors) == {"sacramento", "rhine", "murray"}
    assert _publish(tmp_path).build_id == artifacts.build_id
    with pytest.raises(ValueError, match="production SWORD"):
        load_network_artifacts(artifacts.manifest_path)


def test_network_manifest_rejects_escape_checksum_and_unsupported_versions(tmp_path):
    artifacts = _publish(tmp_path)
    payload = json.loads(artifacts.manifest_path.read_text())
    payload["reaches"]["path"] = "../outside.parquet"
    escaped = tmp_path / "escaped.json"
    escaped.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="escapes"):
        load_network_artifacts(escaped, allow_synthetic=True)

    payload = json.loads(artifacts.manifest_path.read_text())
    payload["network_version"] = "18"
    unsupported = tmp_path / "unsupported.json"
    unsupported.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Unsupported SWORD"):
        load_network_artifacts(unsupported, allow_synthetic=True)

    payload = json.loads(artifacts.manifest_path.read_text())
    payload["build_id"] = "wrong"
    wrong_identity = artifacts.manifest_path.parent / "wrong-identity.json"
    wrong_identity.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="build identity"):
        load_network_artifacts(wrong_identity, allow_synthetic=True)

    payload = json.loads(artifacts.manifest_path.read_text())
    payload["resolved_anchors"]["sacramento"] = "RP1002"
    wrong_anchor = artifacts.manifest_path.parent / "wrong-anchor.json"
    wrong_anchor.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="resolved anchors"):
        load_network_artifacts(wrong_anchor, allow_synthetic=True)

    artifacts.reaches_path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_network_artifacts(artifacts.manifest_path, allow_synthetic=True)


def test_network_table_validation_rejects_duplicate_self_and_missing_edges(tmp_path):
    reaches, edges, anchors = synthetic_network_rows()
    with pytest.raises(ValueError, match="duplicate"):
        publish_network_generation(
            output_dir=tmp_path / "duplicate",
            reaches=reaches + [reaches[0]],
            edges=edges,
            artifact_mode="synthetic",
            source_manifest_sha256="a" * 64,
            resolved_anchors=anchors,
        )
    bad_edges = [*edges, {**edges[0], "from_reach_id": edges[0]["to_reach_id"]}]
    with pytest.raises(ValueError, match="self-edge"):
        publish_network_generation(
            output_dir=tmp_path / "self",
            reaches=reaches,
            edges=bad_edges,
            artifact_mode="synthetic",
            source_manifest_sha256="a" * 64,
            resolved_anchors=anchors,
        )
    missing_reciprocal = edges[1:]
    with pytest.raises(ValueError, match="reciprocal"):
        publish_network_generation(
            output_dir=tmp_path / "reciprocal",
            reaches=reaches,
            edges=missing_reciprocal,
            artifact_mode="synthetic",
            source_manifest_sha256="a" * 64,
            resolved_anchors=anchors,
        )
    with pytest.raises(ValueError, match="Unknown RiverPulse"):
        publish_network_generation(
            output_dir=tmp_path / "mode",
            reaches=reaches,
            edges=edges,
            artifact_mode="test",
            source_manifest_sha256="a" * 64,
            resolved_anchors=anchors,
        )
    with pytest.raises(ValueError, match="resolved anchors"):
        publish_network_generation(
            output_dir=tmp_path / "anchors",
            reaches=reaches,
            edges=edges,
            artifact_mode="synthetic",
            source_manifest_sha256="a" * 64,
            resolved_anchors={"sacramento": anchors["sacramento"]},
        )


def test_source_manifest_and_verified_cache(monkeypatch, tmp_path):
    body = b"official sword archive"
    source = {
        "id": "sword",
        "version": "17b",
        "url": "https://example.test/sword.zip",
        "filename": "sword.zip",
        "checksum_algorithm": "md5",
        "checksum": hashlib.md5(body).hexdigest(),  # noqa: S324 - source contract
        "attribution": "source",
        "license": "terms",
    }
    manifest_path = tmp_path / "sources.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [source],
            }
        )
    )
    assert load_sword_source_manifest(manifest_path)["network_version"] == "17b"
    monkeypatch.setattr(
        network.requests, "get", lambda *_args, **_kwargs: FakeDownload(body)
    )
    archive = acquire_sword_archive(
        source, source_cache=tmp_path / "cache", offline=False
    )
    assert archive.read_bytes() == body
    assert (
        acquire_sword_archive(source, source_cache=tmp_path / "cache", offline=True)
        == archive
    )
    archive.write_bytes(b"bad")
    with pytest.raises(ValueError, match="failed checksum"):
        acquire_sword_archive(source, source_cache=tmp_path / "cache", offline=True)
    archive.unlink()
    with pytest.raises(FileNotFoundError, match="not cached"):
        acquire_sword_archive(source, source_cache=tmp_path / "cache", offline=True)

    class EmptyThenBodyDownload(FakeDownload):
        def iter_content(self, _size):
            yield b""
            yield self.body

    monkeypatch.setattr(
        network.requests,
        "get",
        lambda *_args, **_kwargs: EmptyThenBodyDownload(body),
    )
    assert (
        acquire_sword_archive(
            source, source_cache=tmp_path / "other-cache", offline=False
        ).read_bytes()
        == body
    )


def test_source_manifest_and_download_reject_bad_contract(monkeypatch, tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="incomplete"):
        load_sword_source_manifest(path)
    path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "18",
                "sources": [],
            }
        )
    )
    with pytest.raises(ValueError, match="Unsupported SWORD"):
        load_sword_source_manifest(path)
    path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [],
            }
        )
    )
    with pytest.raises(ValueError, match="exactly one source"):
        load_sword_source_manifest(path)
    incomplete_source = {
        "manifest_version": "1",
        "network_version": "17b",
        "sources": [{"id": "missing-fields"}],
    }
    path.write_text(json.dumps(incomplete_source))
    with pytest.raises(ValueError, match="entry is incomplete"):
        load_sword_source_manifest(path)
    invalid_algorithm = {
        "id": "sword",
        "version": "17b",
        "url": "https://example.test/sword.zip",
        "filename": "sword.zip",
        "checksum_algorithm": "not-a-digest",
        "checksum": "0",
        "attribution": "source",
        "license": "terms",
    }
    path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [invalid_algorithm],
            }
        )
    )
    with pytest.raises(ValueError, match="checksum algorithm"):
        load_sword_source_manifest(path)
    source = {
        "id": "sword",
        "version": "17b",
        "url": "https://example.test/sword.zip",
        "filename": "sword.zip",
        "checksum_algorithm": "md5",
        "checksum": "0" * 32,
        "attribution": "source",
        "license": "terms",
    }
    monkeypatch.setattr(
        network.requests, "get", lambda *_args, **_kwargs: FakeDownload(b"bad")
    )
    with pytest.raises(ValueError, match="Downloaded SWORD"):
        acquire_sword_archive(source, source_cache=tmp_path / "cache", offline=False)
    with pytest.raises(ValueError, match="cache-relative basename"):
        acquire_sword_archive(
            {**source, "filename": "../sword.zip"},
            source_cache=tmp_path / "cache",
            offline=True,
        )


def test_persist_network_and_reject_version_change_with_observations(duck, tmp_path):
    artifacts = _publish(tmp_path)
    with duck.get_connection() as conn:
        assert persist_network_artifacts(artifacts, conn=conn) == {
            "reaches_loaded": 9,
            "edges_loaded": 15,
        }
        manifest_row = conn.execute(
            """
            select network_version, reaches_path, edges_path
            from riverpulse_events_ops.network_artifact_manifest
            """
        ).fetchone()
        assert manifest_row[0] == "17b"
        assert not Path(manifest_row[1]).is_absolute()
        conn.execute(
            """
            insert into riverpulse_events_raw.observation_revisions (
                observation_revision_id, observation_id, reach_id,
                observation_time, cycle_id, pass_id, wse_unit, width_unit,
                slope_unit, collection_name, collection_version, crid,
                sword_version, granule_id, source_ingest_time, collected_at,
                response_sha256, canonical_record_json
            ) values (
                'r', 'o', 'RP1001', current_timestamp, 1, 1, 'm', 'm',
                'm/m', 'SWOT_L2_HR_RiverSP_reach_D', 'D', 'PIC0',
                '17b', 'g', current_timestamp, current_timestamp, 'sha', '{}'
            )
            """
        )
        with pytest.raises(RuntimeError, match="clean warehouse"):
            persist_network_artifacts(
                replace(artifacts, network_version="18"), conn=conn
            )
        with pytest.raises(RuntimeError, match="generation changes"):
            persist_network_artifacts(
                replace(artifacts, build_id="different"), conn=conn
            )


def test_build_production_network_with_pinned_extracted_continents(
    monkeypatch, tmp_path
):
    archive = tmp_path / "sword.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        for continent in ("na", "eu", "oc"):
            zipped.writestr(f"{continent}_sword_reaches_v17.gpkg", b"fixture")
    source_manifest = tmp_path / "sources.json"
    source_manifest.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [
                    {
                        "id": "sword",
                        "version": "17b",
                        "url": "https://example.test/sword.zip",
                        "filename": "sword.zip",
                        "checksum_algorithm": "md5",
                        "checksum": "0" * 32,
                        "attribution": "source",
                        "license": "terms",
                    }
                ],
            }
        )
    )
    pilots = tmp_path / "pilots.json"
    pilots.write_text(
        json.dumps(
            {
                "max_reaches_per_system": 2,
                "systems": [
                    {
                        "basin_key": "sacramento",
                        "continent": "na",
                        "river_name_aliases": ["Sacramento"],
                    },
                    {
                        "basin_key": "rhine",
                        "continent": "eu",
                        "river_name_aliases": ["Rhine"],
                    },
                    {
                        "basin_key": "murray",
                        "continent": "oc",
                        "river_name_aliases": ["Murray"],
                    },
                ],
            }
        )
    )
    monkeypatch.setattr(network, "acquire_sword_archive", lambda *_a, **_k: archive)

    class FakeFrame:
        def __init__(self, records):
            self.records = records

        def to_dict(self, orientation):
            assert orientation == "records"
            return self.records

    def read_file(path):
        name = Path(path).name
        river = (
            "Sacramento River"
            if name.startswith("na")
            else "Rhine"
            if name.startswith("eu")
            else "Murray River"
        )
        prefix = name[:2].upper()
        rows = _records(river)[:2]
        for row in rows:
            row["reach_id"] = prefix + str(row["reach_id"])
            row["rch_id_up"] = [prefix + str(value) for value in row["rch_id_up"]]
            row["rch_id_dn"] = [prefix + str(value) for value in row["rch_id_dn"]]
        return FakeFrame(rows)

    monkeypatch.setitem(sys.modules, "geopandas", SimpleNamespace(read_file=read_file))
    artifacts = build_production_network(
        output_dir=tmp_path / "output",
        source_cache=tmp_path / "cache",
        source_manifest_path=source_manifest,
        pilots_path=pilots,
        offline=True,
    )
    assert artifacts.artifact_mode == "production"
    assert artifacts.reach_count == 6
    assert set(artifacts.resolved_anchors) == {"sacramento", "rhine", "murray"}


def test_production_build_reads_reach_layer_from_continent_geopackage(
    monkeypatch, tmp_path
):
    archive = tmp_path / "sword.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("na_sword_v17b.gpkg", b"fixture")
    source_manifest = tmp_path / "sources.json"
    source_manifest.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [
                    {
                        "id": "sword",
                        "version": "17b",
                        "url": "https://example.test/sword.zip",
                        "filename": "sword.zip",
                        "checksum_algorithm": "md5",
                        "checksum": "0" * 32,
                        "attribution": "source",
                        "license": "terms",
                    }
                ],
            }
        )
    )
    pilots = tmp_path / "pilots.json"
    pilots.write_text(
        json.dumps(
            {
                "max_reaches_per_system": 1,
                "systems": [
                    {
                        "basin_key": "sacramento",
                        "continent": "na",
                        "river_name_aliases": ["Sacramento"],
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(network, "acquire_sword_archive", lambda *_a, **_k: archive)

    class FakeFrame:
        def to_dict(self, orientation):
            assert orientation == "records"
            return _records()[:1]

    layer_names = ["reaches", "nodes"]

    class FakeLayers:
        def __getitem__(self, name):
            assert name == "name"
            return SimpleNamespace(tolist=lambda: layer_names)

    read_options_seen = []

    def read_file(path, **options):
        assert Path(path).name == "na_sword_v17b.gpkg"
        read_options_seen.append(options)
        return FakeFrame()

    monkeypatch.setitem(
        sys.modules,
        "geopandas",
        SimpleNamespace(
            list_layers=lambda _path: FakeLayers(),
            read_file=read_file,
        ),
    )
    artifacts = build_production_network(
        output_dir=tmp_path / "output",
        source_cache=tmp_path / "cache",
        source_manifest_path=source_manifest,
        pilots_path=pilots,
        offline=True,
    )
    assert artifacts.resolved_anchors == {"sacramento": "A"}
    layer_names[:] = ["nodes"]
    build_production_network(
        output_dir=tmp_path / "output-without-reach-layer",
        source_cache=tmp_path / "cache-without-reach-layer",
        source_manifest_path=source_manifest,
        pilots_path=pilots,
        offline=True,
    )
    assert read_options_seen == [{"layer": "reaches"}, {}]


def test_production_build_rejects_ambiguous_geopackages_and_layers(
    monkeypatch, tmp_path
):
    source_manifest = tmp_path / "sources.json"
    source_manifest.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [
                    {
                        "id": "sword",
                        "version": "17b",
                        "url": "https://example.test/sword.zip",
                        "filename": "sword.zip",
                        "checksum_algorithm": "md5",
                        "checksum": "0" * 32,
                        "attribution": "source",
                        "license": "terms",
                    }
                ],
            }
        )
    )
    pilots = tmp_path / "pilots.json"
    pilots.write_text(
        json.dumps(
            {
                "max_reaches_per_system": 1,
                "systems": [
                    {
                        "basin_key": "sacramento",
                        "continent": "na",
                        "river_name_aliases": ["Sacramento"],
                    }
                ],
            }
        )
    )
    archive = tmp_path / "ambiguous-files.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("na_sword_reaches_a.gpkg", b"a")
        zipped.writestr("na_sword_reaches_b.gpkg", b"b")
    monkeypatch.setattr(network, "acquire_sword_archive", lambda *_a, **_k: archive)
    monkeypatch.setitem(sys.modules, "geopandas", SimpleNamespace())
    with pytest.raises(ValueError, match="Ambiguous SWORD GeoPackages"):
        build_production_network(
            output_dir=tmp_path / "ambiguous-output",
            source_cache=tmp_path / "ambiguous-cache",
            source_manifest_path=source_manifest,
            pilots_path=pilots,
            offline=True,
        )

    archive = tmp_path / "ambiguous-layers.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("na_sword_v17b.gpkg", b"fixture")

    class FakeLayers:
        def __getitem__(self, name):
            assert name == "name"
            return SimpleNamespace(tolist=lambda: ["reaches_a", "reaches_b"])

    monkeypatch.setitem(
        sys.modules,
        "geopandas",
        SimpleNamespace(list_layers=lambda _path: FakeLayers()),
    )
    with pytest.raises(ValueError, match="Ambiguous SWORD reach layers"):
        build_production_network(
            output_dir=tmp_path / "layer-output",
            source_cache=tmp_path / "layer-cache",
            source_manifest_path=source_manifest,
            pilots_path=pilots,
            offline=True,
        )


def test_network_value_and_geometry_normalization():
    class ArrayLike:
        def tolist(self):
            return ["A", 0, "nan"]

    assert network._neighbor_ids(None) == []
    assert network._neighbor_ids(ArrayLike()) == ["A"]
    assert network._neighbor_ids("[A, B 0]") == ["A", "B"]
    assert network._float("invalid") is None

    records = _records()
    records[0]["geometry_wkb"] = b"direct-wkb"
    records[0]["x"] = 10
    records[0]["y"] = 20
    reaches, _, _ = select_mainstem(
        records,
        basin_key="x",
        aliases=["Sacramento"],
        max_reaches=1,
    )
    assert reaches[0]["geometry_wkb"] == b"direct-wkb"
    assert reaches[0]["centroid_longitude"] == 10

    no_geometry = _records()
    no_geometry[0].pop("geometry")
    with pytest.raises(ValueError, match="no parseable geometry"):
        select_mainstem(
            no_geometry,
            basin_key="x",
            aliases=["Sacramento"],
            max_reaches=1,
        )
    no_centroid = _records()
    no_centroid[0]["geometry_wkb"] = b"wkb"
    no_centroid[0].pop("geometry")
    with pytest.raises(ValueError, match="no centroid"):
        select_mainstem(
            no_centroid,
            basin_key="x",
            aliases=["Sacramento"],
            max_reaches=1,
        )


def test_network_table_validation_all_rejection_paths():
    reaches, edges, _ = synthetic_network_rows()
    reach_table = pa.Table.from_pylist(reaches).select(network.REACH_COLUMNS)
    edge_table = pa.Table.from_pylist(edges).select(network.EDGE_COLUMNS)

    with pytest.raises(ValueError, match="reaches artifact schema"):
        network._validate_network_tables(
            reach_table.drop(["river_name"]),
            edge_table,
        )
    with pytest.raises(ValueError, match="edges artifact schema"):
        network._validate_network_tables(
            reach_table,
            edge_table.drop(["network_version"]),
        )
    with pytest.raises(ValueError, match="duplicate edges"):
        network._validate_network_tables(
            reach_table,
            pa.Table.from_pylist(edges + [edges[0]]).select(network.EDGE_COLUMNS),
        )
    outside_start = edges + [
        {
            "network_version": "17b",
            "from_reach_id": "OUTSIDE",
            "to_reach_id": "BOUNDARY",
            "is_selection_boundary": True,
        }
    ]
    with pytest.raises(ValueError, match="starts outside"):
        network._validate_network_tables(
            reach_table,
            pa.Table.from_pylist(outside_start).select(network.EDGE_COLUMNS),
        )
    outside_internal = edges + [
        {
            "network_version": "17b",
            "from_reach_id": reaches[0]["reach_id"],
            "to_reach_id": "OUTSIDE",
            "is_selection_boundary": False,
        }
    ]
    with pytest.raises(ValueError, match="internal edge points outside"):
        network._validate_network_tables(
            reach_table,
            pa.Table.from_pylist(outside_internal).select(network.EDGE_COLUMNS),
        )
    oversized = [
        {**reaches[0], "reach_id": f"R{index}", "basin_key": "one"}
        for index in range(101)
    ]
    with pytest.raises(ValueError, match="exceeds 100"):
        network._validate_network_tables(
            pa.Table.from_pylist(oversized).select(network.REACH_COLUMNS),
            edge_table.slice(0, 0),
        )


def test_generation_reuse_rejects_corruption(tmp_path):
    artifacts = _publish(tmp_path)
    artifacts.reaches_path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="generation is corrupt"):
        _publish(tmp_path)


def test_network_manifest_validation_rejection_paths(tmp_path):
    artifacts = _publish(tmp_path)
    original = json.loads(artifacts.manifest_path.read_text())
    root = artifacts.manifest_path.parent

    def rejected(name, mutate, message):
        payload = json.loads(json.dumps(original))
        mutate(payload)
        candidate = root / f"{name}.json"
        candidate.write_text(json.dumps(payload))
        with pytest.raises((ValueError, FileNotFoundError), match=message):
            load_network_artifacts(candidate, allow_synthetic=True)

    rejected("incomplete", lambda value: value.pop("edges"), "incomplete")
    rejected(
        "manifest-version",
        lambda value: value.__setitem__("manifest_version", "2"),
        "manifest version",
    )
    rejected(
        "mode",
        lambda value: value.__setitem__("artifact_mode", "unknown"),
        "artifact mode",
    )
    rejected(
        "absolute",
        lambda value: value["reaches"].__setitem__("path", str(artifacts.reaches_path)),
        "root-relative",
    )
    rejected(
        "missing-file",
        lambda value: value["reaches"].__setitem__("path", "missing.parquet"),
        "not found",
    )
    rejected(
        "row-count",
        lambda value: value["reaches"].__setitem__("row_count", 10),
        "row count",
    )

    no_metadata = root / "no-metadata.parquet"
    pq.write_table(
        pq.read_table(artifacts.reaches_path).replace_schema_metadata({}),
        no_metadata,
    )

    def use_no_metadata(value):
        value["reaches"]["path"] = no_metadata.name
        value["reaches"]["sha256"] = network.sha256_file(no_metadata)

    rejected("metadata", use_no_metadata, "metadata mismatch")


def test_persist_network_rolls_back_failed_manifest_write(duck, tmp_path):
    class FailingConnection:
        def __init__(self, inner):
            self.inner = inner
            self.rolled_back = False

        def execute(self, sql, parameters=None):
            if "network_artifact_manifest" in sql and "INSERT INTO" in sql:
                raise RuntimeError("injected manifest failure")
            if sql.strip() == "ROLLBACK":
                self.rolled_back = True
            return (
                self.inner.execute(sql)
                if parameters is None
                else self.inner.execute(sql, parameters)
            )

        def register(self, *args):
            return self.inner.register(*args)

        def unregister(self, *args):
            return self.inner.unregister(*args)

    artifacts = _publish(tmp_path)
    with duck.get_connection() as conn:
        failing = FailingConnection(conn)
        with pytest.raises(RuntimeError, match="injected manifest failure"):
            persist_network_artifacts(artifacts, conn=failing)
        assert failing.rolled_back
        assert (
            conn.execute(
                "select count(*) from riverpulse_events_raw.reaches"
            ).fetchone()[0]
            == 0
        )


def test_production_build_requires_continent_geopackage(monkeypatch, tmp_path):
    archive = tmp_path / "sword.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("na_sword_reaches_v17.gpkg", b"fixture")
    source_manifest = tmp_path / "sources.json"
    source_manifest.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "network_version": "17b",
                "sources": [
                    {
                        "id": "sword",
                        "version": "17b",
                        "url": "https://example.test/sword.zip",
                        "filename": "sword.zip",
                        "checksum_algorithm": "md5",
                        "checksum": "0" * 32,
                        "attribution": "source",
                        "license": "terms",
                    }
                ],
            }
        )
    )
    pilots = tmp_path / "pilots.json"
    pilots.write_text(
        json.dumps(
            {
                "max_reaches_per_system": 1,
                "systems": [
                    {
                        "basin_key": "rhine",
                        "continent": "eu",
                        "river_name_aliases": ["Rhine"],
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(network, "acquire_sword_archive", lambda *_a, **_k: archive)
    monkeypatch.setitem(sys.modules, "geopandas", SimpleNamespace())
    with pytest.raises(FileNotFoundError, match="continent eu"):
        build_production_network(
            output_dir=tmp_path / "output",
            source_cache=tmp_path / "cache",
            source_manifest_path=source_manifest,
            pilots_path=pilots,
            offline=True,
        )
