import logging

import pytest

pytest.importorskip("dagster_dbt")


def test_prepare_dbt_project_warns_when_prepare_fails_but_manifest_exists(
    tmp_path, caplog
):
    from titanskies_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    caplog.set_level(logging.WARNING)
    dbt_project_mod.prepare_dbt_project(FakeProject(), preparer=FakeProject.preparer)
    assert any("prepare_if_dev() failed" in r.getMessage() for r in caplog.records)


def test_prepare_dbt_project_reraises_when_manifest_missing(tmp_path):
    from titanskies_pipeline.orchestration import dbt_project as dbt_project_mod

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = tmp_path / "missing.json"
        preparer = FakePreparer()

    with pytest.raises(RuntimeError, match="prepare failed"):
        dbt_project_mod.prepare_dbt_project(
            FakeProject(), preparer=FakeProject.preparer
        )


def test_prepare_dbt_project_prepares_outside_dev_when_missing(tmp_path):
    from titanskies_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    prepared: list[str] = []

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, project):
            prepared.append(str(project.manifest_path))
            manifest.write_text("{}")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    dbt_project_mod.prepare_dbt_project(FakeProject(), preparer=FakeProject.preparer)
    assert prepared == [str(manifest)]


def test_prepare_dbt_project_skips_when_manifest_exists_outside_dev(tmp_path):
    from titanskies_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, _project):
            raise AssertionError("should not prepare")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    dbt_project_mod.prepare_dbt_project(FakeProject(), preparer=FakeProject.preparer)


def test_titanskies_dbt_preparer_cli_and_manifest(monkeypatch, tmp_path):
    from titanskies_pipeline.orchestration.dbt_project import (
        TitanSkiesDbtProjectPreparer,
    )

    calls: list[list[str]] = []

    class FakeCli:
        def cli(self, args, **kwargs):
            calls.append(list(args))

            class Invocation:
                def wait(self):
                    return None

            return Invocation()

    preparer = TitanSkiesDbtProjectPreparer()
    monkeypatch.setattr(preparer, "_dbt_cli", lambda _project: FakeCli())
    monkeypatch.setattr(
        preparer, "_invalidate_seeds_in_partial_parse", lambda _project: None
    )
    project = type(
        "P",
        (),
        {"target_path": str(tmp_path), "project_dir": tmp_path},
    )()

    preparer._prepare_packages(project)
    preparer._prepare_manifest(project)
    assert calls[0] == ["deps", "--quiet"]
    assert calls[1][0] == "parse"


def test_titanskies_dbt_preparer_dbt_cli_resource(monkeypatch, tmp_path):
    from titanskies_pipeline.orchestration.dbt_project import (
        TitanSkiesDbtProjectPreparer,
    )

    captured: dict[str, str] = {}

    class FakeDbtCliResource:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(
        "dagster_dbt.core.resource.DbtCliResource",
        FakeDbtCliResource,
    )
    preparer = TitanSkiesDbtProjectPreparer()
    project = type("P", (), {"target_path": str(tmp_path)})()
    preparer._dbt_cli(project)
    assert captured["profile"] == "titanskies"
    assert captured["target"] == "dev"
