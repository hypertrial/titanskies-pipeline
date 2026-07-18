import json
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "Makefile",
    "PRIVACY.md",
    "THIRD_PARTY_NOTICES.md",
    "pyproject.toml",
    ".github/workflows/ci.yml",
    ".github/dependabot.yml",
    ".github/CODEOWNERS",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/ISSUE_TEMPLATE/bug_report.yml",
    ".github/ISSUE_TEMPLATE/feature_request.yml",
    ".github/ISSUE_TEMPLATE/documentation.yml",
    "dbt/dbt_project.yml",
    "docs/index.md",
    "mkdocs.yml",
    "tests/fixtures/SYNTHETIC.md",
]


def test_required_repo_files_exist():
    for relative in REQUIRED:
        assert (ROOT / relative).is_file(), relative


def test_dependabot_policy_is_constrained():
    config = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    updates = {item["package-ecosystem"]: item for item in config["updates"]}

    assert config["version"] == 2
    assert set(updates) == {"uv", "github-actions"}
    for update in updates.values():
        assert update["directory"] == "/"
        assert update["schedule"]["interval"] == "weekly"
        assert update["open-pull-requests-limit"] == 3
        assert len(update["groups"]) == 1
        group = next(iter(update["groups"].values()))
        assert set(group["update-types"]) == {"minor", "patch"}

    assert updates["uv"]["ignore"] == [
        {"dependency-name": "dbt-core", "versions": [">=1.12"]},
        {"dependency-name": "netcdf4", "versions": ["1.7.4"]},
    ]
    assert "ignore" not in updates["github-actions"]


def test_github_actions_are_pinned_to_full_commits():
    workflows = "\n".join(
        path.read_text() for path in (ROOT / ".github/workflows").glob("*.yml")
    )
    refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", workflows)
    assert refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in refs)
    assert "uv sync --locked" in workflows


def test_dbt_telemetry_is_disabled():
    dbt_project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text())
    dagster_instance = yaml.safe_load((ROOT / "dagster_instance.yaml").read_text())

    assert dbt_project["flags"]["send_anonymous_usage_stats"] is False
    assert dagster_instance["telemetry"]["enabled"] is False


def test_repository_ownership_and_templates_are_actionable():
    assert (ROOT / ".github/CODEOWNERS").read_text().strip() == "* @mattfaltyn"

    pull_request = (ROOT / ".github/PULL_REQUEST_TEMPLATE.md").read_text()
    for heading in ("## Summary", "## Test plan", "## Documentation impact"):
        assert heading in pull_request
    assert "No secrets" in pull_request

    for name in ("bug_report.yml", "feature_request.yml", "documentation.yml"):
        form = yaml.safe_load((ROOT / ".github/ISSUE_TEMPLATE" / name).read_text())
        assert form["name"]
        assert form["description"]
        ids = [item["id"] for item in form["body"] if "id" in item]
        assert ids
        assert len(ids) == len(set(ids))
        assert any(item.get("validations", {}).get("required") for item in form["body"])
        guidance = " ".join(
            item.get("attributes", {}).get("value", "") for item in form["body"]
        )
        assert "Do not" in guidance
        assert "DuckDB" in guidance


def test_generated_and_live_artifacts_are_not_tracked():
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    violations = []
    synthetic_binary_fixtures = {
        "tests/fixtures/geo/tempo_grid_region_weights.parquet",
        "tests/fixtures/geo/tempo_region_registry.parquet",
        "tests/fixtures/netcdf/tempo_no2_sample.nc",
    }
    for relative in tracked:
        path = Path(relative)
        if relative in synthetic_binary_fixtures:
            continue
        name = path.name.lower()
        if (
            path.parts[:1] == ("site",)
            or path.parts[:2] in {("dbt", "target"), ("dbt", "logs"), ("data", "raw")}
            or path.parts[:2] == ("artifacts", "geo")
            or ".duckdb" in name
            or name.startswith(".coverage")
            or path.suffix
            in {
                ".7z",
                ".db",
                ".dbf",
                ".gz",
                ".h5",
                ".hdf5",
                ".key",
                ".nc",
                ".nc4",
                ".netcdf",
                ".p12",
                ".parquet",
                ".pem",
                ".pfx",
                ".shp",
                ".shx",
                ".sqlite",
                ".sqlite3",
                ".tar",
                ".tgz",
                ".wal",
                ".zip",
            }
            or any(part.endswith(".zarr") for part in path.parts)
        ):
            violations.append(relative)

    assert not violations, violations


def test_source_matrix_matches_geography_manifest():
    manifest = json.loads((ROOT / "config/geography_sources.json").read_text())
    notices = (ROOT / "THIRD_PARTY_NOTICES.md").read_text()

    assert manifest["sources"]
    for source in manifest["sources"]:
        for field in ("id", "url", "sha256", "attribution", "license"):
            assert source[field] in notices, f"{source['id']}.{field}"


def test_tracked_data_files_are_inventory_scoped_and_synthetic():
    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    data_suffixes = {".csv", ".json", ".nc", ".parquet"}
    expected = {
        "config/geography_sources.json",
        "dbt/seeds/tempo_no2_contract.csv",
        "dbt/seeds/tempo_no2_std_contract.csv",
        "tests/fixtures/cassettes/tempo_cmr_granules.json",
        "tests/fixtures/geo/tempo_grid_region_weights.parquet",
        "tests/fixtures/geo/tempo_region_registry.parquet",
        "tests/fixtures/netcdf/tempo_no2_sample.nc",
    }
    actual = {
        relative
        for relative in tracked
        if (ROOT / relative).is_file()
        and Path(relative).suffix.lower() in data_suffixes
    }
    assert actual == expected
    assert (
        "Generate synthetic" in (ROOT / "scripts/generate_geo_fixtures.py").read_text()
    )
    assert (
        "Generate synthetic"
        in (ROOT / "scripts/generate_netcdf_fixtures.py").read_text()
    )
