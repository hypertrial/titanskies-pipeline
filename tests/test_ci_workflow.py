from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_ci_workflow_is_one_bounded_offline_runner():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    workflow_text = workflow_path.read_text()

    assert set(workflow["jobs"]) == {"fast-gate"}
    assert workflow["jobs"]["fast-gate"]["timeout-minutes"] == 5
    assert (
        "uv run make lint test contract-http dbt-parse docs-build docs-structure"
        in workflow_text
    )
    assert "live-smoke" not in workflow_text
    assert "docs-render" not in workflow_text
    assert "EARTHDATA_USERNAME" not in workflow_text
    assert not (workflow_path.parent / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_path.parent.glob("*.yml")) == [
        "ci.yml",
        "docs.yml",
    ]


def test_docs_workflow_publishes_on_main_tags_and_dispatch():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "docs.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    workflow_text = workflow_path.read_text()
    trigger = workflow.get("on", workflow.get(True))

    assert trigger["push"]["branches"] == ["main"]
    assert trigger["push"]["tags"] == ["v*"]
    assert "workflow_dispatch" in trigger
    assert "pull_request" not in trigger
    assert workflow["permissions"]["contents"] == "write"
    assert set(workflow["jobs"]) == {"publish"}
    assert workflow["jobs"]["publish"]["timeout-minutes"] == 5
    assert "mkdocs gh-deploy" in workflow_text
    assert "live-smoke" not in workflow_text
