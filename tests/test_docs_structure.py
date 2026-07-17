import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
pytestmark = pytest.mark.repo_check


def _nav_targets(items):
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for value in item.values():
                if isinstance(value, str):
                    yield value
                else:
                    yield from _nav_targets(value)


def _config():
    return yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())


def test_navigation_contains_every_docs_page():
    targets = set(_nav_targets(_config()["nav"]))
    pages = {path.relative_to(DOCS_DIR).as_posix() for path in DOCS_DIR.rglob("*.md")}

    assert targets == pages
    for target in targets:
        assert (DOCS_DIR / target).is_file(), target


def test_every_page_starts_with_a_visible_h1():
    for path in DOCS_DIR.rglob("*.md"):
        text = path.read_text()
        assert re.search(r"^# [^#]", text, re.MULTILINE), path.relative_to(DOCS_DIR)


def test_mkdocs_is_self_contained_and_links_to_repository():
    config = _config()

    assert config["site_name"] == "TitanSkies Pipeline"
    assert config["repo_url"] == "https://github.com/hypertrial/titanskies-pipeline"
    assert config["repo_name"] == "hypertrial/titanskies-pipeline"
    assert config["theme"]["name"] == "material"
    assert config["theme"]["custom_dir"] == "overrides"
    assert config["theme"]["font"] is False
    assert "site_url" not in config

    source_override = (REPO_ROOT / "overrides/partials/source.html").read_text()
    assert 'class="md-source"' in source_override
    assert 'data-md-component="source"' not in source_override


def test_readme_links_to_canonical_guides():
    readme = (REPO_ROOT / "README.md").read_text()
    required = [
        "uv run make docs-serve",
        "http://127.0.0.1:8000",
        "(docs/guides/query-the-warehouse.md)",
        "(docs/guides/troubleshooting.md)",
        "(docs/reference/warehouse.md)",
        "(docs/reference/data-dictionary.md)",
        "(docs/concepts/architecture.md)",
        "(docs/development/index.md)",
        "(CONTRIBUTING.md)",
        "(PRIVACY.md)",
        "(SECURITY.md)",
        "(THIRD_PARTY_NOTICES.md)",
    ]

    for term in required:
        assert term in readme


def test_environment_inventory_is_documented():
    example = (REPO_ROOT / ".env.example").read_text()
    documented = (DOCS_DIR / "reference/configuration.md").read_text()
    variables = set(
        re.findall(r"^#?\s*([A-Z][A-Z0-9_]+)=", example, flags=re.MULTILINE)
    )

    assert variables
    for variable in variables:
        assert f"`{variable}`" in documented, variable


def test_public_models_and_registered_jobs_are_documented():
    combined = "\n".join(path.read_text() for path in DOCS_DIR.rglob("*.md"))
    marts = {
        path.stem for path in (REPO_ROOT / "dbt/models/tempo_no2/marts").glob("*.sql")
    }
    observability = {
        path.stem
        for path in (REPO_ROOT / "dbt/models/tempo_no2/observability").glob("*.sql")
    }
    scope_registry = (
        REPO_ROOT / "src/titanskies_pipeline/orchestration/scope_registry.py"
    ).read_text()
    jobs = set(
        re.findall(r'(?:discovery|ingest|dbt|full)_job_name="([^"]+)"', scope_registry)
    )

    assert len(marts) == 6
    assert len(observability) == 2
    assert len(jobs) == 4
    for name in marts | observability | jobs:
        assert name in combined, name


def test_built_homepage_is_semantic():
    index = REPO_ROOT / "site/index.html"
    if not index.exists():
        pytest.skip("Run make docs-build before checking generated HTML.")

    html = index.read_text()
    assert re.search(r'<h1[^>]+id="titanskies-pipeline"[^>]*>TitanSkies Pipeline', html)
    assert 'href="https://github.com/hypertrial/titanskies-pipeline"' in html
    assert "hypertrial/titanskies-pipeline" in html
