import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"

EXPECTED_TOP_NAV = [
    "Home",
    "Audiences",
    "Get started",
    "Guides",
    "Reference",
    "Concepts",
    "Development",
]

STALE_PHRASES = (
    "Version 0.3 requires a new derived warehouse",
    "Follow the v0.3 upgrade guide",
    "standard scope's marts and observability tables are created but remain empty",
    "std marts are built by make demo",
    "Do not add a docs workflow under",
    "Docs publish locally by design",
)

SCHEDULE_ENV_VARS = (
    "PLUMEGRAPH_EVENTS_PIPELINE_SCHEDULE_ENABLED",
    "RIVERPULSE_EVENTS_PIPELINE_SCHEDULE_ENABLED",
    "TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED",
    "TEMPO_NO2_STD_PIPELINE_SCHEDULE_ENABLED",
)


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
    text = (REPO_ROOT / "mkdocs.yml").read_text()
    text = re.sub(r"!!python/name:([^\s]+)", r"\1", text)
    return yaml.safe_load(text)


def _combined_docs() -> str:
    return "\n".join(path.read_text() for path in DOCS_DIR.rglob("*.md"))


def test_navigation_contains_every_docs_page():
    config = _config()
    assert [next(iter(item)) for item in config["nav"]] == EXPECTED_TOP_NAV
    targets = set(_nav_targets(config["nav"]))
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
    assert config["site_url"] == "https://hypertrial.github.io/titanskies-pipeline/"
    assert config["repo_url"] == "https://github.com/hypertrial/titanskies-pipeline"
    assert config["repo_name"] == "hypertrial/titanskies-pipeline"
    assert config["theme"]["name"] == "material"
    assert config["theme"]["custom_dir"] == "overrides"
    assert config["theme"]["font"] is False
    features = set(config["theme"]["features"])
    for required in (
        "navigation.tabs",
        "navigation.sections",
        "navigation.indexes",
        "content.code.copy",
        "search.suggest",
    ):
        assert required in features
    assert "search" in config["plugins"]
    assert "assets/stylesheets/extra.css" in config["extra_css"]

    source_override = (REPO_ROOT / "overrides/partials/source.html").read_text()
    assert 'class="md-source"' in source_override
    assert 'data-md-component="source"' not in source_override


def test_readme_links_to_canonical_guides():
    readme = (REPO_ROOT / "README.md").read_text()
    required = [
        "uv run make docs-serve",
        "http://127.0.0.1:8000",
        "(docs/audiences/analysts.md)",
        "(docs/audiences/operators.md)",
        "(docs/audiences/integrators.md)",
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


def test_security_supported_versions_include_project_line():
    pyproject = (REPO_ROOT / "pyproject.toml").read_text()
    match = re.search(r'^version\s*=\s*"(\d+)\.(\d+)\.\d+"', pyproject, flags=re.M)
    assert match is not None
    supported_line = f"{match.group(1)}.{match.group(2)}.x"
    security = (REPO_ROOT / "SECURITY.md").read_text()
    assert f"| {supported_line}" in security
    assert re.search(
        rf"\|\s*{re.escape(supported_line)}\s*\|\s*Yes\s*\|",
        security,
    )


def test_public_models_and_registered_jobs_are_documented():
    combined = _combined_docs()
    model_families = (
        "tempo_no2",
        "tempo_no2_std",
        "riverpulse_events",
        "plumegraph_events",
    )
    marts = {
        path.stem
        for family in model_families
        for path in (REPO_ROOT / f"dbt/models/{family}/marts").glob("*.sql")
    }
    observability = {
        path.stem
        for family in model_families
        for path in (REPO_ROOT / f"dbt/models/{family}/observability").glob("*.sql")
    }
    scope_registry = (
        REPO_ROOT / "src/titanskies_pipeline/orchestration/scope_registry.py"
    ).read_text()
    jobs = set(
        re.findall(r'(?:discovery|ingest|dbt|full)_job_name="([^"]+)"', scope_registry)
    )
    jobs |= {
        "riverpulse_events_source_discovery",
        "riverpulse_events_observation_ingest",
        "riverpulse_events_dbt_build",
        "riverpulse_events_full_pipeline",
        "plumegraph_events_source_discovery",
        "plumegraph_events_source_ingest",
        "plumegraph_events_analysis",
        "plumegraph_events_dbt_build",
        "plumegraph_events_validation",
        "plumegraph_events_release_build",
        "plumegraph_events_full_pipeline",
    }

    assert len(marts) == 25
    assert len(observability) == 13
    assert len(jobs) == 19
    for name in marts | observability | jobs:
        assert name in combined, name
    for variable in SCHEDULE_ENV_VARS:
        assert variable in combined, variable


def test_scripts_inventory_is_documented():
    scripts_doc = (DOCS_DIR / "reference/scripts.md").read_text()
    public_scripts = sorted(
        path.name
        for path in (REPO_ROOT / "scripts").glob("*.py")
        if not path.name.startswith("_")
    )
    assert public_scripts
    for name in public_scripts:
        assert name in scripts_doc, name


def test_stale_phrase_denylist():
    paths = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "CONTRIBUTING.md",
        *DOCS_DIR.rglob("*.md"),
    ]
    for path in paths:
        text = path.read_text()
        for phrase in STALE_PHRASES:
            assert phrase not in text, f"{path.relative_to(REPO_ROOT)}: {phrase}"


def test_built_homepage_is_semantic():
    index = REPO_ROOT / "site/index.html"
    if not index.exists():
        pytest.skip("Run make docs-build before checking generated HTML.")

    html = index.read_text()
    assert re.search(r"<h1[^>]*>TitanSkies Pipeline", html)
    assert 'href="https://github.com/hypertrial/titanskies-pipeline"' in html
    assert "hypertrial/titanskies-pipeline" in html
    assert "ts-task-grid" in html
