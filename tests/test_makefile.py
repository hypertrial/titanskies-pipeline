"""Makefile recipe sanity checks."""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_makefile_declares_quality_gate_targets():
    makefile = (REPO_ROOT / "Makefile").read_text()
    for target in (
        "lint:",
        "test-cov:",
        "dbt-build-ci:",
        "dbt-unit:",
        "golden-dbt:",
        "gx-data-quality:",
        "costguard:",
        "docs-check:",
        "coverage-report:",
        "live-smoke:",
    ):
        assert target in makefile


def test_make_dbt_parse_dry_run():
    proc = subprocess.run(
        ["make", "-n", "dbt-parse"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "dbt.cli.main parse" in proc.stdout


def test_costguard_local_gate_matches_ci_policy():
    makefile = (REPO_ROOT / "Makefile").read_text()
    assert "--warehouse duckdb" in makefile
    assert "--manifest target/manifest.json" in makefile
    assert "--fail-on high --min-confidence high" in makefile
