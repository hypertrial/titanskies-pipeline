#!/usr/bin/env python3
"""Smoke-check SQL fences in docs/guides/query-recipes.md against demo DuckDB."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_DB = REPO_ROOT / ".cache" / "demo.duckdb"
RECIPES = REPO_ROOT / "docs" / "guides" / "query-recipes.md"
SQL_FENCE = re.compile(r"```sql\n(.*?)```", re.DOTALL | re.IGNORECASE)
COPY_TO = re.compile(
    r"""(?i)\bto\s+('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")""",
)


def _ensure_demo() -> None:
    if DEMO_DB.is_file():
        return
    DEMO_DB.parent.mkdir(parents=True, exist_ok=True)
    print(f"demo warehouse missing; running make demo -> {DEMO_DB}")
    subprocess.run(["make", "demo"], cwd=REPO_ROOT, check=True)
    if not DEMO_DB.is_file():
        raise SystemExit(f"make demo did not create {DEMO_DB}")


def _sql_blocks(text: str) -> list[str]:
    return [match.group(1).strip() for match in SQL_FENCE.finditer(text)]


def _rewrite_copy_targets(sql: str, out_dir: Path) -> str:
    counter = 0

    def _replace(match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        quoted = match.group(1)
        original = quoted[1:-1]
        suffix = Path(original).suffix or ".out"
        target = out_dir / f"recipe_copy_{counter}{suffix}"
        return f"to '{target}'"

    return COPY_TO.sub(_replace, sql)


def _is_skippable_std_catalog_error(message: str) -> bool:
    lowered = message.lower()
    if "tempo_no2_std_" not in lowered:
        return False
    return "does not exist" in lowered or "catalog error" in lowered


def main() -> int:
    _ensure_demo()
    blocks = _sql_blocks(RECIPES.read_text(encoding="utf-8"))
    if not blocks:
        print(
            f"no sql fences found in {RECIPES.relative_to(REPO_ROOT)}", file=sys.stderr
        )
        return 1

    checked = 0
    skipped = 0
    with tempfile.TemporaryDirectory(prefix="titanskies-docs-recipes-") as tmp:
        out_dir = Path(tmp)
        conn = duckdb.connect(str(DEMO_DB), read_only=True)
        try:
            for index, block in enumerate(blocks, start=1):
                sql = _rewrite_copy_targets(block, out_dir)
                try:
                    conn.execute(sql)
                except Exception as exc:  # noqa: BLE001 - surface DuckDB errors
                    message = str(exc)
                    if _is_skippable_std_catalog_error(message):
                        skipped += 1
                        print(
                            f"skip recipe #{index}: standard-scope relation missing "
                            f"in demo warehouse ({message.splitlines()[0]})"
                        )
                        continue
                    print(f"recipe #{index} failed:\n{sql}\n{exc}", file=sys.stderr)
                    return 1
                checked += 1
        finally:
            conn.close()

    print(
        f"docs recipe SQL smoke: checked={checked} skipped_std_missing={skipped} "
        f"total_blocks={len(blocks)} warehouse={DEMO_DB}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
