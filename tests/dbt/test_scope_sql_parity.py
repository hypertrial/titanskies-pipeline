"""Guard NRT and standard dbt SQL trees against logical drift."""

from __future__ import annotations

from pathlib import Path

import pytest

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"
NRT_ROOT = DBT_ROOT / "models" / "tempo_no2"
STD_ROOT = DBT_ROOT / "models" / "tempo_no2_std"

# Relative NRT path -> relative STD path when filenames are not a pure rename.
REGISTRY_PAIR = (
    Path("marts/tempo_region_registry.sql"),
    Path("marts/tempo_no2_std_region_registry.sql"),
)


def _std_relative(nrt_relative: Path) -> Path:
    if nrt_relative == REGISTRY_PAIR[0]:
        return REGISTRY_PAIR[1]
    # Single substitution avoids cascading tempo_no2_ -> tempo_no2_std_ -> *_std_std_*.
    return Path(str(nrt_relative).replace("tempo_no2", "tempo_no2_std"))


def _normalize(sql: str, *, std: bool) -> str:
    if std:
        normalized = sql.replace("tempo_no2_std_", "SCOPE_")
        normalized = normalized.replace("tempo_no2_std", "SCOPE")
    else:
        normalized = sql.replace("tempo_no2_", "SCOPE_")
        normalized = normalized.replace("tempo_no2", "SCOPE")
        normalized = normalized.replace(
            "ref('tempo_region_registry')", "ref('SCOPE_region_registry')"
        )
        normalized = normalized.replace(
            'ref("tempo_region_registry")', 'ref("SCOPE_region_registry")'
        )
    return normalized


def _paired_paths() -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    for nrt_path in sorted(NRT_ROOT.rglob("*.sql")):
        relative = nrt_path.relative_to(NRT_ROOT)
        std_path = STD_ROOT / _std_relative(relative)
        pairs.append((nrt_path, std_path))
    return pairs


@pytest.mark.parametrize(
    ("nrt_path", "std_path"),
    _paired_paths(),
    ids=[str(nrt.relative_to(NRT_ROOT)) for nrt, _std in _paired_paths()],
)
def test_nrt_and_std_sql_are_logically_identical(nrt_path: Path, std_path: Path):
    assert std_path.is_file(), f"missing std twin for {nrt_path}"
    nrt_norm = _normalize(nrt_path.read_text(), std=False)
    std_norm = _normalize(std_path.read_text(), std=True)
    assert nrt_norm == std_norm, (
        f"logical drift between {nrt_path.relative_to(DBT_ROOT)} and "
        f"{std_path.relative_to(DBT_ROOT)}"
    )


def test_every_std_sql_has_nrt_pair():
    expected = {_std_relative(p.relative_to(NRT_ROOT)) for p in NRT_ROOT.rglob("*.sql")}
    actual = {p.relative_to(STD_ROOT) for p in STD_ROOT.rglob("*.sql")}
    assert actual == expected
