import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

_INTEGRATION_ROOTS = (
    ROOT / "tests" / "integration",
    ROOT / "tests" / "dbt",
)


def pytest_collection_modifyitems(config, items):
    for item in items:
        try:
            rp = Path(item.path).resolve()
        except (OSError, TypeError, ValueError, AttributeError):
            continue
        for base in _INTEGRATION_ROOTS:
            try:
                rp.relative_to(base)
            except ValueError:
                continue
            item.add_marker(pytest.mark.integration)
            break
