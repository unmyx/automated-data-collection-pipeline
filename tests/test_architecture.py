"""Layering rules from PLAN section 3.2, enforced as tests.

The API integration must stay independent of the database layer, and the domain
models must stay independent of every framework, so these checks read the source
rather than the runtime (an earlier import in the same process would hide a
violation).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SRC = Path(__file__).resolve().parents[1] / "src" / "adcp"


def _violations(relative: Path, forbidden: tuple[str, ...]) -> dict[str, list[str]]:
    """Modules under ``relative`` that import any of ``forbidden``."""
    found: dict[str, list[str]] = {}
    paths = [relative] if relative.is_file() else sorted(relative.rglob("*.py"))
    for path in paths:
        text = path.read_text(encoding="utf-8")
        hits = [
            name
            for name in forbidden
            if re.search(
                rf"^\s*(from|import)\s+{re.escape(name)}\b",
                text,
                flags=re.MULTILINE,
            )
        ]
        if hits:
            found[str(path.relative_to(SRC))] = hits
    return found


@pytest.mark.parametrize(
    ("relative", "forbidden"),
    [
        (Path("api"), ("adcp.db", "sqlalchemy", "alembic", "typer")),
        (Path("models"), ("adcp.db", "adcp.api", "httpx", "sqlalchemy", "typer")),
        (Path("ports.py"), ("adcp.api", "adcp.db", "httpx", "sqlalchemy")),
        (Path("resilience.py"), ("adcp.api", "adcp.db", "sqlalchemy")),
        (Path("config.py"), ("adcp.api", "adcp.db", "httpx", "sqlalchemy")),
        (Path("normalization.py"), ("adcp.api", "adcp.db", "httpx", "sqlalchemy")),
        (Path("validation"), ("adcp.api", "adcp.db", "httpx", "sqlalchemy")),
        # The scheduler only calls the injected runner; it must not reach for the
        # adapter, the database, or the pipeline service itself.
        (Path("scheduler.py"), ("adcp.api", "adcp.db", "adcp.pipeline", "httpx", "sqlalchemy")),
    ],
)
def test_layer_does_not_import_forbidden_modules(
    relative: Path,
    forbidden: tuple[str, ...],
) -> None:
    assert _violations(SRC / relative, forbidden) == {}


def test_http_is_confined_to_the_adapter_and_its_policy() -> None:
    """The rest of the application can be tested without a network."""
    allowed = {"open_meteo.py", "resilience.py"}
    offenders: dict[str, list[str]] = {}
    for path in sorted(SRC.rglob("*.py")):
        if path.name in allowed:
            continue
        if re.search(r"^\s*(from|import)\s+httpx\b", path.read_text(encoding="utf-8"), re.M):
            offenders[str(path.relative_to(SRC))] = ["httpx"]

    assert offenders == {}
