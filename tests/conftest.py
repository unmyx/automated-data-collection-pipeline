"""Shared test fixtures.

Every test gets an environment purged of ``ADCP_*`` variables and a cleared
``Settings`` cache, so results never depend on the developer's shell or a local
``.env`` file.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from adcp.config import Settings, get_settings

#: Environment variables that are not prefixed but still feed ``Settings``.
UNPREFIXED_VARIABLES = ("DATABASE_URL",)


def _adcp_variables() -> list[str]:
    return [name for name in os.environ if name.upper().startswith("ADCP_")]


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Remove configuration variables for the duration of each test."""
    for name in [*UNPREFIXED_VARIABLES, *_adcp_variables()]:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def make_settings() -> Callable[..., Settings]:
    """Build ``Settings`` objects that ignore any developer ``.env`` file."""

    def _make(**overrides: Any) -> Settings:
        return Settings(_env_file=None, **overrides)

    return _make
