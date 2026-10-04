"""Smoke tests for the package itself."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

import adcp

pytestmark = [pytest.mark.unit, pytest.mark.smoke]


def test_package_exposes_a_semantic_version() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+([.\-+].*)?", adcp.__version__)


def test_package_ships_the_pep561_marker() -> None:
    package_dir = Path(adcp.__file__).parent
    assert (package_dir / "py.typed").is_file()


def test_python_dash_m_entry_point_runs() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "adcp", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("adcp ")
