"""Migration graph and Core metadata - verified without a database."""

from __future__ import annotations

import re
from itertools import pairwise
from pathlib import Path

import pytest

from adcp.db.migrations import MIGRATIONS_DIR
from adcp.db.migrations.runner import build_config, head_revision, script_directory
from adcp.db.tables import INGESTION_STATUS_VALUES, metadata

pytestmark = pytest.mark.unit

VERSIONS_DIR = MIGRATIONS_DIR / "versions"

EXPECTED_CHAIN = (
    "0001_locations",
    "0002_ingestion_runs",
    "0003_weather_hourly",
    "0004_ingestion_run_errors",
    "0005_ingestion_watermarks",
)

EXPECTED_TABLES = {
    "locations",
    "ingestion_runs",
    "weather_hourly",
    "ingestion_run_errors",
    "ingestion_watermarks",
}


def test_revisions_form_one_linear_chain_ending_at_head() -> None:
    scripts = script_directory(build_config())

    assert scripts.get_heads() == ["0005_ingestion_watermarks"]
    assert head_revision() == "0005_ingestion_watermarks"

    ordered = [revision.revision for revision in reversed(list(scripts.walk_revisions()))]
    assert tuple(ordered) == EXPECTED_CHAIN, "migration order must be deterministic"


def test_every_revision_is_reachable_from_the_next_one() -> None:
    scripts = script_directory(build_config())
    for older, newer in pairwise(EXPECTED_CHAIN):
        head_of_older = scripts.get_revision(older).nextrev
        assert head_of_older == {newer}, f"{older} must point at exactly {newer}"


def test_version_files_are_named_after_their_revision() -> None:
    files = sorted(path.name for path in VERSIONS_DIR.glob("*.py"))

    assert files == [f"{revision}.py" for revision in EXPECTED_CHAIN]


def test_migrations_stay_self_contained() -> None:
    """Historical snapshots must not import application code (PLAN section 5.9)."""
    offenders: list[str] = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        if re.search(r"^\s*(from|import)\s+adcp\b", source, flags=re.MULTILINE):
            offenders.append(path.name)

    assert offenders == []


def test_metadata_declares_the_five_plan_tables() -> None:
    assert set(metadata.tables) == EXPECTED_TABLES


def test_ingestion_status_values_match_the_plan() -> None:
    assert INGESTION_STATUS_VALUES == ("running", "succeeded", "partial", "failed", "skipped")


def test_migration_directory_is_inside_the_package() -> None:
    # Packaged with the wheel so `adcp db upgrade` works inside the container.
    assert MIGRATIONS_DIR.is_dir()
    assert (MIGRATIONS_DIR / "env.py").is_file()
    assert (MIGRATIONS_DIR / "script.py.mako").is_file()
    assert Path(MIGRATIONS_DIR).name == "migrations"


def test_alembic_ini_points_at_the_packaged_scripts() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / "alembic.ini").read_text(encoding="utf-8")

    assert "script_location = src/adcp/db/migrations" in text
    assert "sqlalchemy.url =" in text, "the DSN comes from ADCP settings, not the ini"
