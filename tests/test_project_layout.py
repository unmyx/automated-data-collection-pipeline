"""Checks that keep the scaffold, its configuration, and its docs in sync."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import AliasChoices

import adcp
from adcp.config import Settings

pytestmark = [pytest.mark.unit, pytest.mark.smoke]

REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    ".dockerignore",
    ".env.example",
    ".gitignore",
    "Dockerfile",
    "LICENSE",
    "README.md",
    "alembic.ini",
    "docker-compose.yml",
    "pyproject.toml",
    "uv.lock",
    "docs/PLAN.md",
    "docs/ARCHITECTURE.md",
    "docs/CASE_STUDY.md",
    "docs/DEMO.md",
    "docs/PORTFOLIO.md",
    "docs/RUNBOOK.md",
    "docs/queries.sql",
    "docs/samples/collection-summary.txt",
    "docs/samples/idempotent-rerun.txt",
    "docs/samples/retry-log.jsonl",
    "src/adcp/__init__.py",
    "src/adcp/__main__.py",
    "src/adcp/api/__init__.py",
    "src/adcp/api/mapping.py",
    "src/adcp/api/open_meteo.py",
    "src/adcp/api/requests.py",
    "src/adcp/api/schemas.py",
    "src/adcp/cli/__init__.py",
    "src/adcp/cli/common.py",
    "src/adcp/cli/config_cmd.py",
    "src/adcp/cli/db_cmd.py",
    "src/adcp/cli/collect_cmd.py",
    "src/adcp/cli/schedule_cmd.py",
    "src/adcp/cli/main.py",
    "src/adcp/config.py",
    "src/adcp/db/__init__.py",
    "src/adcp/db/engine.py",
    "src/adcp/db/lock.py",
    "src/adcp/db/migrations/__init__.py",
    "src/adcp/db/migrations/env.py",
    "src/adcp/db/migrations/runner.py",
    "src/adcp/db/migrations/script.py.mako",
    "src/adcp/db/migrations/versions/0001_locations.py",
    "src/adcp/db/migrations/versions/0002_ingestion_runs.py",
    "src/adcp/db/migrations/versions/0003_weather_hourly.py",
    "src/adcp/db/migrations/versions/0004_ingestion_run_errors.py",
    "src/adcp/db/migrations/versions/0005_ingestion_watermarks.py",
    "src/adcp/db/repository.py",
    "src/adcp/db/run_tracker.py",
    "src/adcp/db/tables.py",
    "src/adcp/db/watermark_store.py",
    "src/adcp/errors.py",
    "src/adcp/exit_codes.py",
    "src/adcp/logging.py",
    "src/adcp/models/__init__.py",
    "src/adcp/models/location.py",
    "src/adcp/models/observation.py",
    "src/adcp/models/run.py",
    "src/adcp/models/window.py",
    "src/adcp/normalization.py",
    "src/adcp/pipeline/__init__.py",
    "src/adcp/pipeline/service.py",
    "src/adcp/pipeline/window.py",
    "src/adcp/ports.py",
    "src/adcp/py.typed",
    "src/adcp/resilience.py",
    "src/adcp/scheduler.py",
    "src/adcp/validation/__init__.py",
    "src/adcp/validation/rules.py",
    "src/adcp/validation/validator.py",
    "scripts/capture_samples.py",
    "scripts/demo.py",
    "scripts/load_check.py",
    "scripts/record_open_meteo_fixtures.py",
    "scripts/scheduler_demo.py",
    "scripts/seed_locations.py",
)

#: Keys in .env.example that are deliberately not ``Settings`` fields: Compose reads
#: the POSTGRES_* ones, and the test suite reads ADCP_TEST_DATABASE_URL from the
#: process environment directly.
NON_SETTINGS_KEYS = frozenset(
    {
        "ADCP_TEST_ALLOW_ANY_DATABASE",
        "ADCP_TEST_DATABASE_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_PORT",
    },
)


def _pyproject() -> dict[str, Any]:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def _env_example_keys() -> set[str]:
    """Every ``KEY`` mentioned in .env.example, including commented-out lines."""
    keys: set[str] = set()
    for raw_line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip().lstrip("#").strip()
        if "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if key:
            keys.add(key)
    return keys


def _declared_env_names() -> dict[str, set[str]]:
    """Map each ``Settings`` field to the environment names that can set it."""
    declared: dict[str, set[str]] = {}
    for name, field in Settings.model_fields.items():
        alias = field.validation_alias
        if isinstance(alias, AliasChoices):
            declared[name] = {str(choice) for choice in alias.choices}
        elif isinstance(alias, str):
            declared[name] = {alias}
        else:
            declared[name] = {f"ADCP_{name.upper()}"}
    return declared


@pytest.mark.parametrize("relative_path", REQUIRED_FILES)
def test_required_scaffold_files_exist(relative_path: str) -> None:
    assert (REPO_ROOT / relative_path).is_file(), f"{relative_path} is missing from the scaffold"


def test_env_example_documents_every_setting() -> None:
    documented = _env_example_keys()
    missing = {
        field: sorted(names)
        for field, names in _declared_env_names().items()
        if not names & documented
    }

    assert not missing, (
        f"settings are undocumented in .env.example: {json.dumps(missing, indent=2)}"
    )


def test_env_example_has_no_unknown_keys() -> None:
    documented = _env_example_keys()
    known = {name for names in _declared_env_names().values() for name in names} | NON_SETTINGS_KEYS

    assert documented - known == set()


def test_pyproject_metadata_matches_the_package() -> None:
    project = _pyproject()["project"]

    assert project["name"] == "adcp"
    assert project["version"] == adcp.__version__
    assert project["requires-python"].startswith(">=3.12")
    assert project["scripts"]["adcp"] == "adcp.cli:entrypoint"
    assert project["license"] == "MIT"


def test_pyproject_declares_runtime_and_dev_dependencies() -> None:
    pyproject = _pyproject()
    runtime = set(pyproject["project"]["dependencies"])
    dev = set(pyproject["dependency-groups"]["dev"])

    for expected in ("httpx", "pydantic", "pydantic-settings", "structlog", "tenacity", "typer"):
        assert any(dependency.startswith(expected) for dependency in runtime), expected

    for expected in ("alembic", "psycopg", "sqlalchemy"):
        assert any(dependency.startswith(expected) for dependency in runtime), expected

    for expected in ("mypy", "pytest", "respx", "ruff", "testcontainers"):
        assert any(dependency.startswith(expected) for dependency in dev), expected


def test_quality_tooling_is_configured() -> None:
    pyproject = _pyproject()

    assert pyproject["tool"]["ruff"]["line-length"] == 100
    assert pyproject["tool"]["mypy"]["strict"] is True
    assert pyproject["tool"]["pytest"]["ini_options"]["testpaths"] == ["tests"]
    assert pyproject["tool"]["coverage"]["report"]["fail_under"] >= 80


def test_docker_compose_defines_a_healthy_postgres_service() -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    assert compose["services"]["postgres"]["image"].startswith("postgres:")
    assert "healthcheck" in compose["services"]["postgres"]
    assert compose["services"]["postgres"]["volumes"]
    assert compose["volumes"], "a named volume keeps local data across restarts"


def test_plan_document_covers_every_required_section() -> None:
    plan = (REPO_ROOT / "docs" / "PLAN.md").read_text(encoding="utf-8")

    required_sections = (
        "Business/problem statement",
        "Scope and non-goals",
        "Architecture",
        "Data flow",
        "PostgreSQL schema",
        "API integration design",
        "Scheduling approach",
        "Retry/timeout strategy",
        "Validation rules",
        "Idempotency strategy",
        "Failure and partial-failure behaviour",
        "Logging and observability",
        "CLI design",
        "Testing strategy",
        "Configuration/environment variables",
        "Repository structure",
        "Milestones M1 onward",
        "Portfolio/demo requirements",
    )

    for section in required_sections:
        assert section in plan, f"PLAN.md is missing the '{section}' section"
