"""``adcp db ...`` exit codes, JSON output, and credential hygiene."""

from __future__ import annotations

import json

import pytest
import sqlalchemy as sa
from typer.testing import CliRunner

from adcp.cli import app
from adcp.db.migrations.runner import downgrade, schema_revision, upgrade

pytestmark = pytest.mark.integration

runner = CliRunner()

UNREACHABLE_URL = "postgresql+psycopg://adcp:topsecret@127.0.0.1:59999/adcp"


def _combined(result: object) -> str:
    parts = [getattr(result, "output", "") or ""]
    stderr = getattr(result, "stderr", "") or ""
    if stderr:
        parts.append(stderr)
    return "".join(parts)


def _password(url: str) -> str:
    return sa.engine.make_url(url).password or ""


def _assert_password_absent(url: str, output: str) -> None:
    parsed = sa.engine.make_url(url)
    password = parsed.password or ""
    if password and password not in {parsed.database, parsed.username}:
        assert password not in output, "the database password must never be printed"


def _assert_dsn_masked(url: str, output: str) -> None:
    _assert_password_absent(url, output)
    assert "***" in output, "the DSN must be rendered with a masked password"


def test_db_ping_reports_success(cli_database_env: str) -> None:
    result = runner.invoke(app, ["db", "ping"])

    assert result.exit_code == 0, _combined(result)
    assert "Database OK" in result.output
    assert "PostgreSQL" in result.output


def test_db_ping_json_is_machine_readable_and_masked(cli_database_env: str) -> None:
    result = runner.invoke(app, ["db", "ping", "--json"])

    assert result.exit_code == 0, _combined(result)
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["database"] == sa.engine.make_url(cli_database_env).database
    _assert_dsn_masked(cli_database_env, result.output)


def test_db_current_reports_head(cli_database_env: str) -> None:
    result = runner.invoke(app, ["db", "current"])

    assert result.exit_code == 0, _combined(result)
    assert "up to date" in result.output

    json_result = runner.invoke(app, ["db", "current", "--json"])
    payload = json.loads(json_result.output)
    assert payload["up_to_date"] is True
    assert payload["revision"] == payload["head"]
    assert payload["pending"] == []
    _assert_password_absent(cli_database_env, result.output)
    _assert_dsn_masked(cli_database_env, json_result.output)


def test_db_current_check_fails_when_the_schema_is_behind(
    cli_database_env: str,
    migrated_database: str,
) -> None:
    downgrade(database_url=migrated_database, connect_timeout_s=5)

    behind = runner.invoke(app, ["db", "current", "--check"])

    assert behind.exit_code == 1, _combined(behind)
    assert "behind head" in behind.output

    upgrade(database_url=migrated_database, connect_timeout_s=5)
    at_head = runner.invoke(app, ["db", "current", "--check"])

    assert at_head.exit_code == 0, _combined(at_head)


def test_db_upgrade_applies_pending_migrations(
    cli_database_env: str,
    migrated_database: str,
) -> None:
    downgrade(database_url=migrated_database, revision="-2", connect_timeout_s=5)

    result = runner.invoke(app, ["db", "upgrade", "--json"])

    assert result.exit_code == 0, _combined(result)
    payload = json.loads(result.output)
    assert payload["applied"] == ["0004_ingestion_run_errors", "0005_ingestion_watermarks"]
    assert payload["previous_revision"] == "0003_weather_hourly"
    assert payload["revision"] == payload["head"]
    assert payload["up_to_date"] is True
    assert schema_revision(database_url=migrated_database).is_current


def test_db_upgrade_is_a_no_op_when_already_current(cli_database_env: str) -> None:
    result = runner.invoke(app, ["db", "upgrade"])

    assert result.exit_code == 0, _combined(result)
    assert "already up to date" in result.output

    payload = json.loads(runner.invoke(app, ["db", "upgrade", "--json"]).output)
    assert payload["applied"] == []


@pytest.mark.parametrize("command", [["db", "ping"], ["db", "upgrade"], ["db", "current"]])
def test_db_commands_exit_one_when_the_database_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
) -> None:
    monkeypatch.setenv("ADCP_DATABASE_URL", UNREACHABLE_URL)
    monkeypatch.setenv("ADCP_DB_CONNECT_TIMEOUT_S", "1")

    result = runner.invoke(app, command)
    output = _combined(result)

    assert result.exit_code == 1, output
    assert "topsecret" not in output
    assert "***" in output
    assert "Traceback" not in output


def test_db_ping_json_reports_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADCP_DATABASE_URL", UNREACHABLE_URL)
    monkeypatch.setenv("ADCP_DB_CONNECT_TIMEOUT_S", "1")

    result = runner.invoke(app, ["db", "ping", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "unreachable"
    assert payload["error_type"] == "DatabaseUnavailableError"
    assert "topsecret" not in json.dumps(payload)


@pytest.mark.parametrize("command", [["db", "ping"], ["db", "upgrade"], ["db", "current"]])
def test_db_commands_exit_two_on_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
) -> None:
    monkeypatch.setenv("ADCP_LOG_FORMAT", "yaml")

    result = runner.invoke(app, command)

    assert result.exit_code == 2, _combined(result)
    assert "Configuration is invalid" in _combined(result)
