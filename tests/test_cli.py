"""Smoke tests for the CLI scaffold."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from adcp.cli import app

pytestmark = pytest.mark.unit

runner = CliRunner()


def _combined_output(result: object) -> str:
    """Concatenate stdout and (where the click version records it) stderr."""
    parts = [getattr(result, "output", "") or ""]
    stderr = getattr(result, "stderr", "") or ""
    if stderr:
        parts.append(stderr)
    return "".join(parts)


def test_version_flag_prints_version_and_exits_zero() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert "adcp " in result.output


def test_version_command_supports_json() -> None:
    result = runner.invoke(app, ["version", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "adcp"
    assert payload["version"].count(".") >= 1


def test_help_is_shown_when_no_command_is_given() -> None:
    result = runner.invoke(app, [])

    assert result.exit_code == 2
    assert "Usage" in result.output


def test_config_show_masks_the_database_password() -> None:
    result = runner.invoke(app, ["config", "show"])

    assert result.exit_code == 0
    assert "database_url" in result.output
    assert "***" in result.output
    assert "adcp_local_dev" not in result.output


def test_config_show_json_is_machine_readable() -> None:
    result = runner.invoke(app, ["config", "show", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["database_url"] == "postgresql+psycopg://adcp:***@localhost:55432/adcp"
    assert payload["env"] == "local"
    assert payload["open_meteo_api_key"] is None


def test_config_check_succeeds_with_defaults() -> None:
    result = runner.invoke(app, ["config", "check"])

    assert result.exit_code == 0
    assert "Configuration OK" in result.output


def test_config_check_json_reports_ok() -> None:
    result = runner.invoke(app, ["config", "check", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["env"] == "local"
    assert "***" in payload["database_url"]


def test_config_check_fails_with_exit_code_two_on_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADCP_LOG_FORMAT", "yaml")

    result = runner.invoke(app, ["config", "check"])

    assert result.exit_code == 2
    output = _combined_output(result)
    assert "Configuration is invalid" in output
    assert "log_format" in output


def test_config_subcommand_help_lists_available_commands() -> None:
    result = runner.invoke(app, ["config", "--help"])

    assert result.exit_code == 0
    assert "show" in result.output
    assert "check" in result.output
