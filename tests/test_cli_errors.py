"""The CLI's last line of defence: consistent exit codes, no tracebacks."""

from __future__ import annotations

import io
from typing import Any

import pytest

from adcp.cli.main import INTERRUPTED_EXIT_CODE, entrypoint, exit_code_for_error
from adcp.errors import (
    ConfigurationError,
    DatabaseUnavailableError,
    LocationNotFoundError,
    MigrationError,
    RunNotFoundError,
)
from adcp.exit_codes import ExitCode
from adcp.logging import configure_logging
from tests.support import events_named

pytestmark = pytest.mark.unit


class BoomApp:
    """Stands in for the Typer app and raises whatever the test needs."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    def __call__(self, **_kwargs: Any) -> None:
        raise self._error


@pytest.fixture
def logs() -> io.StringIO:
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG", log_format="json", service="adcp", environment="test", stream=buffer
    )
    return buffer


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ConfigurationError("bad flag combination"), ExitCode.CONFIG_ERROR),
        (DatabaseUnavailableError("cannot reach PostgreSQL"), ExitCode.FAILURE),
        (MigrationError("migration failed"), ExitCode.FAILURE),
        (LocationNotFoundError("no such slug"), ExitCode.FAILURE),
        (RunNotFoundError("no such run"), ExitCode.FAILURE),
    ],
)
def test_errors_that_reach_the_top_keep_their_documented_exit_code(
    error: BaseException,
    expected: ExitCode,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    logs: io.StringIO,
) -> None:
    monkeypatch.setattr("adcp.cli.main.app", BoomApp(error))

    with pytest.raises(SystemExit) as info:
        entrypoint()

    assert info.value.code == expected
    printed = capsys.readouterr().err
    assert "Traceback" not in printed
    assert "Error:" in printed


def test_unexpected_errors_are_logged_and_reported_without_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    logs: io.StringIO,
) -> None:
    monkeypatch.setattr(
        "adcp.cli.main.app",
        BoomApp(ZeroDivisionError("division by zero")),
    )

    with pytest.raises(SystemExit) as info:
        entrypoint()

    assert info.value.code == ExitCode.FAILURE
    printed = capsys.readouterr().err
    assert "Traceback" not in printed
    assert "Unexpected error (ZeroDivisionError)" in printed
    assert "ADCP_LOG_LEVEL=DEBUG" in printed
    logged = events_named(logs, "cli.unexpected_error")
    assert len(logged) == 1
    assert logged[0]["error_type"] == "ZeroDivisionError"
    assert logged[0]["exception"]  # the stack still reaches the structured log


def test_interrupts_exit_with_the_shell_convention(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    logs: io.StringIO,
) -> None:
    monkeypatch.setattr("adcp.cli.main.app", BoomApp(KeyboardInterrupt()))

    with pytest.raises(SystemExit) as info:
        entrypoint()

    assert info.value.code == INTERRUPTED_EXIT_CODE == 130
    assert "Interrupted" in capsys.readouterr().err


def test_credentials_are_masked_in_unexpected_error_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    logs: io.StringIO,
) -> None:
    monkeypatch.setattr(
        "adcp.cli.main.app",
        BoomApp(
            RuntimeError(
                "cannot use postgresql+psycopg://adcp:hunter2@db.internal:5432/adcp "
                "with apikey=canary-key-999",
            ),
        ),
    )

    with pytest.raises(SystemExit):
        entrypoint()

    printed = capsys.readouterr().err
    assert "hunter2" not in printed
    assert "canary-key-999" not in printed
    assert "***" in printed
    assert "hunter2" not in logs.getvalue()
    assert "canary-key-999" not in logs.getvalue()


def test_a_clean_run_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    class QuietApp:
        def __call__(self, **_kwargs: Any) -> None:
            return None

    monkeypatch.setattr("adcp.cli.main.app", QuietApp())

    with pytest.raises(SystemExit) as info:
        entrypoint()

    assert info.value.code == ExitCode.OK


def test_error_to_exit_code_mapping_is_exhaustive() -> None:
    assert exit_code_for_error(ConfigurationError("x")) is ExitCode.CONFIG_ERROR
    assert exit_code_for_error(MigrationError("x")) is ExitCode.FAILURE
    assert exit_code_for_error(DatabaseUnavailableError("x")) is ExitCode.FAILURE
