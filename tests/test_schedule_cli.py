"""``adcp schedule``: startup validation, exit codes, and wiring."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
from typer.testing import CliRunner

from adcp.cli import app
from adcp.db.migrations.runner import SchemaRevision
from adcp.exit_codes import ExitCode
from adcp.scheduler import SchedulerMode, SchedulerPlan

pytestmark = pytest.mark.unit

runner = CliRunner()


class StubScheduler:
    """Records how the command configured the scheduler, then returns immediately."""

    started: ClassVar[bool] = False
    plans: ClassVar[list[SchedulerPlan]] = []
    runners: ClassVar[list[Any]] = []

    def __init__(self, plan: SchedulerPlan, *, runner: Any, skip_if_running: bool = True) -> None:
        StubScheduler.plans.append(plan)
        StubScheduler.runners.append(runner)
        self._skip_if_running = skip_if_running

    def start(self) -> None:
        StubScheduler.started = True


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> type[StubScheduler]:
    StubScheduler.started = False
    StubScheduler.plans = []
    StubScheduler.runners = []
    monkeypatch.setattr("adcp.cli.schedule_cmd.CollectionScheduler", StubScheduler)
    return StubScheduler


@pytest.fixture
def current_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "adcp.cli.schedule_cmd.schema_revision",
        lambda **_kwargs: SchemaRevision(
            revision="0005_ingestion_watermarks",
            head="0005_ingestion_watermarks",
            pending=(),
        ),
    )


@pytest.fixture
def scheduling_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADCP_SCHEDULER_ENABLED", "true")


def test_scheduling_help_lists_the_command() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "schedule" in result.output


def test_scheduling_is_refused_when_disabled() -> None:
    result = runner.invoke(app, ["schedule"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "ADCP_SCHEDULER_ENABLED=true" in result.output


def test_scheduling_starts_and_exits_zero(
    stub: type[StubScheduler],
    current_schema: None,
    scheduling_enabled: None,
) -> None:
    result = runner.invoke(app, ["schedule", "--interval-seconds", "15", "--run-once"])

    assert result.exit_code == ExitCode.OK, result.output
    assert stub.started is True
    plan = stub.plans[-1]
    assert plan.mode is SchedulerMode.INTERVAL
    assert plan.run_once is True
    assert "every 15s" in plan.description
    assert "Scheduling: every 15s" in result.output
    assert callable(stub.runners[-1])


def test_scheduling_honours_minute_and_timezone(
    stub: type[StubScheduler],
    current_schema: None,
    scheduling_enabled: None,
) -> None:
    result = runner.invoke(app, ["schedule", "--minute", "42", "--timezone", "Europe/Belgrade"])

    assert result.exit_code == ExitCode.OK, result.output
    plan = stub.plans[-1]
    assert plan.mode is SchedulerMode.HOURLY
    assert plan.timezone == "Europe/Belgrade"
    assert plan.description == "hourly at minute 42"


@pytest.mark.parametrize(
    "arguments",
    [
        ["schedule", "--minute", "99"],
        ["schedule", "--timezone", "Mars/Olympus_Mons"],
        ["schedule", "--interval-seconds", "0"],
    ],
)
def test_invalid_scheduling_configuration_exits_two(
    arguments: list[str],
    stub: type[StubScheduler],
    current_schema: None,
    scheduling_enabled: None,
) -> None:
    result = runner.invoke(app, arguments)

    assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
    assert stub.started is False


def test_schema_drift_is_reported_before_starting(
    stub: type[StubScheduler],
    monkeypatch: pytest.MonkeyPatch,
    scheduling_enabled: None,
) -> None:
    monkeypatch.setattr(
        "adcp.cli.schedule_cmd.schema_revision",
        lambda **_kwargs: SchemaRevision(
            revision="0003_weather_hourly",
            head="0005_ingestion_watermarks",
            pending=("0004_ingestion_run_errors", "0005_ingestion_watermarks"),
        ),
    )

    result = runner.invoke(app, ["schedule"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "behind head" in result.output
    assert stub.started is False


def test_unreachable_database_is_an_operational_failure(
    stub: type[StubScheduler],
    monkeypatch: pytest.MonkeyPatch,
    scheduling_enabled: None,
) -> None:
    monkeypatch.setenv(
        "ADCP_DATABASE_URL",
        "postgresql+psycopg://adcp:topsecret@127.0.0.1:59997/adcp",
    )
    monkeypatch.setenv("ADCP_DB_CONNECT_TIMEOUT_S", "1")

    result = runner.invoke(app, ["schedule"])

    assert result.exit_code == ExitCode.FAILURE
    assert "cannot start the scheduler" in result.output
    assert "topsecret" not in result.output
    assert stub.started is False


def test_scheduling_output_never_contains_secrets(
    stub: type[StubScheduler],
    current_schema: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADCP_SCHEDULER_ENABLED", "true")
    monkeypatch.setenv("ADCP_OPEN_METEO_API_KEY", "canary-scheduler-key")
    monkeypatch.setenv(
        "ADCP_DATABASE_URL", "postgresql+psycopg://adcp:canary-db-pass@localhost:55432/adcp"
    )

    result = runner.invoke(app, ["schedule", "--interval-seconds", "5"])

    assert result.exit_code == ExitCode.OK, result.output
    assert "canary-scheduler-key" not in result.output
    assert "canary-db-pass" not in result.output
