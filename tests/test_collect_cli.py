"""``adcp collect``: exit codes, JSON payloads, and flag plumbing."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, ClassVar, cast

import pytest
from typer.testing import CliRunner

from adcp.cli import app
from adcp.cli.collect_cmd import exit_code_for, render_summary
from adcp.config import Settings
from adcp.errors import ConfigurationError, DatabaseUnavailableError
from adcp.exit_codes import ExitCode
from adcp.models.observation import ObservationSource
from adcp.models.run import (
    LocationResult,
    LocationStatus,
    RunCounts,
    RunStatus,
    RunSummary,
)

pytestmark = pytest.mark.unit

runner = CliRunner()
NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def make_summary(status: RunStatus, **counts: Any) -> RunSummary:
    result = LocationResult(
        slug="belgrade-rs",
        source=ObservationSource.FORECAST,
        status=LocationStatus.SUCCEEDED
        if status is not RunStatus.FAILED
        else LocationStatus.FAILED,
        rows_received=counts.get("rows_received", 3),
        rows_accepted=counts.get("rows_accepted", 3),
        rows_inserted=counts.get("rows_inserted", 3),
        rows_updated=counts.get("rows_updated", 0),
        rows_unchanged=counts.get("rows_unchanged", 0),
        rows_rejected=counts.get("rows_rejected", 0),
        error_type="UpstreamServerError" if status is RunStatus.FAILED else None,
        error_message="upstream is down" if status is RunStatus.FAILED else None,
    )
    return RunSummary(
        status=status,
        trigger="cli",
        started_at=NOW,
        finished_at=NOW,
        counts=RunCounts.from_results([result], locations_total=1),
        results=(result,),
    )


class StubService:
    """Captures how the CLI called the service and returns canned summaries."""

    calls: ClassVar[list[dict[str, Any]]] = []
    settings_seen: ClassVar[list[Settings]] = []
    outcome: ClassVar[Any] = None

    def __init__(self, settings: Settings, **_kwargs: Any) -> None:
        StubService.settings_seen.append(settings)

    def run(self, **kwargs: Any) -> RunSummary:
        StubService.calls.append(kwargs)
        if isinstance(StubService.outcome, Exception):
            raise StubService.outcome
        return cast(RunSummary, StubService.outcome)


@pytest.fixture(autouse=True)
def stub_service(monkeypatch: pytest.MonkeyPatch) -> type[StubService]:
    StubService.calls = []
    StubService.settings_seen = []
    StubService.outcome = make_summary(RunStatus.SUCCEEDED)
    monkeypatch.setattr("adcp.cli.collect_cmd.CollectionService", StubService)
    return StubService


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (RunStatus.SUCCEEDED, ExitCode.OK),
        (RunStatus.SKIPPED, ExitCode.OK),
        (RunStatus.PARTIAL, ExitCode.PARTIAL),
        (RunStatus.FAILED, ExitCode.FAILURE),
    ],
)
def test_exit_codes_follow_the_contract(
    status: RunStatus,
    expected: ExitCode,
) -> None:
    assert exit_code_for(make_summary(status)) is expected


def test_successful_run_exits_zero_with_a_human_summary() -> None:
    result = runner.invoke(app, ["collect"])

    assert result.exit_code == 0, result.output
    assert "Collection complete: succeeded" in result.output
    assert "belgrade-rs" in result.output
    assert "inserted 3" in result.output


def test_partial_run_exits_three() -> None:
    StubService.outcome = make_summary(RunStatus.PARTIAL, rows_rejected=1, rows_unchanged=2)

    result = runner.invoke(app, ["collect"])

    assert result.exit_code == ExitCode.PARTIAL


def test_failed_run_exits_one() -> None:
    StubService.outcome = make_summary(RunStatus.FAILED)

    result = runner.invoke(app, ["collect"])

    assert result.exit_code == ExitCode.FAILURE
    assert "failed" in result.output


def test_json_output_is_machine_readable() -> None:
    result = runner.invoke(app, ["collect", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "succeeded"
    assert payload["rows_inserted"] == 3
    assert payload["locations_total"] == 1
    assert payload["dry_run"] is False
    assert payload["run_id"] is None


def test_json_output_never_contains_the_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADCP_OPEN_METEO_API_KEY", "canary-key-123")

    result = runner.invoke(app, ["collect", "--json"])

    assert result.exit_code == 0
    assert "canary-key-123" not in result.output


def test_dry_run_is_passed_through() -> None:
    runner.invoke(app, ["collect", "--dry-run"])

    assert StubService.calls[-1]["dry_run"] is True


def test_location_filter_is_repeatable() -> None:
    runner.invoke(app, ["collect", "--location", "belgrade-rs", "-l", "reykjavik-is"])

    assert StubService.calls[-1]["location_slugs"] == ["belgrade-rs", "reykjavik-is"]


def test_lookback_and_overlap_overrides_reach_the_service() -> None:
    runner.invoke(app, ["collect", "--lookback-hours", "10", "--overlap-hours", "3"])

    settings = StubService.settings_seen[-1]
    assert settings.ingest_lookback_hours == 10
    assert settings.ingest_overlap_hours == 3


def test_conflicting_overrides_exit_two() -> None:
    result = runner.invoke(app, ["collect", "--lookback-hours", "5", "--overlap-hours", "6"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "invalid option combination" in result.output
    assert StubService.calls == []


def test_out_of_range_flag_exits_two() -> None:
    result = runner.invoke(app, ["collect", "--lookback-hours", "0"])

    assert result.exit_code == ExitCode.CONFIG_ERROR


def test_configuration_error_from_the_service_exits_two() -> None:
    StubService.outcome = ConfigurationError("unknown or inactive location(s): nowhere")

    result = runner.invoke(app, ["collect"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "unknown or inactive" in result.output


def test_database_failure_exits_one() -> None:
    StubService.outcome = DatabaseUnavailableError("cannot reach PostgreSQL")

    result = runner.invoke(app, ["collect", "--json"])

    assert result.exit_code == ExitCode.FAILURE
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["error_type"] == "DatabaseUnavailableError"


def test_render_summary_lists_failures_with_their_reason() -> None:
    summary = make_summary(RunStatus.FAILED)

    rendered = render_summary(summary)

    assert "belgrade-rs" in rendered
    assert "UpstreamServerError: upstream is down" in rendered
