"""``adcp collect`` end to end: real CLI, real client, mocked HTTP, real PostgreSQL.

This is the closest thing to production the suite runs: the command loads
settings, builds the adapter, fetches through ``respx``, validates, and writes to
the test database - so the exit codes and the idempotency claim are checked
against real SQL rather than a stub.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest
import respx
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

from adcp.cli import app
from adcp.db.repository import LocationRepository
from adcp.db.tables import weather_hourly
from adcp.exit_codes import ExitCode
from tests.support import payload_for_recent_hours

pytestmark = pytest.mark.integration

runner = CliRunner()
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

#: Generous enough that an hour ticking over mid-test cannot empty the window.
LOOKBACK_HOURS = 4


@pytest.fixture
def fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADCP_OPEN_METEO_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("ADCP_OPEN_METEO_BACKOFF_INITIAL_S", "0.001")
    monkeypatch.setenv("ADCP_OPEN_METEO_BACKOFF_MAX_S", "0.01")


def seed_location(engine: Engine, slug: str = "belgrade-rs") -> int:
    record = LocationRepository(engine).create(
        slug=slug,
        name=slug.replace("-", " ").title(),
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
        country_code="RS",
    )
    return record.id


def payload_covering_the_window(*, bad_row: bool = False) -> dict[str, Any]:
    """A valid envelope containing exactly the hours the CLI's window covers."""
    # Index 2 is inside the storage window, so it is really validated.
    overrides: dict[int, dict[str, object]] | None = (
        {2: {"temperature_2m": 842.0}} if bad_row else None
    )
    return payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS, overrides=overrides)


def stored_rows(engine: Engine) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                sa.select(weather_hourly).order_by(weather_hourly.c.observed_at),
            ).mappings()
        ]


def json_document(result: Any) -> dict[str, Any]:
    """Parse the command's stdout: structured logs go to stderr, results to stdout."""
    document = json.loads(result.stdout)
    assert isinstance(document, dict)
    return document


def test_collect_runs_end_to_end_and_is_idempotent(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    seed_location(db_engine)
    payload = payload_covering_the_window()
    args = ["collect", "--lookback-hours", str(LOOKBACK_HOURS), "--overlap-hours", "0", "--json"]

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(return_value=httpx.Response(200, json=payload))

        first = runner.invoke(app, args)
        after_first = stored_rows(db_engine)
        second = runner.invoke(app, args)
        after_second = stored_rows(db_engine)

    assert first.exit_code == ExitCode.OK, first.output
    assert second.exit_code == ExitCode.OK, second.output
    first_payload = json_document(first)
    second_payload = json_document(second)

    assert first_payload["status"] == "succeeded"
    assert first_payload["rows_received"] == LOOKBACK_HOURS
    assert first_payload["rows_inserted"] == LOOKBACK_HOURS
    assert first_payload["rows_skipped"] == 0
    assert first_payload["requests_made"] == 1

    # The second run starts from the advanced watermark: it re-observes the newest
    # hour, finds it unchanged, and writes nothing at all.
    assert second_payload["rows_inserted"] == 0
    assert second_payload["rows_updated"] == 0
    assert second_payload["rows_unchanged"] == 1
    assert second_payload["rows_skipped"] == LOOKBACK_HOURS - 1

    assert len(after_first) == LOOKBACK_HOURS
    assert after_second == after_first, "the second run touched nothing"
    assert all(row["revision_count"] == 0 for row in after_second)
    assert all(row["source"] == "forecast" for row in after_second)


def test_collect_reports_partial_success_with_exit_code_three(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    seed_location(db_engine)
    payload = payload_covering_the_window(bad_row=True)

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(return_value=httpx.Response(200, json=payload))
        result = runner.invoke(
            app,
            ["collect", "--lookback-hours", str(LOOKBACK_HOURS), "--overlap-hours", "0", "--json"],
        )

    assert result.exit_code == ExitCode.PARTIAL, result.output
    payload_json = json_document(result)
    assert payload_json["status"] == "partial"
    assert payload_json["rows_rejected"] >= 1
    assert payload_json["rows_inserted"] >= 1


def test_collect_fails_with_exit_code_one_when_the_api_is_down(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    seed_location(db_engine)

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(return_value=httpx.Response(503, text="unavailable"))
        result = runner.invoke(app, ["collect", "--json"])

    assert result.exit_code == ExitCode.FAILURE, result.output
    assert json_document(result)["status"] == "failed"
    assert stored_rows(db_engine) == []


def test_collect_exits_two_for_an_unknown_location(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    seed_location(db_engine)

    result = runner.invoke(app, ["collect", "--location", "nowhere-xx"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "unknown or inactive" in result.output


def test_collect_never_leaks_the_api_key(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADCP_OPEN_METEO_API_KEY", "canary-key-456")
    seed_location(db_engine)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(FORECAST_URL).mock(
            return_value=httpx.Response(200, json=payload_covering_the_window()),
        )
        result = runner.invoke(
            app,
            ["collect", "--lookback-hours", str(LOOKBACK_HOURS), "--overlap-hours", "0", "--json"],
        )

    assert result.exit_code == ExitCode.OK, result.output
    assert route.calls.last.request.url.params["apikey"] == "canary-key-456"
    assert "canary-key-456" not in result.stdout
