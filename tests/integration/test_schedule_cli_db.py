"""``adcp schedule`` and the scheduler loop against real PostgreSQL.

These tests run the *real* scheduler (APScheduler, the same plan builder, the same
job body) with the real CLI composition helper, mocking only the HTTP transport.
They prove the operational promises: a cycle writes rows, a failed cycle does not
corrupt the next one, repeated cycles create no duplicates, and shutdown is clean.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
from datetime import timedelta
from decimal import Decimal
from typing import cast

import httpx
import pytest
import respx
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

from adcp.cli import app
from adcp.cli.collect_cmd import run_collection_once
from adcp.config import Settings, get_settings
from adcp.db.migrations.runner import downgrade, upgrade
from adcp.db.repository import LocationRepository
from adcp.db.tables import ingestion_runs, weather_hourly
from adcp.exit_codes import ExitCode
from adcp.scheduler import CollectionScheduler, build_schedule
from tests.support import payload_for_recent_hours

pytestmark = pytest.mark.integration

runner = CliRunner()
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
LOOKBACK_HOURS = 3


class OneShotScheduler(CollectionScheduler):
    """Real scheduler, but ``start`` runs one cycle and stops (for CLI tests)."""

    def start(self) -> None:
        self.configure()
        self.run_once(trigger="run_once")
        self.shutdown(reason="one-shot")


@pytest.fixture
def fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADCP_OPEN_METEO_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("ADCP_OPEN_METEO_BACKOFF_INITIAL_S", "0.001")
    monkeypatch.setenv("ADCP_OPEN_METEO_BACKOFF_MAX_S", "0.01")
    monkeypatch.setenv("ADCP_SCHEDULER_ENABLED", "true")


@pytest.fixture
def one_shot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("adcp.cli.schedule_cmd.CollectionScheduler", OneShotScheduler)


def seed_location(engine: Engine, slug: str = "belgrade-rs") -> int:
    record = LocationRepository(engine).create(
        slug=slug,
        name=slug.replace("-", " ").title(),
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
        country_code="RS",
    )
    return record.id


def stored_rows(engine: Engine) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                sa.select(weather_hourly).order_by(weather_hourly.c.observed_at),
            ).mappings()
        ]


def run_rows(engine: Engine) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                sa.select(ingestion_runs).order_by(ingestion_runs.c.started_at),
            ).mappings()
        ]


def scheduler_settings(database_url: str) -> Settings:
    return Settings(
        _env_file=None,
        database_url=database_url,
        scheduler_enabled=True,
        scheduler_interval_seconds=1,
        ingest_lookback_hours=LOOKBACK_HOURS,
        ingest_overlap_hours=1,
        open_meteo_max_concurrency=1,
        open_meteo_max_attempts=1,
        open_meteo_backoff_initial_s=0.001,
        open_meteo_backoff_max_s=0.01,
    )


def test_scheduled_cycle_writes_rows_and_labels_the_run(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
    one_shot: None,
) -> None:
    seed_location(db_engine)
    payload = payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS)

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(return_value=httpx.Response(200, json=payload))
        result = runner.invoke(
            app,
            ["schedule", "--interval-seconds", "1", "--run-once"],
        )

    assert result.exit_code == ExitCode.OK, result.output
    rows = stored_rows(db_engine)
    assert len(rows) == LOOKBACK_HOURS
    runs = run_rows(db_engine)
    assert len(runs) == 1
    assert runs[0]["status"] == "succeeded"
    assert runs[0]["trigger"] == "scheduler", "scheduled runs are labelled as such"
    assert runs[0]["run_type"] == "scheduled"
    assert runs[0]["rows_inserted"] == LOOKBACK_HOURS


def test_scheduler_survives_a_failed_cycle_then_collects(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    """Cycle one sees a 503; cycle two must still run and write the data."""
    seed_location(db_engine)
    payload = payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS)
    attempts = itertools.count()

    def responder(_request: httpx.Request) -> httpx.Response:
        # The first cycle fails, everything after it succeeds.
        return (
            httpx.Response(503, text="temporarily down")
            if next(attempts) == 0
            else httpx.Response(200, json=payload)
        )

    settings = scheduler_settings(cli_database_env)
    scheduler = CollectionScheduler(
        build_schedule(settings),
        runner=lambda: run_collection_once(settings, trigger="scheduler"),
    )

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(side_effect=responder)
        thread = threading.Thread(target=scheduler.start, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and len(run_rows(db_engine)) < 2:
                time.sleep(0.1)
        finally:
            scheduler.shutdown(reason="test finished")
            thread.join(timeout=10)

    assert scheduler.stopped is True
    assert not thread.is_alive(), "the scheduler thread stopped"
    runs = run_rows(db_engine)
    assert len(runs) >= 2, "two cycles ran"
    assert runs[0]["status"] == "failed", "the first cycle failed on the 503"
    assert runs[-1]["status"] == "succeeded", "a later cycle still collected"
    assert runs[-1]["rows_inserted"] == LOOKBACK_HOURS
    rows = stored_rows(db_engine)
    assert len(rows) == LOOKBACK_HOURS, "the failed cycle left nothing behind"


def test_repeated_cycles_do_not_duplicate_rows(
    cli_database_env: str,
    db_engine: Engine,
    fast_retries: None,
) -> None:
    seed_location(db_engine)
    payload = payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS)
    settings = scheduler_settings(cli_database_env)
    scheduler = CollectionScheduler(
        build_schedule(settings),
        runner=lambda: run_collection_once(settings, trigger="scheduler"),
    )

    with respx.mock(assert_all_called=False) as router:
        router.get(FORECAST_URL).mock(return_value=httpx.Response(200, json=payload))
        thread = threading.Thread(target=scheduler.start, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and len(run_rows(db_engine)) < 3:
                time.sleep(0.1)
        finally:
            scheduler.shutdown(reason="test finished")
            thread.join(timeout=10)

    runs = run_rows(db_engine)
    assert len(runs) >= 3, "at least three cycles ran"
    rows = stored_rows(db_engine)
    moments = [row["observed_at"] for row in rows]
    assert len(moments) == len(set(moments)), "no duplicate hours"
    assert len(rows) == LOOKBACK_HOURS
    assert all(row["revision_count"] == 0 for row in rows)
    first_inserted = cast(int, runs[0]["rows_inserted"])
    assert first_inserted == LOOKBACK_HOURS
    assert all(cast(int, run["rows_inserted"]) == 0 for run in runs[1:]), "later cycles add nothing"
    assert scheduler.runs_completed >= 3


def test_schedule_refuses_to_start_behind_head_schema(
    cli_database_env: str,
    db_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    fast_retries: None,
) -> None:
    seed_location(db_engine)
    downgrade(database_url=cli_database_env, connect_timeout_s=5)
    get_settings.cache_clear()
    try:
        result = runner.invoke(app, ["schedule"])
    finally:
        upgrade(database_url=cli_database_env, connect_timeout_s=5)
        get_settings.cache_clear()

    assert result.exit_code == ExitCode.CONFIG_ERROR
    assert "behind head" in result.output
    assert run_rows(db_engine) == []


def test_scheduler_plan_describes_the_configured_cadence(
    cli_database_env: str,
) -> None:
    """The development cadence comes from configuration, not from the tests."""
    settings = scheduler_settings(cli_database_env)

    plan = build_schedule(settings)

    assert plan.mode.value == "interval"
    assert plan.description == "every 1s (development cadence)"
    described = plan.as_dict()
    assert json.loads(json.dumps(described))["mode"] == "interval"
    assert isinstance(plan.trigger.interval, timedelta)
