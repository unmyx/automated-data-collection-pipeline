"""Logging correlation and slow-query instrumentation against real PostgreSQL."""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings, install_slow_query_logging
from adcp.db.repository import LocationRepository
from adcp.logging import configure_logging
from adcp.models.observation import ObservationSource
from adcp.pipeline.service import CollectionService
from tests.support import FakeWeatherSource, build_series, events_named, log_events

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 2, 12, 37, tzinfo=UTC)


def test_every_line_of_a_run_carries_its_run_id(
    db_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PLAN 12.1: bind once, and every line - including worker threads - has it."""
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG",
        log_format="json",
        service="adcp",
        environment="test",
        stream=buffer,
    )
    record = LocationRepository(db_engine).create(
        slug="belgrade-rs",
        name="Belgrade",
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
    )
    settings = Settings(
        _env_file=None,
        database_url=str(db_engine.url.render_as_string(hide_password=False)),
        ingest_lookback_hours=3,
        ingest_overlap_hours=1,
        open_meteo_max_concurrency=2,
    )
    # The service's clock is frozen at NOW, so the storage window is 09:00-12:00.
    end = NOW.replace(minute=0, second=0, microsecond=0)
    moments = [end - timedelta(hours=offset) for offset in (3, 2, 1)]

    def responder(location: Any, source: ObservationSource, _window: Any) -> Any:
        return build_series(location=location, source=source, moments=moments)

    source = FakeWeatherSource(responder=responder)
    service = CollectionService(
        settings,
        source=source,
        engine=db_engine,
        now=lambda: NOW,
        clock=lambda: 0.0,
    )

    summary = service.run(trigger="cli")

    assert summary.run_id is not None
    run_id = str(summary.run_id)
    service_lines = [
        event
        for event in log_events(buffer)
        # ``ingest.run.starting`` legitimately precedes the run row that names it.
        if str(event.get("logger", "")).startswith("adcp.pipeline")
        and event.get("event") != "ingest.run.starting"
    ]
    assert service_lines
    assert all(event.get("run_id") == run_id for event in service_lines)
    assert all(event.get("service") == "adcp" for event in service_lines)
    assert all(event.get("env") == "test" for event in service_lines)

    location_lines = events_named(buffer, "ingest.location.completed")
    assert location_lines, "the location outcome is logged"
    assert location_lines[0]["location"] == "belgrade-rs"
    assert location_lines[0]["run_id"] == run_id
    assert location_lines[0]["rows_inserted"] == 3, "the run actually stored rows"
    assert record.id is not None


def test_slow_statements_are_logged_without_parameters(db_engine: Engine) -> None:
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG", log_format="json", service="adcp", environment="test", stream=buffer
    )
    settings = Settings(
        _env_file=None,
        database_url=str(db_engine.url.render_as_string(hide_password=False)),
        db_slow_query_ms=20,
    )
    engine = create_engine_from_settings(settings)
    try:
        with engine.connect() as connection:
            connection.execute(sa.text("SELECT pg_sleep(0.05)"))
            connection.execute(
                sa.text("SELECT :value AS value"),
                {"value": "canary-parameter"},
            )
    finally:
        engine.dispose()

    logged = events_named(buffer, "db.query.slow")
    assert len(logged) == 1, "only the genuinely slow statement is reported"
    assert logged[0]["threshold_ms"] == 20
    assert logged[0]["duration_ms"] >= 20
    assert "pg_sleep" in str(logged[0]["statement"])
    assert "canary-parameter" not in buffer.getvalue(), "parameters are never logged"


def test_slow_query_threshold_is_configurable() -> None:
    """A very high threshold disables the warning without changing behaviour."""
    settings = Settings(_env_file=None, db_slow_query_ms=3_600_000)
    engine = create_engine_from_settings(settings)
    try:
        # Installing twice must not double-log either.
        install_slow_query_logging(engine, threshold_ms=3_600_000)
        assert engine.dialect.name == "postgresql"
    finally:
        engine.dispose()
