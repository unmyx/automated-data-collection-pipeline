"""The collection service against real PostgreSQL.

The API adapter is replaced by an in-process fake, so these tests are about the
pipeline's own guarantees: window planning, validation, per-location transactions,
watermark advance-on-commit, rejection accounting, and the failure budget.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from adcp.config import Settings
from adcp.db.engine import connection_scope
from adcp.db.lock import advisory_lock
from adcp.db.repository import LocationRecord, LocationRepository, WeatherRepository
from adcp.db.run_tracker import RunTracker
from adcp.db.tables import ingestion_run_errors, ingestion_runs, weather_hourly
from adcp.db.watermark_store import WatermarkStore
from adcp.errors import ConfigurationError, UpstreamServerError
from adcp.models.location import Location
from adcp.models.observation import ObservationSource
from adcp.models.run import RequestStats, RunStatus
from adcp.pipeline.service import CollectionService
from tests.support import FakeWeatherSource, build_series

pytestmark = pytest.mark.integration

#: The service's clock: 37 minutes past the hour, so "the current hour" is partial.
NOW = datetime(2026, 10, 2, 12, 37, tzinfo=UTC)
NOW_FLOOR = NOW.replace(minute=0, second=0, microsecond=0)


def storage_hours(count: int = 6) -> list[datetime]:
    """The hours a first run with a six-hour lookback should store."""
    return [NOW_FLOOR - timedelta(hours=offset) for offset in range(count, 0, -1)]


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        ingest_lookback_hours=6,
        ingest_overlap_hours=2,
        ingest_failure_budget_ratio=0.5,
        ingest_max_invalid_row_ratio=0.25,
        open_meteo_max_concurrency=1,
        open_meteo_max_attempts=1,
    )


def seed_locations(engine: Engine, *slugs: str) -> list[Location]:
    repository = LocationRepository(engine)
    created: list[Location] = []
    for index, slug in enumerate(slugs):
        record = repository.create(
            slug=slug,
            name=slug.replace("-", " ").title(),
            latitude=Decimal("44.812500") + Decimal(index) / Decimal("100"),
            longitude=Decimal("20.437500"),
        )
        created.append(Location.from_record(record))
    return created


def run_service(
    engine: Engine,
    settings: Settings,
    weather_source: FakeWeatherSource,
    *,
    stats: Any = None,
    **kwargs: Any,
) -> Any:
    service = CollectionService(
        settings,
        source=weather_source,
        engine=engine,
        stats=stats,
        now=lambda: NOW,
        clock=lambda: 0.0,
    )
    return service.run(trigger="cli", **kwargs)


def rows(engine: Engine) -> list[dict[str, object]]:
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


def error_rows(engine: Engine) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                sa.select(ingestion_run_errors).order_by(ingestion_run_errors.c.id),
            ).mappings()
        ]


def latest_watermark(engine: Engine, location_id: int, source: str = "forecast") -> datetime | None:
    record = WatermarkStore(engine).get(location_id=location_id, source=source)
    return None if record is None else record.last_observed_at


def test_first_run_ingests_the_planned_window(
    db_engine: Engine,
    settings: Settings,
) -> None:
    location = seed_locations(db_engine, "belgrade-rs")[0]
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.SUCCEEDED
    assert summary.lock_acquired is True
    assert summary.counts.locations_total == 1
    assert summary.counts.locations_succeeded == 1
    assert summary.counts.rows_received == 6
    assert summary.counts.rows_inserted == 6
    assert summary.counts.rows_updated == 0
    assert summary.counts.rows_unchanged == 0
    assert summary.counts.rows_rejected == 0
    assert summary.run_id is not None
    assert len(rows(db_engine)) == 6
    assert latest_watermark(db_engine, location.id or 0) == storage_hours()[-1]
    assert summary.results[0].watermark == storage_hours()[-1]
    assert summary.results[0].status.value == "succeeded"


def test_second_identical_run_is_idempotent(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )
    first = run_service(db_engine, settings, source)
    second = run_service(db_engine, settings, source)

    # The second run's window is the watermark minus the overlap: three hours,
    # all of which are already stored unchanged.
    assert second.counts.rows_received == 6
    assert second.counts.rows_inserted == 0
    assert second.counts.rows_updated == 0
    assert second.counts.rows_unchanged == 3
    assert second.counts.rows_skipped == 3
    assert second.status is RunStatus.SUCCEEDED
    assert first.counts.rows_inserted == 6
    assert len(rows(db_engine)) == 6, "no duplicate rows appeared"
    assert all(row["revision_count"] == 0 for row in rows(db_engine))


def test_revised_values_update_in_place(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    moments = storage_hours()
    first_source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )
    run_service(db_engine, settings, first_source)
    revised = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=moments,
            observation_overrides={moments[-1]: {"temperature_2m": Decimal("21.00")}},
        ),
    )

    summary = run_service(db_engine, settings, revised)

    assert summary.counts.rows_updated == 1
    assert summary.counts.rows_inserted == 0
    stored = [row for row in rows(db_engine) if row["observed_at"] == moments[-1]]
    assert stored[0]["temperature_2m"] == Decimal("21.00")
    assert stored[0]["revision_count"] == 1


def test_forecast_and_archive_runs_coexist(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    moments = storage_hours()
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )

    run_service(db_engine, settings, source, source=ObservationSource.FORECAST)
    run_service(db_engine, settings, source, source=ObservationSource.ARCHIVE)

    stored = rows(db_engine)
    assert len(stored) == 12
    assert {row["source"] for row in stored} == {"forecast", "archive"}


def test_domain_invalid_rows_are_rejected_and_recorded(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    moments = storage_hours()
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=moments,
            observation_overrides={moments[0]: {"temperature_2m": Decimal("842")}},
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.PARTIAL
    assert summary.counts.rows_rejected == 1
    assert summary.counts.rows_inserted == 5
    assert len(rows(db_engine)) == 5
    errors = error_rows(db_engine)
    assert len(errors) == 1
    assert errors[0]["phase"] == "validate"
    assert errors[0]["error_type"] == "OutOfRange"
    assert errors[0]["location_id"] is not None
    assert "temperature_2m" in str(errors[0]["message"])
    sample = cast(dict[str, Any], errors[0]["payload_sample"])
    assert sample["observed_at"] == moments[0].isoformat()


def test_reject_budget_failure_writes_nothing(
    db_engine: Engine,
    settings: Settings,
) -> None:
    location = seed_locations(db_engine, "belgrade-rs")[0]
    moments = storage_hours()
    broken = {moment: {"temperature_2m": Decimal("842")} for moment in moments[:3]}
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=moments,
            observation_overrides=broken,
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.FAILED
    assert summary.counts.rows_rejected == 6, "three rejected plus three withheld"
    assert summary.counts.rows_inserted == 0
    assert rows(db_engine) == []
    assert latest_watermark(db_engine, location.id or 0) is None
    assert any(row["error_type"] == "RejectionBudgetExceeded" for row in error_rows(db_engine))


def test_one_failing_location_does_not_roll_back_another(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "healthy-rs", "broken-is")
    moments = storage_hours()

    def responder(location: Location, source: ObservationSource, window: Any) -> Any:
        if location.slug == "broken-is":
            raise UpstreamServerError(
                "Open-Meteo returned a server error (HTTP 500)",
                status_code=500,
                endpoint="https://api.open-meteo.com/v1/forecast",
            )
        return build_series(location=location, source=source, moments=moments)

    summary = run_service(db_engine, settings, FakeWeatherSource(responder=responder))

    assert summary.status is RunStatus.PARTIAL
    assert summary.counts.locations_succeeded == 1
    assert summary.counts.locations_failed == 1
    stored = rows(db_engine)
    assert len(stored) == 6, "the healthy location committed its rows"
    assert {row["source"] for row in stored} == {"forecast"}
    errors = error_rows(db_engine)
    assert len(errors) == 1
    assert errors[0]["phase"] == "fetch"
    assert errors[0]["error_type"] == "UpstreamServerError"
    assert errors[0]["http_status"] == 500
    assert "apikey" not in str(errors[0]["request_url"])


def test_failure_budget_stops_the_run(
    db_engine: Engine,
    settings: Settings,
) -> None:
    # Slugs are ordered alphabetically when loaded, so the failing locations are
    # attempted first and the healthy one is never reached.
    seed_locations(db_engine, "a-broken-rs", "b-broken-rs", "c-healthy-rs")

    def responder(location: Location, source: ObservationSource, window: Any) -> Any:
        if location.slug in {"a-broken-rs", "b-broken-rs"}:
            raise UpstreamServerError("upstream is down", status_code=503)
        return build_series(location=location, source=source, moments=storage_hours())

    summary = run_service(db_engine, settings, FakeWeatherSource(responder=responder))

    assert summary.status is RunStatus.FAILED
    assert summary.counts.locations_failed == 2
    assert summary.locations_attempted == 2, "the healthy location was never attempted"
    assert summary.error_summary is not None
    assert "budget" in summary.error_summary
    # Two locations failed; the run itself also records why it stopped early.
    assert len(error_rows(db_engine)) == 3
    assert any(
        row["error_type"] == "FailureBudgetExceeded" and row["phase"] == "run"
        for row in error_rows(db_engine)
    )
    assert rows(db_engine) == [], "no observations were written"
    runs = run_rows(db_engine)
    assert runs[0]["status"] == "failed"
    assert runs[0]["locations_total"] == 3
    assert runs[0]["locations_failed"] == 2


def test_write_failure_rolls_back_rows_and_watermark(
    db_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    location = seed_locations(db_engine, "belgrade-rs")[0]
    moments = storage_hours()
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )

    def explode(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("watermark store unavailable")

    monkeypatch.setattr(WatermarkStore, "advance", explode)
    failed = run_service(db_engine, settings, source)

    assert failed.status is RunStatus.FAILED
    assert rows(db_engine) == [], "the row upsert rolled back with the watermark"
    assert latest_watermark(db_engine, location.id or 0) is None
    assert any(row["phase"] == "write" for row in error_rows(db_engine))

    monkeypatch.undo()
    recovered = run_service(db_engine, settings, source)

    assert recovered.status is RunStatus.SUCCEEDED
    assert len(rows(db_engine)) == 6
    assert latest_watermark(db_engine, location.id or 0) == moments[-1]


def test_crash_before_commit_leaves_the_window_collectable(
    db_engine: Engine,
    settings: Settings,
) -> None:
    """Rows written without a watermark commit are re-collected safely."""
    location = seed_locations(db_engine, "belgrade-rs")[0]
    moments = storage_hours()
    run_id = (
        RunTracker(db_engine)
        .start_run(
            run_type="manual",
            trigger="test",
            app_version="0.0.0",
        )
        .id
    )
    series = build_series(location=location, source=ObservationSource.FORECAST, moments=moments)
    with connection_scope(db_engine) as connection:
        WeatherRepository(db_engine).upsert_observations(
            connection,
            location_id=location.id or 0,
            run_id=run_id,
            series=series,
            observations=series.observations,
        )
        # No watermark advance: this is the "crashed before commit" shape.

    assert latest_watermark(db_engine, location.id or 0) is None
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )
    summary = run_service(db_engine, settings, source)

    assert summary.counts.rows_inserted == 0
    # Without a watermark advance the whole window is requested again, so every
    # stored row is re-observed and recognised as unchanged.
    assert summary.counts.rows_unchanged == 6
    assert len(rows(db_engine)) == 6, "the re-run did not duplicate anything"
    assert latest_watermark(db_engine, location.id or 0) == moments[-1]


def test_dry_run_writes_nothing(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source, dry_run=True)

    assert summary.dry_run is True
    assert summary.status is RunStatus.SUCCEEDED
    assert summary.counts.rows_accepted == 6
    assert summary.counts.rows_inserted == 0
    assert summary.run_id is None
    assert rows(db_engine) == []
    assert run_rows(db_engine) == []
    assert WatermarkStore(db_engine).get(location_id=1, source="forecast") is None


def test_lock_contention_skips_the_run(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    with advisory_lock(db_engine):
        summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.SKIPPED
    assert summary.lock_acquired is False
    assert summary.error_summary == "another run holds the collection lock"
    assert source.calls == []
    assert rows(db_engine) == []
    assert run_rows(db_engine) == []


def test_no_active_locations_skips_without_a_run_row(
    db_engine: Engine,
    settings: Settings,
) -> None:
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.SKIPPED
    assert summary.counts.locations_total == 0
    assert run_rows(db_engine) == []


def test_retired_locations_are_not_collected(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "active-rs", "retired-is")
    LocationRepository(db_engine).set_active("retired-is", is_active=False)
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert source.slugs() == ["active-rs"]
    assert summary.counts.locations_total == 1
    assert all(row["location_id"] is not None for row in rows(db_engine))


def test_unknown_location_slug_is_a_configuration_error(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    with pytest.raises(ConfigurationError, match="unknown or inactive"):
        run_service(db_engine, settings, source, location_slugs=["nowhere-xx"])

    assert source.calls == []
    assert run_rows(db_engine) == []


def test_summary_matches_the_persisted_run_row(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source)

    stored = run_rows(db_engine)[0]
    assert stored["id"] == summary.run_id
    assert stored["status"] == "succeeded"
    assert stored["rows_received"] == summary.counts.rows_received
    assert stored["rows_inserted"] == summary.counts.rows_inserted
    assert stored["rows_unchanged"] == summary.counts.rows_unchanged
    assert stored["locations_total"] == summary.counts.locations_total
    assert stored["locations_succeeded"] == 1
    assert stored["finished_at"] is not None
    assert stored["duration_ms"] == summary.duration_ms
    assert stored["window_from"] == storage_hours()[0]
    assert stored["window_to"] == NOW_FLOOR


def test_request_stats_deltas_are_reported(
    db_engine: Engine,
    settings: Settings,
) -> None:
    seed_locations(db_engine, "belgrade-rs")
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )
    snapshots = iter([RequestStats(0, 0), RequestStats(2, 1)])

    summary = run_service(db_engine, settings, source, stats=lambda: next(snapshots))

    assert summary.counts.requests_made == 2
    assert summary.counts.requests_retried == 1
    stored = run_rows(db_engine)[0]
    assert stored["requests_made"] == 2
    assert stored["requests_retried"] == 1


def test_referenced_location_that_vanishes_is_a_write_failure(
    db_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Referential integrity is enforced by the database, not by hope."""
    location = seed_locations(db_engine, "belgrade-rs")[0]
    with connection_scope(db_engine) as connection:
        connection.execute(
            sa.text("DELETE FROM locations WHERE id = :id"),
            {"id": location.id},
        )
    ghost = Location(
        slug=location.slug,
        name=location.name,
        latitude=location.latitude,
        longitude=location.longitude,
        id=location.id,
    )
    now = datetime.now(UTC)
    ghost_record = LocationRecord(
        id=location.id or 0,
        slug=ghost.slug,
        name=ghost.name,
        latitude=ghost.latitude,
        longitude=ghost.longitude,
        timezone="UTC",
        country_code=None,
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    monkeypatch.setattr(
        LocationRepository,
        "list_active",
        lambda _self: [ghost_record],
    )
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(
            location=loc,
            source=src,
            moments=storage_hours(),
        ),
    )

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.FAILED
    assert rows(db_engine) == []
    assert any(row["phase"] == "write" for row in error_rows(db_engine))


def test_a_write_failure_mid_run_keeps_committed_locations(
    db_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F12: a database error for one location must not undo another's commit."""
    seed_locations(db_engine, "a-healthy-rs", "b-broken-rs")
    moments = storage_hours()
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )
    original = WeatherRepository.upsert_observations
    calls = itertools.count()

    def flaky(self: WeatherRepository, connection: Any, **kwargs: Any) -> Any:
        if next(calls) == 1:
            msg = "simulated write failure (deadlock, disk full, connection lost)"
            raise OperationalError(msg, None, Exception(msg))
        return original(self, connection, **kwargs)

    monkeypatch.setattr(WeatherRepository, "upsert_observations", flaky)

    summary = run_service(db_engine, settings, source)

    assert summary.status is RunStatus.PARTIAL
    assert summary.counts.locations_succeeded == 1
    assert summary.counts.locations_failed == 1
    assert len(rows(db_engine)) == len(moments), "the healthy location committed"
    writes = [row for row in error_rows(db_engine) if row["phase"] == "write"]
    assert len(writes) == 1
    assert writes[0]["error_type"] == "OperationalError"


def test_the_run_stops_at_its_wall_clock_budget(
    db_engine: Engine,
    settings: Settings,
) -> None:
    """F14: the run deadline stops new work; completed locations stay committed."""
    seed_locations(db_engine, "first-rs", "second-rs", "third-rs")
    moments = storage_hours()
    source = FakeWeatherSource(
        responder=lambda loc, src, _win: build_series(location=loc, source=src, moments=moments),
    )
    ticking = itertools.count(0, 5)  # every read jumps five seconds
    service = CollectionService(
        settings.with_overrides(run_timeout_s=1, open_meteo_max_concurrency=1),
        source=source,
        engine=db_engine,
        now=lambda: NOW,
        clock=ticking.__next__,
    )

    summary = service.run(trigger="cli")

    assert summary.status is RunStatus.FAILED
    assert summary.locations_attempted == 1, "the deadline stopped further submissions"
    assert summary.error_summary is not None
    assert "run_timeout_exceeded" in summary.error_summary
    assert len(rows(db_engine)) == len(moments), "the attempted location committed"
    stops = [row for row in error_rows(db_engine) if row["error_code"] == "run_timeout_exceeded"]
    assert len(stops) == 1
    assert stops[0]["error_type"] == "RunTimeoutExceeded"
    assert stops[0]["phase"] == "run"
