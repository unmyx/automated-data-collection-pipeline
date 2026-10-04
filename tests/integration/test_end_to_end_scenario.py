"""One deterministic scenario proving the pipeline's core guarantees.

Everything here is the real stack - `run_collection_once` (engine, Open-Meteo
adapter, collection service, repository) against real PostgreSQL - with only the
HTTP transport scripted. The six runs walk the guarantees the project claims:

Run 1  three locations ingest cleanly
Run 2  the same observations again: no duplicates, unchanged rows are no-ops
Run 3  revised data updates exactly the affected rows
Run 4  one location fails and the others still commit (partial run)
Run 5  a transient failure is retried by the client and the run succeeds
Run 6  a crash before the watermark commit is safely reprocessed
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
import respx
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from adcp.cli.collect_cmd import exit_code_for, run_collection_once
from adcp.config import Settings
from adcp.db.engine import connection_scope
from adcp.db.repository import LocationRepository, WeatherRepository
from adcp.db.run_tracker import RunTracker
from adcp.db.tables import ingestion_run_errors, ingestion_runs, weather_hourly
from adcp.db.watermark_store import WatermarkStore
from adcp.exit_codes import ExitCode
from adcp.models.location import Location
from adcp.models.observation import ObservationSource
from adcp.models.run import RunStatus, RunSummary
from adcp.pipeline.service import CollectionService
from tests.support import build_series, payload_for_recent_hours

pytestmark = pytest.mark.integration

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
LOOKBACK_HOURS = 3
# One hour of overlap less than the lookback makes run 2 request the *same* window
# that run 1 stored, which is what makes the no-op assertion exact.
OVERLAP_HOURS = 2

ALPHA = ("alpha-rs", Decimal("44.812500"))
BRAVO = ("bravo-is", Decimal("64.146600"))
CHARLIE = ("charlie-ar", Decimal("-54.801900"))
LOCATIONS = (ALPHA, BRAVO, CHARLIE)


def settings_for(database_url: str, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": database_url,
        "ingest_lookback_hours": LOOKBACK_HOURS,
        "ingest_overlap_hours": OVERLAP_HOURS,
        "open_meteo_max_attempts": 1,
        "open_meteo_backoff_initial_s": 0.001,
        "open_meteo_backoff_max_s": 0.01,
        "open_meteo_max_concurrency": 2,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


def seed_locations(engine: Engine) -> dict[str, int]:
    repository = LocationRepository(engine)
    ids: dict[str, int] = {}
    for slug, latitude in LOCATIONS:
        record = repository.create(
            slug=slug,
            name=slug.replace("-", " ").title(),
            latitude=latitude,
            longitude=Decimal("20.437500"),
        )
        ids[slug] = record.id
    return ids


def latitude_of(request: httpx.Request) -> str:
    return request.url.params["latitude"]


def run_once(
    settings: Settings,
    responder: Any,
) -> tuple[RunSummary, respx.Route]:
    """Run one collection through the real stack, with scripted HTTP."""
    with respx.mock(assert_all_called=False) as router:
        route = router.get(FORECAST_URL).mock(side_effect=responder)
        summary = run_collection_once(settings, trigger="cli")
    return summary, route


def always_ok(revisions: dict[int, dict[str, object]] | None = None) -> Any:
    payload = payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS, overrides=revisions)

    def responder(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    return responder


def rows_for(engine: Engine, slug: str) -> list[dict[str, object]]:
    statement = (
        sa.select(weather_hourly)
        .where(weather_hourly.c.location_id == location_id(engine, slug))
        .order_by(weather_hourly.c.observed_at)
    )
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(statement).mappings()]


def location_id(engine: Engine, slug: str) -> int:
    repository = LocationRepository(engine)
    record = repository.get_by_slug(slug)
    assert record is not None, f"{slug} should be registered"
    assert record.id is not None
    return record.id


def watermark(engine: Engine, slug: str) -> datetime | None:
    record = WatermarkStore(engine).get(location_id=location_id(engine, slug), source="forecast")
    return None if record is None else record.last_observed_at


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


# The scenario is long on purpose: six labelled runs, each asserting one guarantee.
def test_six_run_scenario(cli_database_env: str, db_engine: Engine) -> None:  # noqa: PLR0915
    ids = seed_locations(db_engine)
    settings = settings_for(cli_database_env)

    # -- Run 1: three locations ingest cleanly --------------------------------
    first, _ = run_once(settings, always_ok())

    assert first.status is RunStatus.SUCCEEDED
    assert first.counts.locations_total == 3
    assert first.counts.locations_succeeded == 3
    assert first.counts.rows_received == 3 * LOOKBACK_HOURS
    assert first.counts.rows_inserted == 3 * LOOKBACK_HOURS
    assert first.counts.rows_updated == 0
    assert first.counts.rows_rejected == 0
    assert exit_code_for(first) is ExitCode.OK
    for slug, _ in LOCATIONS:
        assert len(rows_for(db_engine, slug)) == LOOKBACK_HOURS
        assert watermark(db_engine, slug) is not None
    assert error_rows(db_engine) == []

    # -- Run 2: the same observations, no duplicates, pure no-ops --------------
    before = {slug: rows_for(db_engine, slug) for slug, _ in LOCATIONS}
    second, _ = run_once(settings, always_ok())
    after = {slug: rows_for(db_engine, slug) for slug, _ in LOCATIONS}

    assert second.status is RunStatus.SUCCEEDED
    assert second.counts.rows_received == 3 * LOOKBACK_HOURS
    assert second.counts.rows_inserted == 0
    assert second.counts.rows_updated == 0
    assert second.counts.rows_unchanged == 3 * LOOKBACK_HOURS
    assert after == before, "unchanged rows are not rewritten at all"
    assert all(row["revision_count"] == 0 for rows in after.values() for row in rows)

    # -- Run 3: revised data updates exactly the affected rows ----------------
    revised: dict[int, dict[str, object]] = {LOOKBACK_HOURS - 1: {"temperature_2m": 21.5}}
    third, _ = run_once(settings, always_ok(revisions=revised))

    assert third.status is RunStatus.SUCCEEDED
    assert third.counts.rows_inserted == 0
    assert third.counts.rows_updated == 3, "one revised hour per location"
    assert third.counts.rows_unchanged == 3 * LOOKBACK_HOURS - 3
    for slug, _ in LOCATIONS:
        newest = rows_for(db_engine, slug)[-1]
        assert newest["temperature_2m"] == Decimal("21.50")
        assert newest["revision_count"] == 1
        assert newest["first_seen_run_id"] == first.run_id, "discovery is preserved"

    # -- Run 4: one location fails, the others commit -------------------------
    def fail_bravo(request: httpx.Request) -> httpx.Response:
        if Decimal(latitude_of(request)) == BRAVO[1]:
            return httpx.Response(503, text="temporarily down")
        return httpx.Response(200, json=payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS))

    fourth, _ = run_once(settings, fail_bravo)

    assert fourth.status is RunStatus.PARTIAL
    assert exit_code_for(fourth) is ExitCode.PARTIAL
    assert fourth.counts.locations_failed == 1
    assert fourth.counts.locations_succeeded == 2
    assert len(rows_for(db_engine, "alpha-rs")) == LOOKBACK_HOURS
    assert len(rows_for(db_engine, "bravo-is")) == LOOKBACK_HOURS, "its earlier rows survive"
    failures = [row for row in error_rows(db_engine) if row["error_type"] == "UpstreamServerError"]
    assert failures, "the failure is recorded with its cause"
    assert all(row["phase"] == "fetch" for row in failures)
    assert {cast(int, row["location_id"]) for row in failures} == {ids["bravo-is"]}

    # -- Run 5: a transient failure is retried and the run succeeds -----------
    attempts = itertools.count()

    def flaky_bravo(request: httpx.Request) -> httpx.Response:
        if Decimal(latitude_of(request)) == BRAVO[1] and next(attempts) == 0:
            return httpx.Response(503, text="temporarily down")
        return httpx.Response(200, json=payload_for_recent_hours(lookback_hours=LOOKBACK_HOURS))

    retrying_settings = settings.with_overrides(open_meteo_max_attempts=2)
    fifth, route = run_once(retrying_settings, flaky_bravo)

    assert fifth.status is RunStatus.SUCCEEDED
    assert fifth.counts.locations_failed == 0
    assert fifth.counts.requests_retried >= 1, "the retry is counted for the run"
    bravo_calls = [call for call in route.calls if Decimal(latitude_of(call.request)) == BRAVO[1]]
    assert len(bravo_calls) == 2, "one failure, one successful retry"

    # -- Run 6: a crash before the watermark commit is reprocessed ------------
    # A location registered after the earlier runs has no watermark yet; a crash
    # that wrote its rows without committing the cursor is exactly what Run 6
    # reprocesses.
    delta_id = (
        LocationRepository(db_engine)
        .create(
            slug="delta-us",
            name="Delta",
            latitude=Decimal("38.907200"),
            longitude=Decimal("-77.036900"),
        )
        .id
    )
    crashed_run = (
        RunTracker(db_engine)
        .start_run(
            run_type="manual",
            trigger="test",
            app_version="0.0.0",
        )
        .id
    )
    delta_record = LocationRepository(db_engine).get_by_slug("delta-us")
    assert delta_record is not None
    moments = [
        datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=offset)
        for offset in range(LOOKBACK_HOURS, 0, -1)
    ]
    series = build_series(
        location=Location.from_record(delta_record),
        source=ObservationSource.FORECAST,
        moments=moments,
    )
    with connection_scope(db_engine) as connection:
        WeatherRepository(db_engine).upsert_observations(
            connection,
            location_id=delta_id,
            run_id=crashed_run,
            series=series,
            observations=series.observations,
        )
        # No watermark advance: exactly the shape of a crash before commit.

    assert watermark(db_engine, "delta-us") is None, "the crashed run left no cursor"
    seventh_settings = settings_for(cli_database_env)
    sixth, _ = run_once(seventh_settings, always_ok())

    assert sixth.status is RunStatus.SUCCEEDED
    assert sixth.counts.locations_total == 4
    stored = rows_for(db_engine, "delta-us")
    moments_seen = [row["observed_at"] for row in stored]
    assert len(moments_seen) == len(set(moments_seen)), "the reprocessing did not duplicate"
    assert len(stored) == LOOKBACK_HOURS, "the crashed rows were adopted, not re-inserted"
    assert sixth.counts.rows_inserted == 0
    assert watermark(db_engine, "delta-us") == stored[-1]["observed_at"], "the cursor caught up"
    crashed = [row for row in run_rows(db_engine) if row["id"] == crashed_run]
    assert crashed[0]["status"] == "running", "the abandoned run is left for the reaper"


def test_many_locations_commit_independently(cli_database_env: str, db_engine: Engine) -> None:
    """Bounded concurrency at a larger scale, with every location accounted for."""
    repository = LocationRepository(db_engine)
    database_url = cli_database_env
    settings = settings_for(database_url, open_meteo_max_concurrency=4)
    slugs = [f"scale-{index:02d}" for index in range(20)]
    for index, slug in enumerate(slugs):
        repository.create(
            slug=slug,
            name=slug,
            latitude=Decimal("40.000000") + Decimal(index) / Decimal("100"),
            longitude=Decimal("20.000000"),
        )
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    moments = [end - timedelta(hours=offset) for offset in (2, 1)]
    in_flight = 0
    peak = 0
    observed: list[str] = []

    class Source:
        # ARG002: the port requires the parameter name even though it is unused.
        def fetch_hourly(self, *, location: Any, source: Any, window: Any) -> Any:  # noqa: ARG002
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                observed.append(location.slug)
                return build_series(location=location, source=source, moments=moments)
            finally:
                in_flight -= 1

    service = CollectionService(settings, source=Source(), engine=db_engine, clock=lambda: 0.0)

    summary = service.run(trigger="cli")

    assert summary.status is RunStatus.SUCCEEDED
    assert summary.counts.locations_total == 20
    assert summary.counts.locations_succeeded == 20
    assert summary.counts.rows_inserted == 20 * len(moments)
    assert peak <= 4, "concurrency stays inside the configured bound"
    assert sorted(observed) == slugs
    stored = sum(len(rows_for(db_engine, slug)) for slug in slugs)
    assert stored == 20 * len(moments)
