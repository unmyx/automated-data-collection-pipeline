"""The idempotent write path, proved against real PostgreSQL.

These tests exercise the statement rather than a mock: the natural key, the
``ON CONFLICT ... WHERE row_hash IS DISTINCT FROM`` predicate, the
``RETURNING (xmax = 0)`` insert/update discriminator, and the batch chunking.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from adcp.db.engine import connection_scope
from adcp.db.repository import (
    UPSERT_CHUNK_SIZE,
    LocationRepository,
    UpsertCounts,
    WeatherRepository,
)
from adcp.db.run_tracker import RunTracker
from adcp.db.tables import weather_hourly
from adcp.models.location import Location
from adcp.models.observation import ObservationSource, WeatherObservation, WeatherSeries
from tests.support import build_series

pytestmark = pytest.mark.integration

HOUR = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def seeded(db_engine: Engine) -> tuple[int, uuid.UUID, Location]:
    """A location row, a run row, and the matching domain location."""
    record = LocationRepository(db_engine).create(
        slug="belgrade-rs",
        name="Belgrade",
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
        country_code="RS",
    )
    run = RunTracker(db_engine).start_run(
        run_type="manual",
        trigger="test",
        app_version="0.0.0",
    )
    return record.id, run.id, Location.from_record(record)


def new_run(engine: Engine) -> uuid.UUID:
    return (
        RunTracker(engine)
        .start_run(
            run_type="manual",
            trigger="test",
            app_version="0.0.0",
        )
        .id
    )


def hours(count: int, *, start: datetime = HOUR) -> list[datetime]:
    return [start + timedelta(hours=offset) for offset in range(count)]


def series_for(
    location: Location,
    moments: list[datetime],
    *,
    source: ObservationSource = ObservationSource.FORECAST,
    overrides: dict[datetime, dict[str, object]] | None = None,
) -> WeatherSeries:
    return build_series(
        location=location,
        source=source,
        moments=moments,
        observation_overrides=overrides,
    )


def upsert(
    engine: Engine,
    *,
    location_id: int,
    run_id: uuid.UUID,
    series: WeatherSeries,
    observations: tuple[WeatherObservation, ...] | None = None,
) -> UpsertCounts:
    """Run the upsert in its own transaction, as the pipeline does per location."""
    rows = series.observations if observations is None else observations
    with connection_scope(engine) as connection:
        return WeatherRepository(engine).upsert_observations(
            connection,
            location_id=location_id,
            run_id=run_id,
            series=series,
            observations=rows,
        )


def fetch_rows(engine: Engine, location_id: int) -> list[dict[str, object]]:
    statement = (
        sa.select(weather_hourly)
        .where(weather_hourly.c.location_id == location_id)
        .order_by(weather_hourly.c.observed_at)
    )
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(statement).mappings()]


def test_first_write_inserts_every_row(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, run_id, location = seeded
    series = series_for(location, hours(3))

    counts = upsert(db_engine, location_id=location_id, run_id=run_id, series=series)

    assert counts.received == 3
    assert counts.inserted == 3
    assert counts.updated == 0
    assert counts.unchanged == 0
    assert counts.written == 3
    rows = fetch_rows(db_engine, location_id)
    assert len(rows) == 3
    assert all(row["row_hash"] for row in rows)
    assert all(row["first_seen_run_id"] == run_id for row in rows)
    assert all(row["last_seen_run_id"] == run_id for row in rows)
    assert all(row["revision_count"] == 0 for row in rows)
    assert rows[0]["temperature_2m"] == Decimal("12.50")
    assert rows[0]["source"] == "forecast"


def test_identical_rerun_is_a_true_storage_no_op(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, first_run, location = seeded
    series = series_for(location, hours(3))
    upsert(db_engine, location_id=location_id, run_id=first_run, series=series)
    before = fetch_rows(db_engine, location_id)

    counts = upsert(
        db_engine,
        location_id=location_id,
        run_id=new_run(db_engine),
        series=series,
    )

    assert (counts.inserted, counts.updated, counts.unchanged) == (0, 0, 3)
    after = fetch_rows(db_engine, location_id)
    assert after == before, "unchanged rows are not rewritten at all"
    assert all(row["revision_count"] == 0 for row in after)
    assert all(row["last_seen_run_id"] == first_run for row in after)


def test_changed_value_updates_the_row_in_place(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, first_run, location = seeded
    moments = hours(3)
    upsert(
        db_engine,
        location_id=location_id,
        run_id=first_run,
        series=series_for(location, moments),
    )
    second_run = new_run(db_engine)
    revised = series_for(
        location,
        moments,
        overrides={HOUR: {"temperature_2m": Decimal("19.75")}},
    )

    counts = upsert(db_engine, location_id=location_id, run_id=second_run, series=revised)

    assert (counts.inserted, counts.updated, counts.unchanged) == (0, 1, 2)
    rows = fetch_rows(db_engine, location_id)
    assert rows[0]["temperature_2m"] == Decimal("19.75")
    assert rows[0]["revision_count"] == 1
    assert rows[0]["first_seen_run_id"] == first_run, "discovery is preserved"
    assert rows[0]["last_seen_run_id"] == second_run
    assert cast(datetime, rows[0]["first_collected_at"]) < cast(
        datetime,
        rows[0]["last_collected_at"],
    )


def test_unquantised_values_hash_consistently(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    """Storage rounds to the column scale; the hash must round the same way."""
    location_id, first_run, location = seeded
    series = series_for(
        location,
        [HOUR],
        overrides={HOUR: {"temperature_2m": Decimal("18.334")}},
    )
    upsert(db_engine, location_id=location_id, run_id=first_run, series=series)

    counts = upsert(
        db_engine,
        location_id=location_id,
        run_id=new_run(db_engine),
        series=series,
    )

    assert (counts.inserted, counts.updated, counts.unchanged) == (0, 0, 1)
    assert fetch_rows(db_engine, location_id)[0]["temperature_2m"] == Decimal("18.33")


def test_forecast_and_archive_rows_coexist_for_the_same_hour(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, run_id, location = seeded
    forecast = series_for(location, [HOUR], source=ObservationSource.FORECAST)
    archive = series_for(
        location,
        [HOUR],
        source=ObservationSource.ARCHIVE,
        overrides={HOUR: {"temperature_2m": Decimal("11.25")}},
    )

    first = upsert(db_engine, location_id=location_id, run_id=run_id, series=forecast)
    second = upsert(db_engine, location_id=location_id, run_id=run_id, series=archive)

    assert (first.inserted, second.inserted) == (1, 1)
    rows = fetch_rows(db_engine, location_id)
    assert {row["source"] for row in rows} == {"forecast", "archive"}
    repository = WeatherRepository(db_engine)
    assert repository.count_for(location_id=location_id, source="forecast") == 1
    assert repository.count_for(location_id=location_id, source=ObservationSource.ARCHIVE) == 1


def test_empty_batch_is_a_no_op(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, run_id, location = seeded

    counts = upsert(
        db_engine,
        location_id=location_id,
        run_id=run_id,
        series=series_for(location, []),
    )

    assert (counts.received, counts.inserted, counts.updated, counts.unchanged) == (0, 0, 0, 0)
    assert fetch_rows(db_engine, location_id) == []


def test_duplicate_timestamps_in_one_batch_are_refused_before_sql(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, run_id, location = seeded
    series = series_for(location, [HOUR, HOUR])

    with pytest.raises(ValueError, match="duplicate observed_at"):
        upsert(db_engine, location_id=location_id, run_id=run_id, series=series)


def test_batches_larger_than_one_chunk_are_inserted(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    location_id, run_id, location = seeded
    series = series_for(location, hours(UPSERT_CHUNK_SIZE + 10))

    counts = upsert(db_engine, location_id=location_id, run_id=run_id, series=series)

    assert counts.inserted == UPSERT_CHUNK_SIZE + 10
    assert (
        WeatherRepository(db_engine).count_for(
            location_id=location_id,
            source=ObservationSource.FORECAST,
        )
        == UPSERT_CHUNK_SIZE + 10
    )


def test_unknown_location_id_fails_the_foreign_key(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID, Location],
) -> None:
    _, run_id, location = seeded
    series = series_for(location, [HOUR])

    with pytest.raises(IntegrityError):
        upsert(db_engine, location_id=999_999, run_id=run_id, series=series)
