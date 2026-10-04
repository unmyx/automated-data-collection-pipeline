"""The database itself is the last line of defence for the plan's invariants."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from adcp.db.tables import (
    ingestion_run_errors,
    ingestion_runs,
    locations,
    weather_hourly,
)

pytestmark = pytest.mark.integration

HOUR = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def sqlstate(exc: IntegrityError) -> str | None:
    """SQLSTATE of the driver error (23505 unique, 23514 check, 23503 foreign key)."""
    return getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)


def constraint_name(exc: IntegrityError) -> str | None:
    """Name of the constraint PostgreSQL rejected, when it reports one."""
    diagnostics = getattr(exc.orig, "diag", None)
    return getattr(diagnostics, "constraint_name", None)


def insert_location(connection: Connection, **overrides: object) -> int:
    values: dict[str, object] = {
        "slug": "belgrade-rs",
        "name": "Belgrade",
        "latitude": Decimal("44.812500"),
        "longitude": Decimal("20.437500"),
        "country_code": "RS",
    }
    values.update(overrides)
    statement = locations.insert().values(**values).returning(locations.c.id)
    return int(connection.execute(statement).scalar_one())


def insert_run(connection: Connection, **overrides: object) -> uuid.UUID:
    values: dict[str, object] = {
        "run_type": "scheduled",
        "trigger": "cli",
        "app_version": "0.1.0",
    }
    values.update(overrides)
    statement = ingestion_runs.insert().values(**values).returning(ingestion_runs.c.id)
    return uuid.UUID(str(connection.execute(statement).scalar_one()))


def insert_observation(
    connection: Connection,
    *,
    location_id: int,
    run_id: uuid.UUID,
    **overrides: object,
) -> None:
    values: dict[str, object] = {
        "location_id": location_id,
        "observed_at": HOUR,
        "source": "forecast",
        "temperature_2m": Decimal("17.40"),
        "row_hash": "hash-1",
        "upstream_latitude": Decimal("44.812500"),
        "upstream_longitude": Decimal("20.437500"),
        "first_seen_run_id": run_id,
        "last_seen_run_id": run_id,
    }
    values.update(overrides)
    connection.execute(weather_hourly.insert().values(**values))


def test_server_defaults_are_applied(db_engine: Engine) -> None:
    with db_engine.begin() as connection:
        location_id = insert_location(connection, country_code=None)
        row = (
            connection.execute(
                sa.select(locations).where(locations.c.id == location_id),
            )
            .mappings()
            .one()
        )
        run_id = insert_run(connection)
        run = (
            connection.execute(
                sa.select(ingestion_runs).where(ingestion_runs.c.id == run_id),
            )
            .mappings()
            .one()
        )

    assert row["timezone"] == "UTC"
    assert row["is_active"] is True
    assert row["created_at"] is not None
    assert row["country_code"] is None
    assert run["status"] == "running"
    assert run["locations_total"] == 0
    assert run["started_at"] is not None
    assert run["finished_at"] is None


@pytest.mark.parametrize(
    ("overrides", "expected_state", "expected_constraint"),
    [
        ({"latitude": Decimal("91.000000")}, "23514", "locations_lat_range"),
        ({"longitude": Decimal("-180.500000")}, "23514", "locations_lon_range"),
        ({"slug": "Not A Slug"}, "23514", "locations_slug_format"),
    ],
)
def test_location_checks_reject_bad_values(
    db_engine: Engine,
    overrides: dict[str, object],
    expected_state: str,
    expected_constraint: str,
) -> None:
    with pytest.raises(IntegrityError) as excinfo, db_engine.begin() as connection:
        insert_location(connection, **overrides)

    assert sqlstate(excinfo.value) == expected_state
    assert constraint_name(excinfo.value) == expected_constraint


def test_duplicate_slug_is_rejected(db_engine: Engine) -> None:
    with db_engine.begin() as connection:
        insert_location(connection)
        with pytest.raises(IntegrityError) as excinfo:
            insert_location(connection, name="Belgrade duplicate")

    assert sqlstate(excinfo.value) == "23505"
    assert constraint_name(excinfo.value) == "locations_slug_key"


@pytest.mark.parametrize(
    ("overrides", "expected_state", "expected_constraint"),
    [
        (
            {"observed_at": HOUR + timedelta(minutes=30)},
            "23514",
            "weather_hourly_hour_aligned",
        ),
        ({"source": "radiosonde"}, "23514", "weather_hourly_source_valid"),
    ],
)
def test_weather_hourly_checks_reject_bad_rows(
    db_engine: Engine,
    overrides: dict[str, object],
    expected_state: str,
    expected_constraint: str,
) -> None:
    with db_engine.begin() as connection:
        location_id = insert_location(connection)
        run_id = insert_run(connection)
        with pytest.raises(IntegrityError) as excinfo:
            insert_observation(connection, location_id=location_id, run_id=run_id, **overrides)

    assert sqlstate(excinfo.value) == expected_state
    assert constraint_name(excinfo.value) == expected_constraint


def test_natural_key_makes_duplicate_observations_impossible(db_engine: Engine) -> None:
    """The idempotency anchor: (location_id, observed_at, source)."""
    with db_engine.begin() as connection:
        location_id = insert_location(connection)
        run_id = insert_run(connection)
        insert_observation(connection, location_id=location_id, run_id=run_id)

        with pytest.raises(IntegrityError) as excinfo:
            insert_observation(
                connection,
                location_id=location_id,
                run_id=run_id,
                temperature_2m=Decimal("18.00"),
            )

    assert sqlstate(excinfo.value) == "23505"
    assert constraint_name(excinfo.value) == "weather_hourly_natural_key"


def test_same_hour_from_a_different_source_is_a_different_fact(db_engine: Engine) -> None:
    with db_engine.begin() as connection:
        location_id = insert_location(connection)
        run_id = insert_run(connection)
        insert_observation(connection, location_id=location_id, run_id=run_id)
        insert_observation(
            connection,
            location_id=location_id,
            run_id=run_id,
            source="archive",
            row_hash="hash-2",
        )
        count = connection.execute(
            sa.select(sa.func.count()).select_from(weather_hourly),
        ).scalar_one()

    assert count == 2


def test_observation_requires_an_existing_location_and_run(db_engine: Engine) -> None:
    with db_engine.begin() as connection:
        with pytest.raises(IntegrityError) as location_error:
            insert_observation(connection, location_id=999_999, run_id=uuid.uuid4())
        # PostgreSQL reports whichever foreign key it checks first; the SQLSTATE is
        # the stable part of the contract.
        assert sqlstate(location_error.value) == "23503"


def test_run_finished_before_started_is_rejected(db_engine: Engine) -> None:
    with pytest.raises(IntegrityError) as excinfo, db_engine.begin() as connection:
        insert_run(
            connection,
            finished_at=datetime.now(UTC) - timedelta(hours=1),
        )

    assert sqlstate(excinfo.value) == "23514"
    assert constraint_name(excinfo.value) == "ingestion_runs_finished_after_started"


def test_foreign_key_actions_match_the_plan(db_engine: Engine) -> None:
    with db_engine.begin() as connection:
        location_id = insert_location(connection)
        run_id = insert_run(connection)
        insert_observation(connection, location_id=location_id, run_id=run_id)
        connection.execute(
            ingestion_run_errors.insert().values(
                run_id=run_id,
                location_id=location_id,
                phase="fetch",
                error_type="ReadTimeout",
                message="upstream timed out",
            ),
        )

    # Deleting the run cascades to its errors and is blocked by the observation FK,
    # which is the point: the audit anchor cannot vanish while data references it.
    with pytest.raises(IntegrityError) as excinfo, db_engine.begin() as connection:
        connection.execute(ingestion_runs.delete().where(ingestion_runs.c.id == run_id))
    assert sqlstate(excinfo.value) == "23503"

    with db_engine.begin() as connection:
        connection.execute(weather_hourly.delete())
        connection.execute(ingestion_runs.delete().where(ingestion_runs.c.id == run_id))
        errors_after_run_delete = connection.execute(
            sa.select(sa.func.count()).select_from(ingestion_run_errors),
        ).scalar_one()
        assert errors_after_run_delete == 0, "run errors cascade with their run"

        connection.execute(locations.delete().where(locations.c.id == location_id))
