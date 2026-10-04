"""Location and weather repositories against real PostgreSQL."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from adcp.db.repository import LocationRepository
from adcp.db.run_tracker import RunTracker
from adcp.db.tables import ingestion_runs, ingestion_watermarks, locations
from adcp.db.watermark_store import WatermarkStore
from adcp.errors import LocationNotFoundError

pytestmark = pytest.mark.integration

BELGRADE: dict[str, Any] = {
    "slug": "belgrade-rs",
    "name": "Belgrade",
    "latitude": Decimal("44.812500"),
    "longitude": Decimal("20.437500"),
    "country_code": "RS",
}


def test_location_round_trip(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)

    created = repository.create(**BELGRADE)

    assert created.id > 0
    assert created.slug == "belgrade-rs"
    assert created.timezone == "UTC"
    assert created.is_active is True
    assert created.created_at.tzinfo is not None
    assert repository.get_by_slug("belgrade-rs") == created
    assert repository.get_by_slug("nowhere") is None
    assert repository.list_all() == [created]
    assert repository.list_active() == [created]


def test_retired_locations_are_skipped_but_not_deleted(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)
    repository.create(**BELGRADE)
    repository.create(
        **(
            BELGRADE
            | {
                "slug": "reykjavik-is",
                "name": "Reykjavik",
                "latitude": Decimal("64.146600"),
                "longitude": Decimal("-21.942600"),
                "country_code": "IS",
            }
        ),
    )

    retired = repository.set_active("belgrade-rs", is_active=False)

    assert retired.is_active is False
    assert [location.slug for location in repository.list_active()] == ["reykjavik-is"]
    assert len(repository.list_all()) == 2, "history keeps the retired location"

    reactivated = repository.set_active("belgrade-rs", is_active=True)
    assert reactivated.is_active is True


def test_location_repository_surfaces_database_constraints(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)
    repository.create(**BELGRADE)

    with pytest.raises(IntegrityError):
        repository.create(**BELGRADE)

    with pytest.raises(IntegrityError):
        repository.create(**(BELGRADE | {"slug": "reykjavik-is", "latitude": Decimal("95")}))


def test_set_active_rejects_an_unknown_slug(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)

    with pytest.raises(LocationNotFoundError, match="nowhere"):
        repository.set_active("nowhere", is_active=False)


def test_start_run_creates_a_running_row(db_engine: Engine) -> None:
    tracker = RunTracker(db_engine)
    window_from = datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    window_to = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)

    run = tracker.start_run(
        run_type="scheduled",
        trigger="cli",
        app_version="0.1.0",
        window_from=window_from,
        window_to=window_to,
        hostname="test-host",
    )

    assert isinstance(run.id, uuid.UUID)
    assert run.status == "running"
    assert run.run_type == "scheduled"
    assert run.window_from == window_from
    assert run.window_to == window_to
    assert run.finished_at is None
    assert (run.locations_total, run.rows_inserted, run.error_count) == (0, 0, 0)


def test_list_recent_returns_newest_first_and_honours_the_limit(db_engine: Engine) -> None:
    now = datetime.now(UTC)
    with db_engine.begin() as connection:
        for offset, label in ((2, "oldest"), (0, "newest"), (1, "middle")):
            connection.execute(
                ingestion_runs.insert().values(
                    run_type=label,
                    trigger="cli",
                    app_version="0.1.0",
                    started_at=now - timedelta(hours=offset),
                ),
            )

    recent = RunTracker(db_engine).list_recent(limit=2)

    assert [run.run_type for run in recent] == ["newest", "middle"]


def test_watermark_lookup_returns_none_until_a_row_exists(db_engine: Engine) -> None:
    store = WatermarkStore(db_engine)

    assert store.get(location_id=1, source="forecast") is None

    run_id = (
        RunTracker(db_engine)
        .start_run(
            run_type="scheduled",
            trigger="cli",
            app_version="0.1.0",
        )
        .id
    )
    with db_engine.begin() as connection:
        location_id = connection.execute(
            locations.insert()
            .values(
                slug="belgrade-rs",
                name="Belgrade",
                latitude=Decimal("44.812500"),
                longitude=Decimal("20.437500"),
            )
            .returning(locations.c.id),
        ).scalar_one()
        connection.execute(
            ingestion_watermarks.insert().values(
                location_id=int(location_id),
                source="forecast",
                last_observed_at=datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
                last_run_id=run_id,
            ),
        )

    watermark = store.get(location_id=int(location_id), source="forecast")

    assert watermark is not None
    assert watermark.last_observed_at == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert watermark.last_run_id == run_id
    assert store.get(location_id=int(location_id), source="archive") is None
