"""Watermarks: monotonic advancement inside the caller's transaction."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.engine import Engine

from adcp.db.engine import connection_scope
from adcp.db.repository import LocationRepository
from adcp.db.run_tracker import RunTracker
from adcp.db.watermark_store import WatermarkStore
from adcp.models.observation import ObservationSource

pytestmark = pytest.mark.integration

HOUR = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def seeded(db_engine: Engine) -> tuple[int, uuid.UUID]:
    record = LocationRepository(db_engine).create(
        slug="belgrade-rs",
        name="Belgrade",
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
    )
    run = RunTracker(db_engine).start_run(
        run_type="manual",
        trigger="test",
        app_version="0.0.0",
    )
    return record.id, run.id


def test_first_advance_creates_the_row(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID],
) -> None:
    location_id, run_id = seeded
    store = WatermarkStore(db_engine)
    assert store.get(location_id=location_id, source="forecast") is None

    with connection_scope(db_engine) as connection:
        store.advance(
            connection,
            location_id=location_id,
            source=ObservationSource.FORECAST,
            last_observed_at=HOUR,
            run_id=run_id,
        )

    stored = store.get(location_id=location_id, source="forecast")
    assert stored is not None
    assert stored.last_observed_at == HOUR
    assert stored.last_run_id == run_id


def test_advance_is_monotonic(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID],
) -> None:
    location_id, run_id = seeded
    store = WatermarkStore(db_engine)

    with connection_scope(db_engine) as connection:
        store.advance(
            connection,
            location_id=location_id,
            source="forecast",
            last_observed_at=HOUR,
            run_id=run_id,
        )
        store.advance(
            connection,
            location_id=location_id,
            source="forecast",
            last_observed_at=HOUR + timedelta(hours=5),
            run_id=run_id,
        )
        # A late-committing batch must never move the cursor backwards.
        store.advance(
            connection,
            location_id=location_id,
            source="forecast",
            last_observed_at=HOUR - timedelta(hours=10),
            run_id=run_id,
        )

    stored = store.get(location_id=location_id, source="forecast")
    assert stored is not None
    assert stored.last_observed_at == HOUR + timedelta(hours=5)


def test_rollback_leaves_the_watermark_untouched(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID],
) -> None:
    """The crash-before-commit case: the same window is collected again."""
    location_id, run_id = seeded
    store = WatermarkStore(db_engine)

    # The advance has to succeed before the simulated crash, so the block holds
    # two statements by design.
    with (  # noqa: PT012
        pytest.raises(RuntimeError),
        connection_scope(db_engine) as connection,
    ):
        store.advance(
            connection,
            location_id=location_id,
            source="forecast",
            last_observed_at=HOUR,
            run_id=run_id,
        )
        raise RuntimeError("crash before commit")

    assert store.get(location_id=location_id, source="forecast") is None


def test_sources_have_independent_watermarks(
    db_engine: Engine,
    seeded: tuple[int, uuid.UUID],
) -> None:
    location_id, run_id = seeded
    store = WatermarkStore(db_engine)

    with connection_scope(db_engine) as connection:
        store.advance(
            connection,
            location_id=location_id,
            source=ObservationSource.FORECAST,
            last_observed_at=HOUR,
            run_id=run_id,
        )

    assert store.get(location_id=location_id, source="archive") is None
    assert store.get(location_id=location_id, source="forecast") is not None
