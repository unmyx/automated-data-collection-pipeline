"""Repositories - the only place that writes rows.

``LocationRepository`` manages configured locations. ``WeatherRepository`` owns
the idempotent write path: the natural key ``(location_id, observed_at, source)``
plus a content hash, so that re-observing an unchanged hour is a true storage
no-op and a revised value updates in place (PLAN sections 10.1-10.3).

Every write method takes a :class:`~sqlalchemy.engine.Connection` rather than
opening its own: the caller decides the transaction boundary, which is what makes
"one transaction per location, watermark included" possible (PLAN section 10.4).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine, RowMapping

from adcp.db.engine import connection_scope
from adcp.db.tables import locations, weather_hourly
from adcp.errors import LocationNotFoundError
from adcp.models.observation import (
    HOURLY_VARIABLES,
    ObservationSource,
    WeatherObservation,
    WeatherSeries,
)
from adcp.normalization import row_hash

#: Columns refreshed when the same natural key arrives with different content.
#: ``first_seen_run_id``/``first_collected_at`` are deliberately absent: they record
#: discovery, not revision.
MUTABLE_COLUMNS: tuple[str, ...] = (
    *HOURLY_VARIABLES,
    "upstream_latitude",
    "upstream_longitude",
    "upstream_elevation_m",
    "upstream_timezone",
)

#: Rows per statement. Big enough to amortise round trips, small enough to keep
#: statement size and lock duration modest for a full backfill chunk.
UPSERT_CHUNK_SIZE = 500


@dataclass(frozen=True, slots=True)
class LocationRecord:
    """A row of ``locations`` as plain, typed data."""

    id: int
    slug: str
    name: str
    latitude: Decimal
    longitude: Decimal
    timezone: str
    country_code: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class UpsertCounts:
    """How an observation batch changed the table."""

    received: int
    inserted: int
    updated: int
    unchanged: int

    @property
    def written(self) -> int:
        """Rows that actually reached storage (inserted or updated)."""
        return self.inserted + self.updated

    def as_dict(self) -> dict[str, int]:
        return {
            "received": self.received,
            "inserted": self.inserted,
            "updated": self.updated,
            "unchanged": self.unchanged,
        }


def _location_from_row(row: RowMapping) -> LocationRecord:
    country_code = row["country_code"]
    return LocationRecord(
        id=int(row["id"]),
        slug=str(row["slug"]),
        name=str(row["name"]),
        latitude=Decimal(row["latitude"]),
        longitude=Decimal(row["longitude"]),
        timezone=str(row["timezone"]),
        # char(2) is blank padded by PostgreSQL; normalise on the way out.
        country_code=None if country_code is None else str(country_code).strip(),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class LocationRepository:
    """CRUD for the ``locations`` table - configuration stored as data."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def list_all(self) -> list[LocationRecord]:
        """Every configured location, ordered by slug."""
        statement = sa.select(locations).order_by(locations.c.slug)
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return [_location_from_row(row) for row in rows]

    def list_active(self) -> list[LocationRecord]:
        """Only the locations a collection run should visit, ordered by slug."""
        statement = sa.select(locations).where(locations.c.is_active).order_by(locations.c.slug)
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return [_location_from_row(row) for row in rows]

    def get_by_slug(self, slug: str) -> LocationRecord | None:
        """Return the location with this slug, or ``None``."""
        statement = sa.select(locations).where(locations.c.slug == slug)
        with self._engine.connect() as connection:
            row = connection.execute(statement).mappings().one_or_none()
        return None if row is None else _location_from_row(row)

    def create(  # noqa: PLR0913 - keyword-only location fields, all self-documenting
        self,
        *,
        slug: str,
        name: str,
        latitude: Decimal | float | str,
        longitude: Decimal | float | str,
        timezone: str = "UTC",
        country_code: str | None = None,
        is_active: bool = True,
    ) -> LocationRecord:
        """Insert a location.

        Raises:
            sqlalchemy.exc.IntegrityError: the slug is taken, or a check
                constraint rejected the values (bad slug format, latitude or
                longitude out of range). Callers surface it as a readable
                message; ``scripts/seed_locations.py`` is the demo entry point.
        """
        statement = (
            locations.insert()
            .values(
                slug=slug,
                name=name,
                latitude=latitude,
                longitude=longitude,
                timezone=timezone,
                country_code=country_code,
                is_active=is_active,
            )
            .returning(*locations.c)
        )
        with connection_scope(self._engine) as connection:
            row = connection.execute(statement).mappings().one()
        return _location_from_row(row)

    def set_active(self, slug: str, *, is_active: bool) -> LocationRecord:
        """Retire or re-enable a location without deleting its history.

        Raises:
            LocationNotFoundError: no location has that slug.
        """
        statement = (
            locations.update()
            .where(locations.c.slug == slug)
            .values(is_active=is_active, updated_at=sa.func.now())
            .returning(*locations.c)
        )
        with connection_scope(self._engine) as connection:
            row = connection.execute(statement).mappings().one_or_none()
        if row is None:
            msg = f"location {slug!r} does not exist"
            raise LocationNotFoundError(msg)
        return _location_from_row(row)


class WeatherRepository:
    """Persistence for ``weather_hourly``."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def upsert_observations(
        self,
        connection: Connection,
        *,
        location_id: int,
        run_id: uuid.UUID,
        series: WeatherSeries,
        observations: Sequence[WeatherObservation],
    ) -> UpsertCounts:
        """Insert or update a batch of observations inside the caller's transaction.

        Unchanged rows are untouched: the conflict branch only fires when the
        content hash differs, so a re-run of an identical window writes nothing at
        all (``inserted == 0 and updated == 0``).

        Args:
            connection: an open transaction; the caller commits.
            location_id: **the database's** location id, never a value from the
                provider payload (PLAN section 9.5).
            run_id: the run these rows were seen by.
            series: supplies the source and the upstream grid metadata.
            observations: already-validated rows for this location and source.

        Raises:
            ValueError: the batch contains the same ``observed_at`` twice, which
                PostgreSQL would reject as "cannot affect row a second time".
        """
        if not observations:
            return UpsertCounts(received=0, inserted=0, updated=0, unchanged=0)
        self._reject_duplicate_timestamps(observations)

        inserted = 0
        updated = 0
        for chunk in _chunked(observations, UPSERT_CHUNK_SIZE):
            rows = [
                self._row(observation, location_id=location_id, run_id=run_id, series=series)
                for observation in chunk
            ]
            insert_statement = pg_insert(weather_hourly).values(rows)
            excluded = insert_statement.excluded
            conflict_statement: Any = insert_statement.on_conflict_do_update(
                index_elements=["location_id", "observed_at", "source"],
                set_={
                    **{name: excluded[name] for name in MUTABLE_COLUMNS},
                    "row_hash": excluded.row_hash,
                    "last_seen_run_id": excluded.last_seen_run_id,
                    "last_collected_at": sa.func.now(),
                    # Only rows whose hash differs reach this branch, so the
                    # revision counter can simply increment.
                    "revision_count": weather_hourly.c.revision_count + 1,
                },
                where=weather_hourly.c.row_hash.is_distinct_from(excluded.row_hash),
            ).returning(sa.literal_column("(xmax = 0)").label("inserted"))

            flags = connection.execute(conflict_statement).scalars().all()
            inserted += sum(1 for flag in flags if flag)
            updated += sum(1 for flag in flags if not flag)

        return UpsertCounts(
            received=len(observations),
            inserted=inserted,
            updated=updated,
            unchanged=len(observations) - inserted - updated,
        )

    def _row(
        self,
        observation: WeatherObservation,
        *,
        location_id: int,
        run_id: uuid.UUID,
        series: WeatherSeries,
    ) -> dict[str, Any]:
        return {
            "location_id": location_id,
            "observed_at": observation.observed_at,
            "source": series.source.value,
            **{name: getattr(observation, name) for name in HOURLY_VARIABLES},
            "row_hash": row_hash(observation),
            "upstream_latitude": series.grid_latitude,
            "upstream_longitude": series.grid_longitude,
            "upstream_elevation_m": series.elevation_m,
            "upstream_timezone": series.upstream_timezone,
            "first_seen_run_id": run_id,
            "last_seen_run_id": run_id,
        }

    @staticmethod
    def _reject_duplicate_timestamps(observations: Sequence[WeatherObservation]) -> None:
        moments = [observation.observed_at for observation in observations]
        if len(set(moments)) != len(moments):
            msg = "batch contains duplicate observed_at values for one location/source"
            raise ValueError(msg)

    def count_for(
        self,
        *,
        location_id: int,
        source: ObservationSource | str,
    ) -> int:
        """Number of stored rows for a location/source, used by tests and reports."""
        source_value = source.value if isinstance(source, ObservationSource) else source
        statement = (
            sa.select(sa.func.count())
            .select_from(weather_hourly)
            .where(
                weather_hourly.c.location_id == location_id,
                weather_hourly.c.source == source_value,
            )
        )
        with self._engine.connect() as connection:
            return int(connection.execute(statement).scalar_one())


def _chunked(
    observations: Sequence[WeatherObservation],
    size: int,
) -> Iterator[Sequence[WeatherObservation]]:
    for start in range(0, len(observations), size):
        yield observations[start : start + size]


__all__ = [
    "MUTABLE_COLUMNS",
    "UPSERT_CHUNK_SIZE",
    "LocationRecord",
    "LocationRepository",
    "UpsertCounts",
    "WeatherRepository",
]
