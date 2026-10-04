"""The incremental cursor.

The watermark is what makes incremental collection correct: it records the newest
hour that has been *committed* for a location/source. Advancing it happens inside
the same transaction as the row upserts it accompanies (PLAN sections 4.3, 5.7 and
10.4), so a crash before commit leaves the cursor where it was and the next run
simply re-requests the same window.

Advancement is monotonic (``GREATEST``): a late-committing batch can never move a
watermark backwards.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine, RowMapping

from adcp.db.tables import ingestion_watermarks
from adcp.models.observation import ObservationSource


@dataclass(frozen=True, slots=True)
class WatermarkRecord:
    """A row of ``ingestion_watermarks`` as plain, typed data."""

    location_id: int
    source: str
    last_observed_at: datetime
    last_run_id: uuid.UUID | None
    updated_at: datetime


def _watermark_from_row(row: RowMapping) -> WatermarkRecord:
    last_run_id = row["last_run_id"]
    return WatermarkRecord(
        location_id=int(row["location_id"]),
        source=str(row["source"]),
        last_observed_at=row["last_observed_at"],
        last_run_id=None if last_run_id is None else uuid.UUID(str(last_run_id)),
        updated_at=row["updated_at"],
    )


class WatermarkStore:
    """Read/write access to ``ingestion_watermarks``."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def get(self, *, location_id: int, source: str) -> WatermarkRecord | None:
        """Return the stored watermark for a location/source pair, if any."""
        statement = sa.select(ingestion_watermarks).where(
            ingestion_watermarks.c.location_id == location_id,
            ingestion_watermarks.c.source == source,
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement).mappings().one_or_none()
        return None if row is None else _watermark_from_row(row)

    def advance(
        self,
        connection: Connection,
        *,
        location_id: int,
        source: ObservationSource | str,
        last_observed_at: datetime,
        run_id: uuid.UUID,
    ) -> None:
        """Move the watermark forward inside the caller's transaction.

        Call this only after the corresponding row upserts succeeded, and within
        the same transaction: the two must commit or roll back together.
        """
        source_value = source.value if isinstance(source, ObservationSource) else source
        statement = pg_insert(ingestion_watermarks).values(
            location_id=location_id,
            source=source_value,
            last_observed_at=last_observed_at,
            last_run_id=run_id,
        )
        statement = statement.on_conflict_do_update(
            index_elements=["location_id", "source"],
            set_={
                "last_observed_at": sa.func.greatest(
                    statement.excluded.last_observed_at,
                    ingestion_watermarks.c.last_observed_at,
                ),
                "last_run_id": statement.excluded.last_run_id,
                "updated_at": sa.func.now(),
            },
        )
        connection.execute(statement)


__all__ = ["WatermarkRecord", "WatermarkStore"]
