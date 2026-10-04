"""Ingestion run accounting: the audit trail of every collection run.

``ingestion_runs`` is written in three phases (PLAN section 5.5): a row when the
run starts, an error row per failure while it proceeds, and a final update that
sets the terminal status, the counters, and the duration. Errors are recorded in
their own transaction on purpose - a location whose transaction rolled back must
still leave evidence behind.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine, RowMapping

from adcp.db.engine import connection_scope
from adcp.db.tables import ingestion_run_errors, ingestion_runs, weather_hourly
from adcp.errors import RunNotFoundError
from adcp.logging import mask_credentials_in_text
from adcp.models.run import RunStatus

#: Payload samples are forensic, not archival: bound them (PLAN section 5.6).
MAX_PAYLOAD_SAMPLE_CHARS = 4096

#: Error messages are bounded too, so one pathological response cannot bloat the
#: table.
MAX_ERROR_MESSAGE_CHARS = 2_000

_SAMPLE_EXCERPT_CHARS = MAX_PAYLOAD_SAMPLE_CHARS - 200

#: Counters that map onto real columns; anything else is a programming error.
COUNTER_COLUMNS: frozenset[str] = frozenset(
    {
        "locations_total",
        "locations_succeeded",
        "locations_failed",
        "requests_made",
        "requests_retried",
        "rows_received",
        "rows_inserted",
        "rows_updated",
        "rows_unchanged",
        "rows_rejected",
        "error_count",
    },
)

#: Summary written onto a run the reaper had to abandon.
REAPED_SUMMARY = "reaped: the run was still marked running long past its budget"


@dataclass(frozen=True, slots=True)
class PruneSummary:
    """What retention pruning found (and, unless dry-run, removed)."""

    dry_run: bool
    runs_eligible: int
    runs_deleted: int
    runs_kept_by_provenance: int
    errors_eligible: int
    errors_deleted: int
    runs_cutoff: datetime
    errors_cutoff: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "runs_eligible": self.runs_eligible,
            "runs_deleted": self.runs_deleted,
            "runs_kept_by_provenance": self.runs_kept_by_provenance,
            "errors_eligible": self.errors_eligible,
            "errors_deleted": self.errors_deleted,
            "runs_cutoff": self.runs_cutoff.isoformat(),
            "errors_cutoff": self.errors_cutoff.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class RunRecord:
    """A row of ``ingestion_runs`` as plain, typed data."""

    id: uuid.UUID
    run_type: str
    trigger: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    window_from: datetime | None
    window_to: datetime | None
    locations_total: int
    locations_succeeded: int
    locations_failed: int
    rows_received: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_rejected: int
    error_count: int
    error_summary: str | None


def _run_from_row(row: RowMapping) -> RunRecord:
    return RunRecord(
        id=uuid.UUID(str(row["id"])),
        run_type=str(row["run_type"]),
        trigger=str(row["trigger"]),
        status=str(row["status"]),
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        window_from=row["window_from"],
        window_to=row["window_to"],
        locations_total=int(row["locations_total"]),
        locations_succeeded=int(row["locations_succeeded"]),
        locations_failed=int(row["locations_failed"]),
        rows_received=int(row["rows_received"]),
        rows_inserted=int(row["rows_inserted"]),
        rows_updated=int(row["rows_updated"]),
        rows_unchanged=int(row["rows_unchanged"]),
        rows_rejected=int(row["rows_rejected"]),
        error_count=int(row["error_count"]),
        error_summary=None if row["error_summary"] is None else str(row["error_summary"]),
    )


class RunTracker:
    """Read/write access to ``ingestion_runs`` and ``ingestion_run_errors``."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def start_run(  # noqa: PLR0913 - keyword-only run metadata, all self-documenting
        self,
        *,
        run_type: str,
        trigger: str,
        app_version: str,
        window_from: datetime | None = None,
        window_to: datetime | None = None,
        requested_from: datetime | None = None,
        requested_to: datetime | None = None,
        hostname: str | None = None,
    ) -> RunRecord:
        """Insert a run row with status ``running`` and return it."""
        statement = (
            ingestion_runs.insert()
            .values(
                run_type=run_type,
                trigger=trigger,
                status="running",
                app_version=app_version,
                window_from=window_from,
                window_to=window_to,
                requested_from=requested_from,
                requested_to=requested_to,
                hostname=hostname,
            )
            .returning(*ingestion_runs.c)
        )
        with connection_scope(self._engine) as connection:
            row = connection.execute(statement).mappings().one()
        return _run_from_row(row)

    def list_recent(self, *, limit: int = 10) -> list[RunRecord]:
        """Most recently started runs first."""
        statement = (
            sa.select(ingestion_runs).order_by(ingestion_runs.c.started_at.desc()).limit(limit)
        )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return [_run_from_row(row) for row in rows]

    def finish_run(  # noqa: PLR0913 - mirrors the ingestion_runs accounting columns
        self,
        connection: Connection,
        run_id: uuid.UUID,
        *,
        status: RunStatus | str,
        counts: Mapping[str, int] | None = None,
        error_summary: str | None = None,
        duration_ms: int | None = None,
        finished_at: datetime | None = None,
    ) -> RunRecord:
        """Set the terminal status, counters, and duration for a run.

        Raises:
            ValueError: a counter is not an ``ingestion_runs`` column.
            RunNotFoundError: no run has that id.
        """
        status_value = status.value if isinstance(status, RunStatus) else status
        values: dict[str, Any] = {
            "status": status_value,
            "finished_at": finished_at if finished_at is not None else sa.func.now(),
            "duration_ms": duration_ms,
            "error_summary": (
                None if error_summary is None else _bounded(error_summary, MAX_ERROR_MESSAGE_CHARS)
            ),
        }
        for column, value in (counts or {}).items():
            if column not in COUNTER_COLUMNS:
                msg = f"{column!r} is not an ingestion_runs counter column"
                raise ValueError(msg)
            values[column] = int(value)

        statement = (
            ingestion_runs.update()
            .where(ingestion_runs.c.id == run_id)
            .values(**values)
            .returning(*ingestion_runs.c)
        )
        with connection.begin_nested():
            row = connection.execute(statement).mappings().one_or_none()
        if row is None:
            msg = f"ingestion run {run_id} does not exist"
            raise RunNotFoundError(msg)
        return _run_from_row(row)

    def record_error(  # noqa: PLR0913 - mirrors the ingestion_run_errors columns
        self,
        connection: Connection,
        run_id: uuid.UUID,
        *,
        phase: str,
        error_type: str,
        message: str,
        location_id: int | None = None,
        attempt: int | None = None,
        http_status: int | None = None,
        error_code: str | None = None,
        request_url: str | None = None,
        payload_sample: Mapping[str, object] | Sequence[object] | None = None,
    ) -> None:
        """Quarantine one failure, with credentials stripped and payloads bounded."""
        connection.execute(
            ingestion_run_errors.insert().values(
                run_id=run_id,
                location_id=location_id,
                phase=phase,
                error_type=error_type,
                error_code=error_code,
                message=_bounded(mask_credentials_in_text(message), MAX_ERROR_MESSAGE_CHARS),
                attempt=attempt,
                http_status=http_status,
                request_url=(
                    None
                    if request_url is None
                    else _bounded(mask_credentials_in_text(request_url), MAX_ERROR_MESSAGE_CHARS)
                ),
                payload_sample=_bounded_sample(payload_sample),
            ),
        )

    def count_errors(self, run_id: uuid.UUID) -> int:
        """How many error rows a run produced."""
        statement = (
            sa.select(sa.func.count())
            .select_from(ingestion_run_errors)
            .where(ingestion_run_errors.c.run_id == run_id)
        )
        with self._engine.connect() as connection:
            return int(connection.execute(statement).scalar_one())

    def reap_stale_runs(self, *, max_age_s: int) -> list[uuid.UUID]:
        """Mark runs that have been ``running`` for too long as ``failed``.

        A process killed mid-run leaves its row ``running`` forever, which would
        make the run table lie about the system's health (PLAN section 11.3). The
        reaper is called at the start of every collection, inside the advisory
        lock, and returns the ids it changed so the caller can log them.
        """
        statement = (
            ingestion_runs.update()
            .where(
                ingestion_runs.c.status == "running",
                ingestion_runs.c.finished_at.is_(None),
                ingestion_runs.c.started_at
                < sa.func.now() - sa.text(f"make_interval(secs => {int(max_age_s)})"),
            )
            .values(
                status=RunStatus.FAILED.value,
                finished_at=sa.func.now(),
                error_summary=sa.func.coalesce(ingestion_runs.c.error_summary, REAPED_SUMMARY),
            )
            .returning(ingestion_runs.c.id)
        )
        with connection_scope(self._engine) as connection:
            return [uuid.UUID(str(row)) for row in connection.execute(statement).scalars()]

    def prune(
        self,
        *,
        runs_older_than_days: int,
        errors_older_than_days: int,
        dry_run: bool = True,
    ) -> PruneSummary:
        """Apply the retention policy from PLAN section 5.8.

        ``weather_hourly`` refers to the run that first and last saw each row, so
        a run that produced observations cannot be deleted without destroying
        provenance. Those runs are kept and counted separately - the honest
        behaviour, and the reason the plan calls it "archive" rather than
        "delete" - while their error rows still age out normally.
        """
        now = datetime.now(UTC)
        runs_cutoff = now - timedelta(days=runs_older_than_days)
        errors_cutoff = now - timedelta(days=errors_older_than_days)

        referenced = sa.exists(
            sa.select(1)
            .select_from(weather_hourly)
            .where(
                sa.or_(
                    weather_hourly.c.first_seen_run_id == ingestion_runs.c.id,
                    weather_hourly.c.last_seen_run_id == ingestion_runs.c.id,
                ),
            ),
        )
        prunable_runs = sa.and_(
            ingestion_runs.c.started_at < runs_cutoff,
            ingestion_runs.c.status != "running",
        )
        deletable_runs = sa.and_(prunable_runs, ~referenced)
        # Error rows disappear for two reasons: they aged out, or their run is
        # being deleted (the foreign key cascades). Both are deleted explicitly so
        # the reported count is exact rather than "however many the cascade took".
        deletable_errors = sa.or_(
            ingestion_run_errors.c.occurred_at < errors_cutoff,
            ingestion_run_errors.c.run_id.in_(
                sa.select(ingestion_runs.c.id).where(deletable_runs),
            ),
        )

        with connection_scope(self._engine) as connection:
            runs_eligible = _count(connection, ingestion_runs, deletable_runs)
            kept_by_provenance = _count(
                connection,
                ingestion_runs,
                sa.and_(prunable_runs, referenced),
            )
            errors_eligible = _count(connection, ingestion_run_errors, deletable_errors)

            runs_deleted = 0
            errors_deleted = 0
            if not dry_run:
                errors_deleted = int(
                    connection.execute(
                        ingestion_run_errors.delete().where(deletable_errors),
                    ).rowcount,
                )
                runs_deleted = int(
                    connection.execute(
                        ingestion_runs.delete().where(deletable_runs),
                    ).rowcount,
                )

        return PruneSummary(
            dry_run=dry_run,
            runs_eligible=runs_eligible,
            runs_deleted=runs_deleted,
            runs_kept_by_provenance=kept_by_provenance,
            errors_eligible=errors_eligible,
            errors_deleted=errors_deleted,
            runs_cutoff=runs_cutoff,
            errors_cutoff=errors_cutoff,
        )


def _count(connection: Connection, table: sa.Table, predicate: Any) -> int:
    statement = sa.select(sa.func.count()).select_from(table).where(predicate)
    return int(connection.execute(statement).scalar_one())


def _bounded(text: str, limit: int) -> str:
    return text if len(text) <= limit else f"{text[: limit - 3]}..."


def _bounded_sample(
    sample: Mapping[str, object] | Sequence[object] | None,
) -> dict[str, object] | list[object] | None:
    """Keep a payload sample under the size cap while staying valid JSON.

    Truncating serialised JSON and casting it back to ``jsonb`` (as the plan's
    sketch suggested) would produce invalid JSON, so an oversized sample is stored
    as a wrapper object carrying a plain-text excerpt instead.
    """
    if sample is None:
        return None
    serialized = json.dumps(sample, default=str, sort_keys=True)
    if len(serialized) <= MAX_PAYLOAD_SAMPLE_CHARS:
        return dict(sample) if isinstance(sample, Mapping) else list(sample)
    return {"truncated": True, "excerpt": serialized[:_SAMPLE_EXCERPT_CHARS]}


__all__ = [
    "COUNTER_COLUMNS",
    "MAX_ERROR_MESSAGE_CHARS",
    "MAX_PAYLOAD_SAMPLE_CHARS",
    "REAPED_SUMMARY",
    "PruneSummary",
    "RunRecord",
    "RunTracker",
]
