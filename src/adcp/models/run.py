"""Run-level accounting: the types the collection service produces.

These mirror ``ingestion_runs`` (PLAN section 5.5) and the status transitions in
section 11.3, so the CLI, the database writer, and the tests all speak one
vocabulary. Persistent columns are listed in :meth:`RunCounts.to_db_columns`;
anything else is derived and reported but not stored.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from adcp.models.observation import ObservationSource


class RunStatus(StrEnum):
    """Terminal status of a run (``ingestion_status`` enum values minus running)."""

    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


class LocationStatus(StrEnum):
    """Outcome for a single location within a run."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class RequestStats:
    """Upstream request counters, snapshotted so a run can report its deltas."""

    requests_made: int = 0
    requests_retried: int = 0

    def delta(self, later: RequestStats) -> RequestStats:
        """Counters accumulated between this snapshot and a later one."""
        return RequestStats(
            requests_made=max(later.requests_made - self.requests_made, 0),
            requests_retried=max(later.requests_retried - self.requests_retried, 0),
        )


@dataclass(frozen=True, slots=True)
class LocationResult:
    """Everything one location contributed to a run."""

    slug: str
    source: ObservationSource
    status: LocationStatus
    rows_received: int = 0
    rows_accepted: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    rows_rejected: int = 0
    rows_skipped: int = 0
    duration_ms: int = 0
    watermark: datetime | None = None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def failed(self) -> bool:
        return self.status is LocationStatus.FAILED

    @property
    def succeeded(self) -> bool:
        return self.status is LocationStatus.SUCCEEDED

    def as_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "source": self.source.value,
            "status": self.status.value,
            "rows_received": self.rows_received,
            "rows_accepted": self.rows_accepted,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_unchanged": self.rows_unchanged,
            "rows_rejected": self.rows_rejected,
            "rows_skipped": self.rows_skipped,
            "duration_ms": self.duration_ms,
            "watermark": None if self.watermark is None else self.watermark.isoformat(),
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, slots=True)
class RunCounts:
    """Run totals. Only the fields in :meth:`to_db_columns` are persisted."""

    locations_total: int = 0
    locations_succeeded: int = 0
    locations_failed: int = 0
    requests_made: int = 0
    requests_retried: int = 0
    rows_received: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    rows_rejected: int = 0
    error_count: int = 0
    #: Derived, reported in the CLI/dry-run but not a database column.
    rows_accepted: int = 0
    rows_skipped: int = 0

    @classmethod
    def from_results(
        cls,
        results: Iterable[LocationResult],
        *,
        locations_total: int,
        requests_made: int = 0,
        requests_retried: int = 0,
    ) -> RunCounts:
        """Aggregate per-location results into run totals."""
        collected = list(results)
        return cls(
            locations_total=locations_total,
            locations_succeeded=sum(1 for item in collected if item.succeeded),
            locations_failed=sum(1 for item in collected if item.failed),
            requests_made=requests_made,
            requests_retried=requests_retried,
            rows_received=sum(item.rows_received for item in collected),
            rows_inserted=sum(item.rows_inserted for item in collected),
            rows_updated=sum(item.rows_updated for item in collected),
            rows_unchanged=sum(item.rows_unchanged for item in collected),
            rows_rejected=sum(item.rows_rejected for item in collected),
            rows_accepted=sum(item.rows_accepted for item in collected),
            rows_skipped=sum(item.rows_skipped for item in collected),
            error_count=sum(1 for item in collected if item.failed),
        )

    def to_db_columns(self) -> dict[str, int]:
        """The ``ingestion_runs`` columns this accounting maps onto."""
        return {
            "locations_total": self.locations_total,
            "locations_succeeded": self.locations_succeeded,
            "locations_failed": self.locations_failed,
            "requests_made": self.requests_made,
            "requests_retried": self.requests_retried,
            "rows_received": self.rows_received,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_unchanged": self.rows_unchanged,
            "rows_rejected": self.rows_rejected,
            "error_count": self.error_count,
        }

    def as_dict(self) -> dict[str, int]:
        return {
            **self.to_db_columns(),
            "rows_accepted": self.rows_accepted,
            "rows_skipped": self.rows_skipped,
        }


@dataclass(frozen=True, slots=True)
class RunSummary:
    """The result of one collection run: what the CLI reports and exits on."""

    status: RunStatus
    trigger: str
    started_at: datetime
    finished_at: datetime
    counts: RunCounts
    run_id: uuid.UUID | None = None
    lock_acquired: bool = True
    dry_run: bool = False
    window_truncated: bool = False
    error_summary: str | None = None
    results: tuple[LocationResult, ...] = ()

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1_000)

    @property
    def locations_attempted(self) -> int:
        return len(self.results)

    def as_dict(self) -> dict[str, Any]:
        """JSON-ready payload for ``adcp collect --json``."""
        return {
            "run_id": None if self.run_id is None else str(self.run_id),
            "status": self.status.value,
            "trigger": self.trigger,
            "dry_run": self.dry_run,
            "lock_acquired": self.lock_acquired,
            "window_truncated": self.window_truncated,
            "locations_attempted": self.locations_attempted,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_ms": self.duration_ms,
            "error_summary": self.error_summary,
            **self.counts.as_dict(),
        }


__all__ = [
    "LocationResult",
    "LocationStatus",
    "RequestStats",
    "RunCounts",
    "RunStatus",
    "RunSummary",
]
