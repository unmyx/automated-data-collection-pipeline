"""Turn a watermark into "what to request" and "what to store".

Two different windows matter, and confusing them is how a pipeline either loses
data or rejects half of every payload:

**Storage window** - ``[start, end)``: the hours this run is allowed to write.
``end`` is the current hour floored, and therefore exclusive: the hour in progress
is incomplete and gets collected by the next run. ``start`` is the watermark minus
the revision overlap, clamped to the configured lookback (PLAN section 4.3).

**Accept range** - ``[accept_from, accept_to)``: everything the provider can
legitimately return for the request we are about to send. Open-Meteo selects whole
calendar days, so with ``past_days=3&forecast_days=1`` requested at any time on
2026-10-02 the payload covers ``2026-09-29T00:00`` through ``2026-10-02T23:00``
(verified against the live API). Rows inside the accept range but outside the
storage window are **skipped**, not rejected - they are the forecast tail of today
that the next run will observe properly. Rows outside the accept range are a
provider or pipeline bug and are rejected as ``OutOfWindow``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from adcp.models.window import RecentWindow
from adcp.normalization import normalize_timestamp

#: The current day must be requested for its completed hours to appear; the tail
#: of the day is forecast data and is skipped by the storage window.
FORECAST_DAYS = 1


class RowPlacement(StrEnum):
    """Where an observation from the payload belongs."""

    STORED = "stored"
    SKIPPED = "skipped"
    OUT_OF_RANGE = "out_of_range"


def start_of_day(moment: datetime) -> datetime:
    """Midnight UTC of the day containing ``moment``."""
    return normalize_timestamp(moment).replace(hour=0, minute=0, second=0, microsecond=0)


def floor_to_hour(moment: datetime) -> datetime:
    """The hour boundary at or before ``moment``."""
    return normalize_timestamp(moment).replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True, slots=True)
class CollectionWindow:
    """Storage bounds plus the accept range for one fetch."""

    storage_start: datetime
    storage_end: datetime
    accept_from: datetime
    accept_to: datetime
    truncated: bool = False

    def __post_init__(self) -> None:
        if self.storage_end <= self.storage_start:
            msg = f"storage window {self.storage_start}..{self.storage_end} is empty"
            raise ValueError(msg)
        if self.accept_to <= self.accept_from:
            msg = f"accept range {self.accept_from}..{self.accept_to} is empty"
            raise ValueError(msg)

    @property
    def hours(self) -> int:
        """How many complete hours the storage window covers."""
        return int((self.storage_end - self.storage_start).total_seconds() // 3_600)

    def placement(self, moment: datetime) -> RowPlacement:
        """Classify a payload timestamp against both windows."""
        candidate = normalize_timestamp(moment)
        if self.storage_start <= candidate < self.storage_end:
            return RowPlacement.STORED
        if self.accept_from <= candidate < self.accept_to:
            return RowPlacement.SKIPPED
        return RowPlacement.OUT_OF_RANGE

    def contains(self, moment: datetime) -> bool:
        """Whether the moment belongs in the storage window."""
        return self.placement(moment) is RowPlacement.STORED

    def as_dict(self) -> dict[str, object]:
        return {
            "storage_start": self.storage_start.isoformat(),
            "storage_end": self.storage_end.isoformat(),
            "storage_hours": self.hours,
            "accept_from": self.accept_from.isoformat(),
            "accept_to": self.accept_to.isoformat(),
            "truncated": self.truncated,
        }


@dataclass(frozen=True, slots=True)
class ScheduledWindow:
    """The request to send and the bounds to validate its answer against."""

    request: RecentWindow
    bounds: CollectionWindow

    @property
    def truncated(self) -> bool:
        return self.bounds.truncated


def plan_scheduled_window(
    *,
    now: datetime,
    watermark: datetime | None,
    lookback_hours: int,
    overlap_hours: int,
) -> ScheduledWindow:
    """Plan a scheduled collection window from the stored watermark.

    ``watermark`` is the newest hour already committed for this location/source;
    ``None`` means "never collected", and the full lookback is requested.
    """
    moment = normalize_timestamp(now)
    end = floor_to_hour(moment)
    earliest_hour = end - timedelta(hours=lookback_hours)

    if watermark is None:
        start = earliest_hour
        truncated = False
    else:
        candidate = floor_to_hour(watermark) - timedelta(hours=overlap_hours)
        truncated = candidate < earliest_hour
        start = max(candidate, earliest_hour)

    past_days = max(1, math.ceil(lookback_hours / 24))
    request = RecentWindow(past_days=past_days, forecast_days=FORECAST_DAYS)
    bounds = CollectionWindow(
        storage_start=start,
        storage_end=end,
        accept_from=start_of_day(moment) - timedelta(days=past_days),
        accept_to=start_of_day(moment) + timedelta(days=FORECAST_DAYS),
        truncated=truncated,
    )
    return ScheduledWindow(request=request, bounds=bounds)


__all__ = [
    "FORECAST_DAYS",
    "CollectionWindow",
    "RowPlacement",
    "ScheduledWindow",
    "floor_to_hour",
    "plan_scheduled_window",
    "start_of_day",
]
