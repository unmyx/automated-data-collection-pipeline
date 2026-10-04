"""The time span a request asks for.

Two shapes exist because Open-Meteo exposes two different ways to select hours:
a recent window (``past_days``/``forecast_days``) for scheduled runs, and an
explicit range (``start_date``/``end_date``) for backfills - see ``docs/PLAN.md``
section 6.2.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from adcp.errors import ConfigurationError

#: Open-Meteo's documented limits; re-verified against the live API and enforced
#: here as configuration errors (never retried - the request is wrong).
MAX_PAST_DAYS = 92
MAX_FORECAST_DAYS = 16


@dataclass(frozen=True, slots=True)
class DateRange:
    """An inclusive calendar range, as used by the archive endpoint."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            msg = f"date range end {self.end.isoformat()} precedes start {self.start.isoformat()}"
            raise ConfigurationError(msg)

    @property
    def days(self) -> int:
        """Number of calendar days covered, inclusive."""
        return (self.end - self.start).days + 1


@dataclass(frozen=True, slots=True)
class RecentWindow:
    """A window expressed as "the last N days plus the next M days"."""

    past_days: int
    forecast_days: int = 1

    def __post_init__(self) -> None:
        if not 0 <= self.past_days <= MAX_PAST_DAYS:
            msg = f"past_days must be between 0 and {MAX_PAST_DAYS}, got {self.past_days}"
            raise ConfigurationError(msg)
        if not 0 <= self.forecast_days <= MAX_FORECAST_DAYS:
            msg = (
                f"forecast_days must be between 0 and {MAX_FORECAST_DAYS}, got {self.forecast_days}"
            )
            raise ConfigurationError(msg)
        if self.past_days + self.forecast_days < 1:
            msg = "a recent window must cover at least one day"
            raise ConfigurationError(msg)


type HourlyWindow = DateRange | RecentWindow

__all__ = [
    "MAX_FORECAST_DAYS",
    "MAX_PAST_DAYS",
    "DateRange",
    "HourlyWindow",
    "RecentWindow",
]
