"""Deterministic request construction for the Open-Meteo hourly endpoints.

Keeping construction in its own value object means the same parameters are used
for the real request, for the dry-run report, and for the tests - and the order
of the query parameters never depends on dictionary iteration order.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from adcp.errors import ConfigurationError
from adcp.models.location import Location
from adcp.models.observation import HOURLY_VARIABLES, ObservationSource
from adcp.models.window import DateRange, HourlyWindow, RecentWindow

# Variables requested by default: exactly the columns the fact table stores.
DEFAULT_VARIABLES: tuple[str, ...] = HOURLY_VARIABLES

# Pinning the timezone keeps every timestamp unambiguous (PLAN 6.2).
REQUEST_TIMEZONE = "UTC"

# Ask for the nearest grid cell rather than the nearest land cell.
CELL_SELECTION = "nearest"


def format_coordinate(value: Decimal) -> str:
    """Render a coordinate as a plain decimal string (no exponent, no padding)."""
    return format(Decimal(value).normalize(), "f")


@dataclass(frozen=True, slots=True)
class HourlyRequest:
    """One location, one source, one window, one set of hourly variables."""

    location: Location
    source: ObservationSource
    window: HourlyWindow
    variables: tuple[str, ...] = DEFAULT_VARIABLES

    def __post_init__(self) -> None:
        if not self.variables:
            msg = "a request must ask for at least one hourly variable"
            raise ConfigurationError(msg)
        unknown = tuple(sorted({name for name in self.variables if name not in HOURLY_VARIABLES}))
        if unknown:
            msg = f"unsupported hourly variables: {', '.join(unknown)}"
            raise ConfigurationError(msg)
        if self.source is ObservationSource.ARCHIVE and not isinstance(self.window, DateRange):
            msg = (
                "the archive endpoint needs an explicit start_date/end_date window; "
                "use DateRange, not RecentWindow"
            )
            raise ConfigurationError(msg)

    def params(self) -> dict[str, str]:
        """Query parameters in a fixed, reviewable order (credentials excluded)."""
        params: dict[str, str] = {
            "latitude": format_coordinate(self.location.latitude),
            "longitude": format_coordinate(self.location.longitude),
            "hourly": ",".join(self.variables),
            "timezone": REQUEST_TIMEZONE,
            "cell_selection": CELL_SELECTION,
        }
        if isinstance(self.window, DateRange):
            params["start_date"] = self.window.start.isoformat()
            params["end_date"] = self.window.end.isoformat()
        elif isinstance(self.window, RecentWindow):
            params["past_days"] = str(self.window.past_days)
            params["forecast_days"] = str(self.window.forecast_days)
        return params

    def describe(self) -> dict[str, object]:
        """Structured fields for log lines and error records."""
        return {
            "location": self.location.slug,
            "source": self.source.value,
            "window": window_label(self.window),
            "variables": len(self.variables),
        }


def window_label(window: HourlyWindow) -> str:
    """Short human-readable window description."""
    if isinstance(window, DateRange):
        return f"{window.start.isoformat()}..{window.end.isoformat()}"
    return f"past_{window.past_days}d+forecast_{window.forecast_days}d"


__all__ = [
    "CELL_SELECTION",
    "DEFAULT_VARIABLES",
    "REQUEST_TIMEZONE",
    "HourlyRequest",
    "format_coordinate",
    "window_label",
]
