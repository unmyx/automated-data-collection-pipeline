"""Domain models: storage- and transport-independent types.

Nothing in this package imports ``httpx``, ``sqlalchemy``, or ``typer`` - the
architecture rule from ``docs/PLAN.md`` section 3.2. Adapters translate to and
from these types, which is what keeps the pipeline testable without a network or
a database.
"""

from __future__ import annotations

from adcp.models.location import Location, LocationLike
from adcp.models.observation import (
    HOURLY_VARIABLES,
    ObservationSource,
    WeatherObservation,
    WeatherSeries,
)
from adcp.models.window import (
    MAX_FORECAST_DAYS,
    MAX_PAST_DAYS,
    DateRange,
    HourlyWindow,
    RecentWindow,
)

__all__ = [
    "HOURLY_VARIABLES",
    "MAX_FORECAST_DAYS",
    "MAX_PAST_DAYS",
    "DateRange",
    "HourlyWindow",
    "Location",
    "LocationLike",
    "ObservationSource",
    "RecentWindow",
    "WeatherObservation",
    "WeatherSeries",
]
