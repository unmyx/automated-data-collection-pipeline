"""Observations as the application sees them, plus the ``source`` distinction."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from adcp.models.location import Location


class ObservationSource(StrEnum):
    """Where a series of observations came from.

    ``source`` is part of the natural key of ``weather_hourly`` (PLAN section 5.3)
    because a forecast and an archive observation of the same hour are different
    measurements of reality and must be able to coexist. The values match the
    ``CHECK`` constraint in the migration of the same name.
    """

    FORECAST = "forecast"
    HISTORICAL_FORECAST = "historical_forecast"
    ARCHIVE = "archive"


#: Hourly variables the pipeline collects, in the canonical request order
#: (PLAN section 6.2). Order matters: it makes request construction deterministic.
HOURLY_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
)


@dataclass(frozen=True, slots=True)
class WeatherObservation:
    """One hour of measurements for one location and one source.

    Hashable by construction (frozen, all fields immutable) so it can be used as
    a set/dict key and compared cheaply when the upsert decides whether a stored
    row actually changed.

    ``weather_code`` and ``wind_direction_10m`` are integers because the fact
    table stores them as ``smallint`` (PLAN section 5.3); the mapper rejects
    non-integral values rather than rounding them.
    """

    observed_at: datetime
    temperature_2m: Decimal | None = None
    relative_humidity_2m: Decimal | None = None
    dew_point_2m: Decimal | None = None
    apparent_temperature: Decimal | None = None
    precipitation: Decimal | None = None
    rain: Decimal | None = None
    snowfall: Decimal | None = None
    weather_code: int | None = None
    cloud_cover: Decimal | None = None
    pressure_msl: Decimal | None = None
    wind_speed_10m: Decimal | None = None
    wind_direction_10m: int | None = None
    wind_gusts_10m: Decimal | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            msg = f"observed_at {self.observed_at!r} must be timezone-aware"
            raise ValueError(msg)

    def values(self) -> dict[str, Decimal | int | None]:
        """Measurements keyed by column name, in canonical order."""
        return {name: getattr(self, name) for name in HOURLY_VARIABLES}


@dataclass(frozen=True, slots=True)
class WeatherSeries:
    """A parsed response for one location and one source.

    This is the boundary type between the API layer and the validation/persistence
    layers: no raw dictionaries travel past it.
    """

    location: Location
    source: ObservationSource
    observations: tuple[WeatherObservation, ...]
    grid_latitude: Decimal
    grid_longitude: Decimal
    elevation_m: Decimal | None
    upstream_timezone: str | None
    fetched_at: datetime
    #: Provider unit strings, kept as provenance. Excluded from hashing (mappings
    #: are unhashable) but included in equality.
    units: Mapping[str, str] = field(default_factory=dict, hash=False)

    @property
    def hours(self) -> int:
        """How many hourly rows the series carries."""
        return len(self.observations)

    @property
    def first_observed_at(self) -> datetime | None:
        """Oldest hour in the series, or ``None`` when empty."""
        return self.observations[0].observed_at if self.observations else None

    @property
    def last_observed_at(self) -> datetime | None:
        """Newest hour in the series, or ``None`` when empty."""
        return self.observations[-1].observed_at if self.observations else None


__all__ = [
    "HOURLY_VARIABLES",
    "ObservationSource",
    "WeatherObservation",
    "WeatherSeries",
]
