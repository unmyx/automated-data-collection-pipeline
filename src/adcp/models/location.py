"""The domain location: what a request needs to know, independent of storage."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, runtime_checkable

from adcp.errors import ConfigurationError


@runtime_checkable
class LocationLike(Protocol):
    """Structural type for anything that can describe a location.

    ``adcp.db.repository.LocationRecord`` satisfies this protocol as-is, which lets
    the API layer accept database rows without importing the database layer.

    Members are declared as read-only properties so that frozen dataclasses (like
    the database records and :class:`Location` itself) satisfy the protocol.
    """

    @property
    def slug(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def latitude(self) -> Decimal: ...

    @property
    def longitude(self) -> Decimal: ...

    @property
    def timezone(self) -> str: ...

    @property
    def country_code(self) -> str | None: ...


@dataclass(frozen=True, slots=True)
class Location:
    """A place to collect weather for.

    Coordinates are :class:`~decimal.Decimal` because the provider is asked for
    six decimal places and the fact table stores ``numeric(9, 6)``; keeping the
    domain honest here avoids float round-tripping through the pipeline.
    """

    slug: str
    name: str
    latitude: Decimal
    longitude: Decimal
    timezone: str = "UTC"
    country_code: str | None = None
    id: int | None = None

    def __post_init__(self) -> None:
        if not self.slug.strip():
            msg = "location slug must not be empty"
            raise ConfigurationError(msg)
        if not Decimal("-90") <= self.latitude <= Decimal("90"):
            msg = f"location {self.slug!r} has latitude {self.latitude} outside [-90, 90]"
            raise ConfigurationError(msg)
        if not Decimal("-180") <= self.longitude <= Decimal("180"):
            msg = f"location {self.slug!r} has longitude {self.longitude} outside [-180, 180]"
            raise ConfigurationError(msg)

    @classmethod
    def from_record(cls, record: LocationLike) -> Location:
        """Build a domain location from a database row or any location-like object."""
        return cls(
            slug=record.slug,
            name=record.name,
            latitude=Decimal(record.latitude),
            longitude=Decimal(record.longitude),
            timezone=record.timezone,
            country_code=record.country_code,
            id=getattr(record, "id", None),
        )

    @property
    def label(self) -> str:
        """Short, log-friendly identifier."""
        return f"{self.slug} ({self.latitude},{self.longitude})"


__all__ = ["Location", "LocationLike"]
