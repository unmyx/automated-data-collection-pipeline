"""Ports: the protocols the pipeline depends on.

``WeatherSource`` is the one protocol the pipeline genuinely needs to invert: the
API adapter implements it, so the ingestion core never imports ``adcp.api``.
Persistence is deliberately *not* hidden behind a protocol - the pipeline calls
the concrete repositories in ``adcp.db`` directly, because the transactional
behaviour (one transaction per location, watermark advanced in the same
transaction) is part of the design, not an implementation detail to swap out.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from adcp.models.location import Location
from adcp.models.observation import ObservationSource, WeatherSeries
from adcp.models.window import HourlyWindow


@runtime_checkable
class WeatherSource(Protocol):
    """Reads hourly weather series from an upstream provider.

    Implemented by :class:`adcp.api.open_meteo.OpenMeteoClient`. The pipeline talks
    to this protocol, so a recorded-fixture source or a fake can replace the real
    provider in tests without touching the pipeline.
    """

    def fetch_hourly(
        self,
        *,
        location: Location,
        source: ObservationSource,
        window: HourlyWindow,
    ) -> WeatherSeries:
        """Return the hourly series for one location, source, and window."""
        ...


__all__ = ["WeatherSource"]
