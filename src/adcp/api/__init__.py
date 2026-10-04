"""Open-Meteo API adapter.

Layout:

- :mod:`adcp.api.open_meteo` - the HTTP adapter (requests, retries, logging)
- :mod:`adcp.api.requests` - deterministic request construction
- :mod:`adcp.api.schemas` - strict wire-format models
- :mod:`adcp.api.mapping` - payload -> domain translation and anomaly handling

Nothing here imports the database layer; the API package is independently
testable with a mocked transport.
"""

from __future__ import annotations

from adcp.api.mapping import parse_hourly_response
from adcp.api.open_meteo import OpenMeteoClient, endpoint_for
from adcp.api.requests import DEFAULT_VARIABLES, HourlyRequest
from adcp.api.schemas import OPEN_METEO_UNITS, OpenMeteoHourly, OpenMeteoResponse

__all__ = [
    "DEFAULT_VARIABLES",
    "OPEN_METEO_UNITS",
    "HourlyRequest",
    "OpenMeteoClient",
    "OpenMeteoHourly",
    "OpenMeteoResponse",
    "endpoint_for",
    "parse_hourly_response",
]
