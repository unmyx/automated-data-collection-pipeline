"""Small helpers shared by the API tests.

Kept as plain functions rather than fixtures so any test can load a recorded
Open-Meteo payload without pulling in a fixture graph.
"""

from __future__ import annotations

import io
import json
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from adcp.api.schemas import OPEN_METEO_UNITS
from adcp.models.location import Location
from adcp.models.observation import (
    ObservationSource,
    WeatherObservation,
    WeatherSeries,
)
from adcp.models.run import LocationResult, LocationStatus, RunCounts, RunStatus, RunSummary
from adcp.models.window import HourlyWindow

#: Root of the committed fixture tree.
FIXTURES_DIR = Path(__file__).parent / "fixtures"

#: Recorded Open-Meteo payloads (see scripts/record_open_meteo_fixtures.py).
OPEN_METEO_DIR = FIXTURES_DIR / "open_meteo"

#: The location all recorded fixtures were captured for; the provider snaps it to
#: the grid cell 44.817726, 20.435654, which is inside the drift tolerance.
BELGRADE_LATITUDE = Decimal("44.812500")
BELGRADE_LONGITUDE = Decimal("20.437500")

#: A fixed "now" so `fetched_at` assertions are deterministic.
FROZEN_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def belgrade() -> Location:
    """The domain location used by the API tests."""
    return Location(
        slug="belgrade-rs",
        name="Belgrade",
        latitude=BELGRADE_LATITUDE,
        longitude=BELGRADE_LONGITUDE,
        country_code="RS",
    )


def open_meteo_payload(name: str) -> dict[str, Any]:
    """Load a recorded JSON fixture by file name."""
    raw = (OPEN_METEO_DIR / name).read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):  # pragma: no cover - fixture misuse
        msg = f"fixture {name} is not a JSON object"
        raise TypeError(msg)
    return payload


def open_meteo_text(name: str) -> str:
    """Load a fixture that is deliberately not valid JSON as raw text."""
    return (OPEN_METEO_DIR / name).read_text(encoding="utf-8")


def log_events(buffer: io.StringIO) -> list[dict[str, Any]]:
    """Parse a structlog JSON stream into a list of event dictionaries."""
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


def events_named(buffer: io.StringIO, event: str) -> list[dict[str, Any]]:
    """Every logged event with this name, in order."""
    return [entry for entry in log_events(buffer) if entry.get("event") == event]


def build_observation(moment: datetime, /, **overrides: Any) -> WeatherObservation:
    """An observation with one plausible measurement and everything else null."""
    fields: dict[str, Any] = {
        "observed_at": moment,
        "temperature_2m": Decimal("12.50"),
    }
    fields.update(overrides)
    return WeatherObservation(**fields)


def build_series(  # noqa: PLR0913 - a test builder with sensible defaults
    *,
    location: Location,
    source: ObservationSource,
    moments: Sequence[datetime],
    fetched_at: datetime | None = None,
    grid_latitude: Decimal | None = None,
    grid_longitude: Decimal | None = None,
    elevation_m: Decimal | None = Decimal("75.00"),
    observation_overrides: dict[datetime, dict[str, Any]] | None = None,
) -> WeatherSeries:
    """Build a series whose observations carry one measurement each."""
    overrides = observation_overrides or {}
    observations = tuple(
        build_observation(moment, **overrides.get(moment, {})) for moment in moments
    )
    return WeatherSeries(
        location=location,
        source=source,
        observations=observations,
        grid_latitude=location.latitude if grid_latitude is None else grid_latitude,
        grid_longitude=location.longitude if grid_longitude is None else grid_longitude,
        elevation_m=elevation_m,
        upstream_timezone="GMT",
        fetched_at=fetched_at or FROZEN_NOW,
        units={"time": "iso8601"},
    )


@dataclass(frozen=True, slots=True)
class RecordedCall:
    """One call the fake source received."""

    slug: str
    source: ObservationSource
    window: HourlyWindow


@dataclass
class FakeWeatherSource:
    """An in-process :class:`adcp.ports.WeatherSource` for pipeline tests.

    The responder may return a series or raise an exception, which is how tests
    exercise per-location failure isolation without touching the network.
    """

    responder: Callable[[Location, ObservationSource, HourlyWindow], WeatherSeries]
    calls: list[RecordedCall] = field(default_factory=list)

    def fetch_hourly(
        self,
        *,
        location: Location,
        source: ObservationSource,
        window: HourlyWindow,
    ) -> WeatherSeries:
        self.calls.append(RecordedCall(slug=location.slug, source=source, window=window))
        return self.responder(location, source, window)

    def slugs(self) -> list[str]:
        return [call.slug for call in self.calls]


def constant_source(
    moments: Sequence[datetime],
    *,
    overrides: dict[datetime, dict[str, Any]] | None = None,
) -> FakeWeatherSource:
    """A fake source that answers every location with the same hours.

    Out-of-window hours are handled by the validator, so tests can pass a whole
    multi-day series and let the pipeline decide what is stored, skipped, or
    rejected.
    """

    def responder(
        location: Location,
        source: ObservationSource,
        window: HourlyWindow,
    ) -> WeatherSeries:
        del window  # the port requires it; the fixture answers with fixed hours
        return build_series(
            location=location,
            source=source,
            moments=moments,
            observation_overrides=overrides,
        )

    return FakeWeatherSource(responder=responder)


#: Plausible in-range values for every hourly variable.
DEFAULT_MEASUREMENTS: dict[str, object] = {
    "temperature_2m": 12.5,
    "relative_humidity_2m": 60,
    "dew_point_2m": 5.0,
    "apparent_temperature": 11.0,
    "precipitation": 0.0,
    "rain": 0.0,
    "snowfall": 0.0,
    "weather_code": 0,
    "cloud_cover": 20,
    "pressure_msl": 1013.2,
    "wind_speed_10m": 5.0,
    "wind_direction_10m": 180,
    "wind_gusts_10m": 8.0,
}


def open_meteo_payload_for(
    moments: Sequence[datetime],
    *,
    latitude: float = 44.8177,
    longitude: float = 20.4357,
    elevation: float = 75.0,
    overrides: dict[int, dict[str, object]] | None = None,
) -> dict[str, Any]:
    """A complete Open-Meteo response envelope for the given hours.

    Used by the CLI integration tests so the mocked HTTP response matches the
    window the pipeline is actually asking for.
    """

    def value_at(name: str, index: int) -> object:
        per_row = (overrides or {}).get(index, {})
        return per_row.get(name, DEFAULT_MEASUREMENTS[name])

    hourly: dict[str, Any] = {
        "time": [moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M") for moment in moments],
        **{
            name: [value_at(name, index) for index in range(len(moments))]
            for name in DEFAULT_MEASUREMENTS
        },
    }
    return {
        "latitude": latitude,
        "longitude": longitude,
        "generationtime_ms": 0.42,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": elevation,
        "hourly_units": dict(OPEN_METEO_UNITS),
        "hourly": hourly,
    }


def payload_for_recent_hours(
    *,
    lookback_hours: int,
    overrides: dict[int, dict[str, object]] | None = None,
) -> dict[str, Any]:
    """A payload containing exactly the hours a scheduled window will store.

    The window is computed from the wall clock (the service uses the real clock),
    so this helper builds hours relative to the current complete hour.
    """
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    moments = [end - timedelta(hours=offset) for offset in range(lookback_hours, 0, -1)]
    return open_meteo_payload_for(moments, overrides=overrides)


#: A stable run id for summaries that do not care which run they describe.
DEFAULT_RUN_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")


def run_summary(  # noqa: PLR0913 - a compact builder for the counter fields
    *,
    status: RunStatus = RunStatus.SUCCEEDED,
    slug: str = "belgrade-rs",
    run_id: uuid.UUID | None = DEFAULT_RUN_ID,
    rows_received: int = 3,
    rows_inserted: int = 3,
    rows_updated: int = 0,
    rows_unchanged: int = 0,
    rows_rejected: int = 0,
    rows_skipped: int = 0,
    locations_total: int = 1,
) -> RunSummary:
    """A run summary with plausible counters, for scheduler and CLI tests."""
    location_status = (
        LocationStatus.FAILED if status is RunStatus.FAILED else LocationStatus.SUCCEEDED
    )
    result = LocationResult(
        slug=slug,
        source=ObservationSource.FORECAST,
        status=location_status,
        rows_received=rows_received,
        rows_accepted=rows_inserted + rows_updated + rows_unchanged,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        rows_unchanged=rows_unchanged,
        rows_rejected=rows_rejected,
        rows_skipped=rows_skipped,
        error_type="UpstreamServerError" if status is RunStatus.FAILED else None,
        error_message="upstream is down" if status is RunStatus.FAILED else None,
    )
    return RunSummary(
        status=status,
        trigger="scheduler",
        started_at=FROZEN_NOW,
        finished_at=FROZEN_NOW,
        counts=RunCounts.from_results([result], locations_total=locations_total),
        run_id=run_id,
        results=(result,),
    )
