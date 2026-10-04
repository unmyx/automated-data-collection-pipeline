"""Translate a decoded Open-Meteo payload into domain types.

Every anomaly the provider can produce - a missing ``hourly`` block, an absent
variable, ragged arrays, a changed unit, an unparseable timestamp, out-of-order
hours, invalid location metadata, a provider error document - leaves this module
as a structured :class:`~adcp.errors.SchemaError` (or
:class:`~adcp.errors.UpstreamResponseError` for the error document) carrying the
offending field paths, ready for the collection layer to record.

Row-level *semantics* (value ranges, window membership, future timestamps) belong
to the validation layer, not to this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from adcp.api.requests import HourlyRequest
from adcp.api.schemas import (
    OPEN_METEO_UNITS,
    OpenMeteoHourly,
    OpenMeteoResponse,
    normalise_unit,
)
from adcp.errors import SchemaError, UpstreamResponseError
from adcp.models.observation import WeatherObservation, WeatherSeries

# Variables the fact table stores as smallint: reject fractional values rather
# than rounding them.
INTEGRAL_VARIABLES: frozenset[str] = frozenset({"weather_code", "wind_direction_10m"})


def parse_hourly_response(
    payload: object,
    *,
    request: HourlyRequest,
    fetched_at: datetime,
    endpoint: str,
    status_code: int | None = None,
) -> WeatherSeries:
    """Validate a decoded payload and build the domain series.

    Raises:
        SchemaError: the payload violates the provider contract.
        UpstreamResponseError: the payload is a provider error document.
    """
    context: dict[str, Any] = {"endpoint": endpoint, "status_code": status_code}
    response = _validate_envelope(payload, **context)
    _require_utc_payload(response, **context)
    _require_requested_variables(response.hourly, request, **context)
    _check_units(response, request, **context)
    moments = _parse_timestamps(response.hourly.time, **context)
    observations = _build_observations(response.hourly, moments, **context)

    return WeatherSeries(
        location=request.location,
        source=request.source,
        observations=observations,
        grid_latitude=Decimal(str(response.latitude)),
        grid_longitude=Decimal(str(response.longitude)),
        elevation_m=None if response.elevation is None else Decimal(str(response.elevation)),
        upstream_timezone=response.timezone,
        fetched_at=fetched_at,
        units=dict(response.hourly_units),
    )


def _validate_envelope(payload: object, **context: Any) -> OpenMeteoResponse:
    if isinstance(payload, Mapping) and payload.get("error") is True:
        reason = payload.get("reason")
        detail = reason if isinstance(reason, str) else "no reason supplied"
        msg = f"Open-Meteo returned an error document: {detail}"
        raise UpstreamResponseError(msg, detail=detail, **context)
    if not isinstance(payload, Mapping):
        msg = f"expected a JSON object, got {type(payload).__name__}"
        raise SchemaError(msg, **context)
    try:
        return OpenMeteoResponse.model_validate(dict(payload))
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['msg']}"
            for error in exc.errors()
        )
        field_paths = tuple(".".join(str(part) for part in error["loc"]) for error in exc.errors())
        msg = f"payload does not match the Open-Meteo contract: {problems}"
        raise SchemaError(msg, field_paths=field_paths, **context) from exc


def _require_utc_payload(response: OpenMeteoResponse, **context: Any) -> None:
    if response.utc_offset_seconds != 0:
        msg = (
            "expected UTC timestamps because timezone=UTC was requested, but the "
            f"payload reports utc_offset_seconds={response.utc_offset_seconds}"
        )
        raise SchemaError(msg, field_paths=("utc_offset_seconds",), **context)


def _require_requested_variables(
    hourly: OpenMeteoHourly,
    request: HourlyRequest,
    **context: Any,
) -> None:
    missing = tuple(f"hourly.{name}" for name in request.variables if getattr(hourly, name) is None)
    if missing:
        msg = f"response is missing requested hourly variables: {', '.join(missing)}"
        raise SchemaError(msg, field_paths=missing, **context)


def _check_units(response: OpenMeteoResponse, request: HourlyRequest, **context: Any) -> None:
    problems: list[str] = []
    for name in ("time", *request.variables):
        expected = OPEN_METEO_UNITS[name]
        actual = response.hourly_units.get(name)
        if actual is None:
            problems.append(f"{name}: missing unit (expected {expected!r})")
        elif normalise_unit(actual) != normalise_unit(expected):
            problems.append(f"{name}: expected {expected!r}, got {actual!r}")
    if problems:
        msg = f"hourly_units do not match the provider contract: {'; '.join(problems)}"
        raise SchemaError(msg, field_paths=("hourly_units",), **context)


def _parse_timestamps(values: list[str], **context: Any) -> tuple[datetime, ...]:
    moments: list[datetime] = []
    for index, value in enumerate(values):
        field = f"hourly.time[{index}]"
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            msg = f"{field} is not a valid ISO-8601 timestamp: {value!r}"
            raise SchemaError(msg, field_paths=(field,), **context) from exc
        if parsed.tzinfo is None:
            # The provider omits the offset when timezone=UTC (verified against a
            # recorded response), so localise explicitly.
            parsed = parsed.replace(tzinfo=UTC)
        elif parsed.utcoffset() != UTC.utcoffset(parsed):
            msg = f"{field} is not a UTC timestamp: {value!r}"
            raise SchemaError(msg, field_paths=(field,), **context)
        moments.append(parsed.astimezone(UTC))

    for index in range(1, len(moments)):
        if moments[index] <= moments[index - 1]:
            msg = (
                "hourly.time must be strictly increasing; "
                f"index {index} ({moments[index].isoformat()}) does not follow "
                f"index {index - 1} ({moments[index - 1].isoformat()})"
            )
            raise SchemaError(msg, field_paths=("hourly.time",), **context)
    return tuple(moments)


def _build_observations(
    hourly: OpenMeteoHourly,
    moments: tuple[datetime, ...],
    **context: Any,
) -> tuple[WeatherObservation, ...]:
    return tuple(
        _observation_from(hourly, moment=moment, index=index, context=context)
        for index, moment in enumerate(moments)
    )


def _observation_from(
    hourly: OpenMeteoHourly,
    *,
    moment: datetime,
    index: int,
    context: dict[str, Any],
) -> WeatherObservation:
    def decimal(name: str) -> Decimal | None:
        return _decimal(_series(hourly, name, index), name, index, **context)

    def integer(name: str) -> int | None:
        return _integer(_series(hourly, name, index), name, index, **context)

    return WeatherObservation(
        observed_at=moment,
        temperature_2m=decimal("temperature_2m"),
        relative_humidity_2m=decimal("relative_humidity_2m"),
        dew_point_2m=decimal("dew_point_2m"),
        apparent_temperature=decimal("apparent_temperature"),
        precipitation=decimal("precipitation"),
        rain=decimal("rain"),
        snowfall=decimal("snowfall"),
        weather_code=integer("weather_code"),
        cloud_cover=decimal("cloud_cover"),
        pressure_msl=decimal("pressure_msl"),
        wind_speed_10m=decimal("wind_speed_10m"),
        wind_direction_10m=integer("wind_direction_10m"),
        wind_gusts_10m=decimal("wind_gusts_10m"),
    )


def _series(hourly: OpenMeteoHourly, name: str, index: int) -> float | None:
    values: list[float | None] | None = getattr(hourly, name)
    if values is None:
        # Variable not present in the payload: nothing is stored for it. The
        # caller-requested set is verified before this point.
        return None
    return values[index]


def _decimal(value: float | None, name: str, index: int, **context: Any) -> Decimal | None:
    if value is None:
        return None
    field = f"hourly.{name}[{index}]"
    try:
        # str() keeps the provider's own decimal representation instead of the
        # binary float's full expansion, which is what the numeric columns and
        # the row content hash need.
        return Decimal(str(value))
    except InvalidOperation as exc:  # pragma: no cover - schema guarantees numbers
        msg = f"{field} is not a finite number: {value!r}"
        raise SchemaError(msg, field_paths=(field,), **context) from exc


def _integer(value: float | None, name: str, index: int, **context: Any) -> int | None:
    if value is None:
        return None
    field = f"hourly.{name}[{index}]"
    if value != int(value):
        msg = f"{field} must be a whole number to fit a smallint column, got {value!r}"
        raise SchemaError(msg, field_paths=(field,), **context)
    return int(value)


__all__ = ["INTEGRAL_VARIABLES", "parse_hourly_response"]
