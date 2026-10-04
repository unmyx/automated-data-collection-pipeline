"""Wire-format models for Open-Meteo responses.

These models describe exactly what the provider sends. They are deliberately
strict, because a silent coercion here would corrupt the fact table later:

- ``extra="ignore"`` so new provider fields never break ingestion (PLAN 9.2);
- ``strict=True`` so a string where a number belongs is a contract violation
  rather than a convenience conversion;
- ``allow_inf_nan=False`` because ``NaN``/``Infinity`` are not valid JSON numbers
  (Python's decoder accepts them; RFC 8259 does not).

Unit strings come from a recorded live response (``tests/fixtures/open_meteo``),
so drift is caught by the contract tests rather than by a human reading the docs.
"""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from adcp.models.observation import HOURLY_VARIABLES

# Expected unit string per hourly field, exactly as the provider spells them.
OPEN_METEO_UNITS: Final[dict[str, str]] = {
    "time": "iso8601",
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "dew_point_2m": "°C",
    "apparent_temperature": "°C",
    "precipitation": "mm",
    "rain": "mm",
    "snowfall": "cm",
    "weather_code": "wmo code",
    "cloud_cover": "%",
    "pressure_msl": "hPa",
    "wind_speed_10m": "km/h",
    "wind_direction_10m": "°",
    "wind_gusts_10m": "km/h",
}

# Alternative spellings that mean the same unit (PLAN 9.2: normalise before
# comparing). Kept tiny on purpose: an unlisted spelling is a contract change.
UNIT_ALIASES: Final[dict[str, str]] = {
    "percent": "%",
    "c": "°C",
    "celsius": "°C",
}

_STRICT = ConfigDict(
    extra="ignore",
    frozen=True,
    strict=True,
    allow_inf_nan=False,
)


def normalise_unit(value: str) -> str:
    """Normalise a provider unit string for comparison."""
    candidate = value.strip()
    return UNIT_ALIASES.get(candidate.lower(), candidate)


class OpenMeteoHourly(BaseModel):
    """The ``hourly`` block: one array per variable, all the same length."""

    model_config = _STRICT

    time: list[str]
    temperature_2m: list[float | None] | None = None
    relative_humidity_2m: list[float | None] | None = None
    dew_point_2m: list[float | None] | None = None
    apparent_temperature: list[float | None] | None = None
    precipitation: list[float | None] | None = None
    rain: list[float | None] | None = None
    snowfall: list[float | None] | None = None
    weather_code: list[float | None] | None = None
    cloud_cover: list[float | None] | None = None
    pressure_msl: list[float | None] | None = None
    wind_speed_10m: list[float | None] | None = None
    wind_direction_10m: list[float | None] | None = None
    wind_gusts_10m: list[float | None] | None = None

    @model_validator(mode="after")
    def _arrays_share_one_length(self) -> OpenMeteoHourly:
        """Ragged arrays are a provider contract violation (PLAN 9.2)."""
        expected = len(self.time)
        mismatched = {
            name: len(series)
            for name in HOURLY_VARIABLES
            if (series := getattr(self, name)) is not None and len(series) != expected
        }
        if mismatched:
            msg = f"hourly arrays must all have {expected} entries, got {mismatched}"
            raise ValueError(msg)
        return self


class OpenMeteoResponse(BaseModel):
    """The response envelope shared by the forecast, historical, and archive APIs."""

    model_config = _STRICT

    latitude: float
    longitude: float
    utc_offset_seconds: int = 0
    timezone: str | None = None
    timezone_abbreviation: str | None = None
    elevation: float | None = None
    generationtime_ms: float | None = None
    hourly_units: dict[str, str] = Field(default_factory=dict)
    hourly: OpenMeteoHourly

    @field_validator("latitude")
    @classmethod
    def _latitude_is_a_coordinate(cls, value: float) -> float:
        if not -90.0 <= value <= 90.0:
            msg = f"latitude {value} is outside [-90, 90]"
            raise ValueError(msg)
        return value

    @field_validator("longitude")
    @classmethod
    def _longitude_is_a_coordinate(cls, value: float) -> float:
        if not -180.0 <= value <= 180.0:
            msg = f"longitude {value} is outside [-180, 180]"
            raise ValueError(msg)
        return value


__all__ = [
    "OPEN_METEO_UNITS",
    "UNIT_ALIASES",
    "OpenMeteoHourly",
    "OpenMeteoResponse",
    "normalise_unit",
]
