"""Domain rules from PLAN section 9.3, expressed as small pure functions.

Keeping these separate from the validator means every rule can be exercised in
isolation - one positive and one negative case each - and the validator reads as
a short sequence of questions rather than a wall of comparisons.

Two deliberate design choices:

- **Values are only rejected, never repaired.** A measurement outside its range is
  reported with the exact field and value; it is not clamped, dropped silently, or
  coerced to zero.
- **Bounds are wide on purpose.** They exist to catch provider bugs and unit
  changes (millimetres reported as centimetres, wind in m/s instead of km/h), not
  to second-guess unusual weather.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final

from adcp.models.observation import ObservationSource


class RejectionCode(StrEnum):
    """Machine-readable reason a row was rejected (PLAN sections 9.3 and 9.6)."""

    INVALID_TIMESTAMP = "InvalidTimestamp"
    NOT_HOUR_ALIGNED = "NotHourAligned"
    OUT_OF_WINDOW = "OutOfWindow"
    FUTURE_TIMESTAMP = "FutureTimestamp"
    DUPLICATE_TIMESTAMP = "DuplicateTimestamp"
    OUT_OF_RANGE = "OutOfRange"
    NEGATIVE_VALUE = "NegativeValue"
    INVALID_WEATHER_CODE = "InvalidWeatherCode"
    LOCATION_MISMATCH = "LocationMismatch"
    SOURCE_MISMATCH = "SourceMismatch"
    REJECTION_BUDGET_EXCEEDED = "RejectionBudgetExceeded"


@dataclass(frozen=True, slots=True)
class Rejection:
    """A rejected row (or payload) with everything needed to explain it.

    Carries the reason code, a human-readable message, the offending fields, the
    observation timestamp, and the location/source it came from, so the collection
    layer can persist it in ``ingestion_run_errors`` with the run that produced it
    (PLAN section 5.6) without inventing context later.
    """

    code: RejectionCode
    message: str
    slug: str
    source: ObservationSource
    observed_at: datetime | None = None
    fields: tuple[str, ...] = ()

    def as_details(self) -> dict[str, object]:
        """Structured fields for logs and error records."""
        return {
            "code": self.code.value,
            "message": self.message,
            "location": self.slug,
            "source": self.source.value,
            "observed_at": None if self.observed_at is None else self.observed_at.isoformat(),
            "fields": list(self.fields),
        }


@dataclass(frozen=True, slots=True)
class RangeRule:
    """Inclusive bounds for one measurement, plus the code to use below the floor."""

    minimum: Decimal
    maximum: Decimal
    below_min_code: RejectionCode = RejectionCode.OUT_OF_RANGE
    above_max_code: RejectionCode = RejectionCode.OUT_OF_RANGE


def _rule(
    minimum: str,
    maximum: str,
    below: RejectionCode | None = None,
    above: RejectionCode | None = None,
) -> RangeRule:
    return RangeRule(
        minimum=Decimal(minimum),
        maximum=Decimal(maximum),
        below_min_code=below or RejectionCode.OUT_OF_RANGE,
        above_max_code=above or RejectionCode.OUT_OF_RANGE,
    )


#: Inclusive bounds per variable, exactly as specified in PLAN section 9.3.
#: The upper bounds the plan omits (precipitation, rain, snowfall) are generous
#: sanity limits - a millimetre/centimetre mix-up produces values orders of
#: magnitude above any real hourly total, which is exactly what these catch.
RANGE_RULES: Final[Mapping[str, RangeRule]] = {
    "temperature_2m": _rule("-90", "60"),
    "dew_point_2m": _rule("-90", "60"),
    "apparent_temperature": _rule("-100", "70"),
    "relative_humidity_2m": _rule("0", "100"),
    "precipitation": _rule("0", "1000", RejectionCode.NEGATIVE_VALUE),
    "rain": _rule("0", "1000", RejectionCode.NEGATIVE_VALUE),
    "snowfall": _rule("0", "1000", RejectionCode.NEGATIVE_VALUE),
    "weather_code": RangeRule(
        minimum=Decimal("0"),
        maximum=Decimal("99"),
        below_min_code=RejectionCode.INVALID_WEATHER_CODE,
        above_max_code=RejectionCode.INVALID_WEATHER_CODE,
    ),
    "cloud_cover": _rule("0", "100"),
    "pressure_msl": _rule("800", "1100"),
    "wind_speed_10m": _rule("0", "500"),
    "wind_direction_10m": _rule("0", "360"),
    "wind_gusts_10m": _rule("0", "500"),
}

#: ``:59.999``-style timestamps are treated as the following hour (PLAN section 9.3).
HOUR_ALIGNMENT_TOLERANCE_S: Final[float] = 1.0

#: Clock skew allowance before a timestamp counts as being in the future.
FUTURE_TOLERANCE: Final[timedelta] = timedelta(hours=1)


def snap_to_hour(moment: datetime) -> tuple[datetime, bool]:
    """Return ``(hour, snapped)``.

    An exactly hour-aligned timestamp is returned unchanged. A timestamp within
    ``HOUR_ALIGNMENT_TOLERANCE_S`` *below* the next hour boundary is snapped up to
    it (the ``:59.999`` case). Anything else is returned unchanged with
    ``snapped=False`` so the caller can reject it as ``NotHourAligned``.
    """
    floor = moment.replace(minute=0, second=0, microsecond=0)
    if moment == floor:
        return floor, False
    ceiling = floor + timedelta(hours=1)
    if (ceiling - moment).total_seconds() <= HOUR_ALIGNMENT_TOLERANCE_S:
        return ceiling, True
    return moment, False


def check_measurement(name: str, value: Decimal | int | None) -> str | None:
    """Return a human-readable problem description, or ``None`` when in range."""
    if value is None:
        return None
    rule = RANGE_RULES.get(name)
    candidate = Decimal(value)
    if rule is None:
        return None
    if candidate < rule.minimum:
        return f"{name}={value} is below the minimum {rule.minimum}"
    if candidate > rule.maximum:
        return f"{name}={value} is above the maximum {rule.maximum}"
    return None


def rejection_code_for(name: str, value: Decimal | int) -> RejectionCode:
    """The machine-readable code for a measurement outside its bounds."""
    rule = RANGE_RULES.get(name)
    if rule is None:
        return RejectionCode.OUT_OF_RANGE
    return rule.below_min_code if Decimal(value) < rule.minimum else rule.above_max_code


__all__ = [
    "FUTURE_TOLERANCE",
    "HOUR_ALIGNMENT_TOLERANCE_S",
    "RANGE_RULES",
    "RangeRule",
    "Rejection",
    "RejectionCode",
    "check_measurement",
    "rejection_code_for",
    "snap_to_hour",
]
