"""Domain rules in isolation: one positive and one negative case each."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from adcp.models.observation import HOURLY_VARIABLES
from adcp.validation.rules import (
    HOUR_ALIGNMENT_TOLERANCE_S,
    RANGE_RULES,
    RejectionCode,
    check_measurement,
    rejection_code_for,
    snap_to_hour,
)

pytestmark = pytest.mark.unit

HOUR = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("temperature_2m", Decimal("-90"), None),
        ("temperature_2m", Decimal("60"), None),
        ("temperature_2m", Decimal("-90.01"), "below the minimum"),
        ("temperature_2m", Decimal("60.01"), "above the maximum"),
        ("relative_humidity_2m", Decimal("0"), None),
        ("relative_humidity_2m", Decimal("101"), "above the maximum"),
        ("precipitation", Decimal("0"), None),
        ("precipitation", Decimal("-0.1"), "below the minimum"),
        ("wind_speed_10m", Decimal("500"), None),
        ("wind_speed_10m", Decimal("-1"), "below the minimum"),
        ("pressure_msl", Decimal("799.9"), "below the minimum"),
        ("pressure_msl", Decimal("1100.1"), "above the maximum"),
        ("cloud_cover", Decimal("101"), "above the maximum"),
        ("weather_code", 99, None),
        ("weather_code", 100, "above the maximum"),
        ("wind_direction_10m", 360, None),
        ("wind_direction_10m", 361, "above the maximum"),
        ("temperature_2m", None, None),
        ("unknown_variable", Decimal("999"), None),
    ],
)
def test_range_rules(name: str, value: Any, expected: str | None) -> None:
    problem = check_measurement(name, value)

    if expected is None:
        assert problem is None
    else:
        assert problem is not None
        assert expected in problem


def test_every_measurement_has_a_rule() -> None:
    assert set(RANGE_RULES) == set(HOURLY_VARIABLES)


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("precipitation", Decimal("-1"), RejectionCode.NEGATIVE_VALUE),
        ("rain", Decimal("-0.5"), RejectionCode.NEGATIVE_VALUE),
        ("snowfall", Decimal("-2"), RejectionCode.NEGATIVE_VALUE),
        ("weather_code", 150, RejectionCode.INVALID_WEATHER_CODE),
        ("weather_code", -1, RejectionCode.INVALID_WEATHER_CODE),
        ("temperature_2m", Decimal("999"), RejectionCode.OUT_OF_RANGE),
        ("pressure_msl", Decimal("200"), RejectionCode.OUT_OF_RANGE),
    ],
)
def test_rejection_codes_are_specific(
    name: str,
    value: Any,
    expected: RejectionCode,
) -> None:
    assert rejection_code_for(name, value) is expected


@pytest.mark.parametrize(
    ("minute", "second", "microsecond", "expected_snapped"),
    [
        (0, 0, 0, False),
        (30, 0, 0, False),
        (59, 59, 999_999, True),
    ],
)
def test_hour_alignment_snapping(
    minute: int,
    second: int,
    microsecond: int,
    expected_snapped: bool,
) -> None:
    moment = HOUR.replace(minute=minute, second=second, microsecond=microsecond)

    snapped, was_snapped = snap_to_hour(moment)

    assert was_snapped is expected_snapped
    if expected_snapped:
        assert snapped == HOUR + timedelta(hours=1)
    else:
        assert snapped == moment


def test_alignment_tolerance_is_one_second() -> None:
    assert HOUR_ALIGNMENT_TOLERANCE_S == 1.0
    just_outside = HOUR + timedelta(minutes=59, seconds=58)

    snapped, was_snapped = snap_to_hour(just_outside)

    assert was_snapped is False
    assert snapped == just_outside
