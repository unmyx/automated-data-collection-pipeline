"""Deterministic request construction (PLAN section 6.2)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from adcp.api.requests import (
    CELL_SELECTION,
    DEFAULT_VARIABLES,
    REQUEST_TIMEZONE,
    HourlyRequest,
    format_coordinate,
    window_label,
)
from adcp.errors import ConfigurationError
from adcp.models.observation import HOURLY_VARIABLES, ObservationSource
from adcp.models.window import DateRange, RecentWindow
from tests.support import belgrade

pytestmark = pytest.mark.unit


def test_recent_window_parameters_are_exact_and_ordered() -> None:
    request = HourlyRequest(
        location=belgrade(),
        source=ObservationSource.FORECAST,
        window=RecentWindow(past_days=2, forecast_days=1),
    )

    params = request.params()

    assert list(params) == [
        "latitude",
        "longitude",
        "hourly",
        "timezone",
        "cell_selection",
        "past_days",
        "forecast_days",
    ]
    assert params["latitude"] == "44.8125"
    assert params["longitude"] == "20.4375"
    assert params["timezone"] == REQUEST_TIMEZONE
    assert params["cell_selection"] == CELL_SELECTION
    assert params["past_days"] == "2"
    assert params["forecast_days"] == "1"
    assert params["hourly"].split(",") == list(DEFAULT_VARIABLES)


def test_date_range_parameters_use_start_and_end_dates() -> None:
    request = HourlyRequest(
        location=belgrade(),
        source=ObservationSource.ARCHIVE,
        window=DateRange(start=date(2026, 9, 20), end=date(2026, 9, 22)),
    )

    params = request.params()

    assert params["start_date"] == "2026-09-20"
    assert params["end_date"] == "2026-09-22"
    assert "past_days" not in params
    assert "forecast_days" not in params


def test_request_construction_is_deterministic() -> None:
    window = RecentWindow(past_days=3)
    first = HourlyRequest(
        location=belgrade(),
        source=ObservationSource.FORECAST,
        window=window,
    )
    second = HourlyRequest(
        location=belgrade(),
        source=ObservationSource.FORECAST,
        window=window,
    )

    assert first.params() == second.params()
    assert list(first.params()) == list(second.params())


def test_archive_requires_an_explicit_date_range() -> None:
    with pytest.raises(ConfigurationError, match="archive endpoint"):
        HourlyRequest(
            location=belgrade(),
            source=ObservationSource.ARCHIVE,
            window=RecentWindow(past_days=1),
        )


def test_unknown_variables_are_rejected_before_any_request() -> None:
    with pytest.raises(ConfigurationError, match="unsupported hourly variables"):
        HourlyRequest(
            location=belgrade(),
            source=ObservationSource.FORECAST,
            window=RecentWindow(past_days=1),
            variables=("temperature_2m", "ultraviolet_index"),
        )


def test_an_empty_variable_set_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="at least one hourly variable"):
        HourlyRequest(
            location=belgrade(),
            source=ObservationSource.FORECAST,
            window=RecentWindow(past_days=1),
            variables=(),
        )


def test_default_variables_are_the_planned_set() -> None:
    assert DEFAULT_VARIABLES == HOURLY_VARIABLES
    assert len(DEFAULT_VARIABLES) == 13
    assert "time" not in DEFAULT_VARIABLES


def test_describe_reports_context_without_parameters() -> None:
    request = HourlyRequest(
        location=belgrade(),
        source=ObservationSource.FORECAST,
        window=RecentWindow(past_days=2),
    )

    described = request.describe()

    assert described == {
        "location": "belgrade-rs",
        "source": "forecast",
        "window": "past_2d+forecast_1d",
        "variables": 13,
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("44.812500"), "44.8125"),
        (Decimal("20.437500"), "20.4375"),
        (Decimal("-21.942600"), "-21.9426"),
        (Decimal("64.000000"), "64"),
        (Decimal("0.000000"), "0"),
    ],
)
def test_coordinate_formatting_is_plain_decimal(value: Decimal, expected: str) -> None:
    assert format_coordinate(value) == expected


def test_window_labels() -> None:
    assert window_label(RecentWindow(past_days=2, forecast_days=1)) == "past_2d+forecast_1d"
    assert window_label(DateRange(date(2026, 1, 1), date(2026, 1, 3))) == "2026-01-01..2026-01-03"
