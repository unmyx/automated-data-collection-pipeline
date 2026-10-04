"""Domain models: validation, drift guards with the database, and behaviour."""

from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from adcp.api.mapping import INTEGRAL_VARIABLES
from adcp.db.tables import WEATHER_SOURCE_VALUES, weather_hourly
from adcp.errors import ConfigurationError
from adcp.models.location import Location, LocationLike
from adcp.models.observation import HOURLY_VARIABLES, ObservationSource, WeatherObservation
from adcp.models.window import DateRange, RecentWindow
from tests.support import belgrade

pytestmark = pytest.mark.unit


class FakeLocationRecord:
    """A database row stand-in: same attributes, no database import needed."""

    id = 7
    slug = "belgrade-rs"
    name = "Belgrade"
    latitude = Decimal("44.812500")
    longitude = Decimal("20.437500")
    timezone = "UTC"
    country_code = "RS"


def test_location_accepts_a_record_like_object() -> None:
    record = FakeLocationRecord()
    assert isinstance(record, LocationLike)

    location = Location.from_record(record)

    assert location == dataclasses.replace(belgrade(), id=7)
    assert location.label == "belgrade-rs (44.812500,20.437500)"


@pytest.mark.parametrize(
    ("latitude", "longitude", "slug"),
    [
        (Decimal("91"), Decimal("20"), "belgrade-rs"),
        (Decimal("-90.5"), Decimal("20"), "belgrade-rs"),
        (Decimal("44"), Decimal("181"), "belgrade-rs"),
        (Decimal("44"), Decimal("-180.5"), "belgrade-rs"),
        (Decimal("44"), Decimal("20"), "  "),
    ],
)
def test_invalid_locations_are_rejected(
    latitude: Decimal,
    longitude: Decimal,
    slug: str,
) -> None:
    with pytest.raises(ConfigurationError):
        Location(slug=slug, name="Nowhere", latitude=latitude, longitude=longitude)


def test_observation_sources_match_the_database_check_constraint() -> None:
    assert {source.value for source in ObservationSource} == set(WEATHER_SOURCE_VALUES)


def test_hourly_variables_match_the_observation_dataclass() -> None:
    fields = tuple(
        field.name
        for field in dataclasses.fields(WeatherObservation)
        if field.name != "observed_at"
    )

    assert fields == HOURLY_VARIABLES, "the canonical variable order is the dataclass order"


def test_hourly_variables_match_the_fact_table_columns() -> None:
    columns = {column.name for column in weather_hourly.columns}

    assert set(HOURLY_VARIABLES) <= columns
    assert {"weather_code", "wind_direction_10m"} == INTEGRAL_VARIABLES
    for name in INTEGRAL_VARIABLES:
        assert str(weather_hourly.c[name].type) == "SMALLINT"
    assert str(weather_hourly.c.temperature_2m.type) == "NUMERIC(5, 2)"


def test_observation_requires_a_timezone_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        # Naive on purpose: this is the guard being tested.
        WeatherObservation(observed_at=datetime(2026, 10, 1, 12, 0))  # noqa: DTZ001


def test_observation_is_hashable_and_keeps_null_measurements() -> None:
    observation = WeatherObservation(observed_at=datetime(2026, 10, 1, 12, 0, tzinfo=UTC))

    assert hash(observation) == hash(
        WeatherObservation(observed_at=datetime(2026, 10, 1, 12, 0, tzinfo=UTC)),
    )
    assert observation.values()["temperature_2m"] is None
    assert list(observation.values()) == list(HOURLY_VARIABLES)


def test_date_range_rejects_an_inverted_range() -> None:
    with pytest.raises(ConfigurationError, match="precedes start"):
        DateRange(start=date(2026, 2, 1), end=date(2026, 1, 31))


@pytest.mark.parametrize(
    ("past_days", "forecast_days"),
    [(-1, 1), (93, 1), (0, 17), (0, 0)],
)
def test_recent_window_bounds_are_enforced(past_days: int, forecast_days: int) -> None:
    with pytest.raises(ConfigurationError):
        RecentWindow(past_days=past_days, forecast_days=forecast_days)


def test_date_range_day_count_is_inclusive() -> None:
    assert DateRange(date(2026, 9, 20), date(2026, 9, 22)).days == 3
