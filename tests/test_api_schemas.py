"""Wire-format models: strictness, contract drift, and the recorded fixtures."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from adcp.api.schemas import (
    OPEN_METEO_UNITS,
    OpenMeteoResponse,
    normalise_unit,
)
from adcp.models.observation import HOURLY_VARIABLES
from tests.support import open_meteo_payload

pytestmark = pytest.mark.unit


def _forecast() -> dict[str, Any]:
    return open_meteo_payload("forecast_single_location.json")


@pytest.mark.parametrize(
    "name",
    ["forecast_single_location.json", "archive_date_range.json"],
)
def test_recorded_responses_satisfy_the_wire_model(name: str) -> None:
    response = OpenMeteoResponse.model_validate(open_meteo_payload(name))

    assert len(response.hourly.time) > 0
    assert response.utc_offset_seconds == 0
    assert response.timezone is not None
    for variable in HOURLY_VARIABLES:
        assert getattr(response.hourly, variable) is not None, variable


def test_provider_omits_the_offset_when_utc_is_requested() -> None:
    """Appendix A1: recorded evidence that times arrive naive and must be localised."""
    response = OpenMeteoResponse.model_validate(_forecast())

    assert response.hourly.time[0] == "2026-09-30T00:00"
    assert "+" not in response.hourly.time[0]


def test_units_match_the_expected_contract_exactly() -> None:
    payload = _forecast()

    assert payload["hourly_units"] == OPEN_METEO_UNITS
    assert set(OPEN_METEO_UNITS) == {"time", *HOURLY_VARIABLES}


def test_unknown_provider_fields_are_ignored() -> None:
    payload = _forecast()
    payload["brand_new_metadata"] = {"nested": True}
    payload["hourly"]["a_future_variable"] = [1.0] * len(payload["hourly"]["time"])

    response = OpenMeteoResponse.model_validate(payload)

    assert response.hourly.temperature_2m is not None
    assert not hasattr(response.hourly, "a_future_variable")


def test_missing_hourly_section_is_rejected() -> None:
    payload = open_meteo_payload("forecast_missing_hourly.json")

    with pytest.raises(ValidationError, match="hourly"):
        OpenMeteoResponse.model_validate(payload)


def test_wrong_value_type_is_rejected_instead_of_coerced() -> None:
    payload = open_meteo_payload("forecast_wrong_field_type.json")

    with pytest.raises(ValidationError):
        OpenMeteoResponse.model_validate(payload)


def test_ragged_arrays_are_rejected() -> None:
    payload = open_meteo_payload("forecast_ragged_arrays.json")

    with pytest.raises(ValidationError, match="must all have"):
        OpenMeteoResponse.model_validate(payload)


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected(bad_value: float) -> None:
    """Python's json module accepts NaN/Infinity; the provider contract does not."""
    payload = _forecast()
    payload["hourly"]["temperature_2m"][0] = bad_value

    with pytest.raises(ValidationError):
        OpenMeteoResponse.model_validate(payload)


@pytest.mark.parametrize("bad_latitude", [999.0, -91.0])
def test_invalid_location_metadata_is_rejected(bad_latitude: float) -> None:
    payload = _forecast()
    payload["latitude"] = bad_latitude

    with pytest.raises(ValidationError, match="latitude"):
        OpenMeteoResponse.model_validate(payload)


def test_booleans_are_not_numbers() -> None:
    payload = _forecast()
    payload["hourly"]["cloud_cover"][0] = True

    with pytest.raises(ValidationError):
        OpenMeteoResponse.model_validate(payload)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("°C", "°C"),
        ("  °C ", "°C"),
        ("percent", "%"),
        ("%", "%"),
        ("km/h", "km/h"),
        ("wmo code", "wmo code"),
        ("metres", "metres"),
    ],
)
def test_unit_normalisation(raw: str, expected: str) -> None:
    assert normalise_unit(raw) == expected
