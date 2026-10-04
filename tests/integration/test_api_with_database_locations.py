"""The adapter consumes configured locations instead of hard-coded ones."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
import respx
from sqlalchemy.engine import Engine

from adcp.api.open_meteo import OpenMeteoClient
from adcp.config import Settings
from adcp.db.repository import LocationRepository
from adcp.models.location import Location, LocationLike
from adcp.models.observation import ObservationSource
from adcp.models.window import RecentWindow
from tests.support import FROZEN_NOW, open_meteo_payload

pytestmark = pytest.mark.integration


def test_request_is_built_from_a_database_location(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)
    record = repository.create(
        slug="reykjavik-is",
        name="Reykjavik",
        latitude=Decimal("64.146600"),
        longitude=Decimal("-21.942600"),
        country_code="IS",
    )
    assert isinstance(record, LocationLike), "records satisfy the domain protocol"

    location = Location.from_record(record)
    assert location.id == record.id
    assert location.slug == "reykjavik-is"

    settings = Settings(_env_file=None, open_meteo_max_attempts=1)
    payload = open_meteo_payload("forecast_single_location.json")

    with respx.mock(assert_all_called=False) as router:
        route = router.get("https://api.open-meteo.com/v1/forecast").mock(
            return_value=httpx.Response(200, json=payload),
        )
        with OpenMeteoClient(settings, now=lambda: FROZEN_NOW) as source:
            series = source.fetch_hourly(
                location=location,
                source=ObservationSource.FORECAST,
                window=RecentWindow(past_days=2),
            )

    params = route.calls.last.request.url.params
    assert params["latitude"] == "64.1466", "coordinates come from the database row"
    assert params["longitude"] == "-21.9426"
    assert params["timezone"] == "UTC"
    assert series.location.slug == "reykjavik-is"
    assert series.hours == 72


def test_retired_locations_are_not_offered_to_the_api(db_engine: Engine) -> None:
    repository = LocationRepository(db_engine)
    repository.create(
        slug="reykjavik-is",
        name="Reykjavik",
        latitude=Decimal("64.146600"),
        longitude=Decimal("-21.942600"),
    )
    repository.set_active("reykjavik-is", is_active=False)

    active = [Location.from_record(record) for record in repository.list_active()]

    assert active == []
