"""Opt-in smoke test against the real Open-Meteo API.

Skipped unless ``ADCP_LIVE_API_TESTS=true``. It asserts shape invariants only -
never recorded values - so it cannot fail because the weather changed.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from adcp.api.open_meteo import OpenMeteoClient
from adcp.config import Settings
from adcp.models.observation import ObservationSource
from adcp.models.window import RecentWindow
from tests.support import belgrade

pytestmark = [pytest.mark.integration, pytest.mark.live]


@pytest.mark.skipif(
    os.environ.get("ADCP_LIVE_API_TESTS", "").strip().lower() not in {"1", "true", "yes"},
    reason="set ADCP_LIVE_API_TESTS=true to call the real Open-Meteo API",
)
def test_live_forecast_fetch_returns_a_consistent_series() -> None:
    settings = Settings(_env_file=None, open_meteo_max_attempts=2)

    with OpenMeteoClient(settings) as source:
        series = source.fetch_hourly(
            location=belgrade(),
            source=ObservationSource.FORECAST,
            window=RecentWindow(past_days=1),
        )

    assert series.hours >= 24
    assert series.source is ObservationSource.FORECAST
    assert series.first_observed_at is not None
    assert series.last_observed_at is not None
    assert all(observation.observed_at.tzinfo is not None for observation in series.observations)
    moments = [observation.observed_at for observation in series.observations]
    assert moments == sorted(moments), "the mapper already guarantees ordering"
    assert any(observation.temperature_2m is not None for observation in series.observations)

    horizon = datetime.now(UTC) - timedelta(days=5)
    assert series.last_observed_at > horizon
