"""Open-Meteo adapter: HTTP behaviour, retry classification, and logging.

Every request goes through ``respx`` - there is no live network access in the
unit suite. ``respx.mock`` also raises on unmocked requests, so a test can never
silently reach the internet.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
import respx
from pydantic import ValidationError

from adcp.api.open_meteo import MAX_RESPONSE_BYTES, OpenMeteoClient, endpoint_for
from adcp.config import Settings
from adcp.errors import (
    ConfigurationError,
    MalformedJsonError,
    PayloadTooLargeError,
    SchemaError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamRateLimitedError,
    UpstreamResponseError,
    UpstreamServerError,
    UpstreamTimeoutError,
)
from adcp.logging import configure_logging
from adcp.models.observation import HOURLY_VARIABLES, ObservationSource
from adcp.models.window import DateRange, RecentWindow
from adcp.ports import WeatherSource
from tests.support import FROZEN_NOW, belgrade, events_named, open_meteo_payload, open_meteo_text

pytestmark = pytest.mark.unit

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HISTORICAL_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"


@pytest.fixture
def settings() -> Settings:
    """Fast, deterministic retry settings: tiny backoff, injected sleeps."""
    return Settings(
        _env_file=None,
        open_meteo_max_attempts=3,
        open_meteo_backoff_initial_s=0.001,
        open_meteo_backoff_max_s=0.01,
        open_meteo_timeout_total_s=30.0,
    )


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def logs() -> io.StringIO:
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG",
        log_format="json",
        service="adcp",
        environment="test",
        stream=buffer,
    )
    return buffer


@pytest.fixture
def client(
    settings: Settings,
    sleeps: list[float],
    logs: io.StringIO,
) -> Iterator[OpenMeteoClient]:
    instance = OpenMeteoClient(settings, sleep=sleeps.append, now=lambda: FROZEN_NOW)
    yield instance
    instance.close()


@pytest.fixture
def router() -> Iterator[respx.Router]:
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


def ok(
    router: respx.Router,
    *,
    url: str = FORECAST_URL,
    payload: dict[str, Any] | None = None,
) -> respx.Route:
    body = payload if payload is not None else open_meteo_payload("forecast_single_location.json")
    return router.get(url).mock(return_value=httpx.Response(200, json=body))


def failing(router: respx.Router, response: httpx.Response | Exception) -> respx.Route:
    route = router.get(FORECAST_URL)
    if isinstance(response, Exception):
        route.mock(side_effect=response)
    else:
        route.mock(return_value=response)
    return route


def forecast_request(**overrides: Any) -> dict[str, Any]:
    request: dict[str, Any] = {
        "location": belgrade(),
        "source": ObservationSource.FORECAST,
        "window": RecentWindow(past_days=2),
    }
    request.update(overrides)
    return request


def test_successful_fetch_returns_a_domain_series(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    route = ok(router)

    series = client.fetch_hourly(**forecast_request())

    assert route.called
    assert series.hours == 72
    assert series.source is ObservationSource.FORECAST
    assert series.location == belgrade()
    assert series.fetched_at == FROZEN_NOW
    assert series.first_observed_at == datetime(2026, 9, 30, 0, 0, tzinfo=UTC)
    assert series.last_observed_at == datetime(2026, 10, 2, 23, 0, tzinfo=UTC)
    assert series.grid_latitude == Decimal("44.817726")
    assert series.grid_longitude == Decimal("20.435654")
    assert series.elevation_m == Decimal("75.0")
    assert series.upstream_timezone == "GMT"
    assert series.units["temperature_2m"] == "°C"
    first = series.observations[0]
    assert first.temperature_2m == Decimal("18.3")
    assert first.relative_humidity_2m == Decimal("51.0")
    assert first.weather_code == 0
    assert isinstance(first.weather_code, int)
    assert first.wind_direction_10m == 131


def test_request_construction_reaches_the_provider(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    route = ok(router)

    client.fetch_hourly(**forecast_request())

    request = route.calls.last.request
    params = request.url.params
    assert request.url.host == "api.open-meteo.com"
    assert params["latitude"] == "44.8125"
    assert params["longitude"] == "20.4375"
    assert params["timezone"] == "UTC"
    assert params["cell_selection"] == "nearest"
    assert params["past_days"] == "2"
    assert params["forecast_days"] == "1"
    assert params["hourly"].split(",") == list(HOURLY_VARIABLES)
    assert request.headers["accept"] == "application/json"
    assert request.headers["user-agent"].startswith("adcp/")
    assert "apikey" not in params


def test_base_url_is_configurable(router: respx.Router, sleeps: list[float]) -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_forecast_url="https://open-meteo.internal.example/v1/forecast",
        open_meteo_max_attempts=1,
    )
    route = ok(router, url="https://open-meteo.internal.example/v1/forecast")

    with OpenMeteoClient(settings, sleep=sleeps.append, now=lambda: FROZEN_NOW) as source:
        series = source.fetch_hourly(**forecast_request())

    assert route.called
    assert series.hours == 72


@pytest.mark.parametrize(
    ("source", "url"),
    [
        (ObservationSource.FORECAST, FORECAST_URL),
        (ObservationSource.HISTORICAL_FORECAST, HISTORICAL_URL),
    ],
)
def test_source_selects_the_endpoint(
    router: respx.Router,
    client: OpenMeteoClient,
    source: ObservationSource,
    url: str,
) -> None:
    route = ok(router, url=url)

    series = client.fetch_hourly(
        location=belgrade(),
        source=source,
        window=RecentWindow(past_days=1),
    )

    assert route.called
    assert series.source is source


def test_archive_source_uses_a_date_range(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    route = ok(router, url=ARCHIVE_URL, payload=open_meteo_payload("archive_date_range.json"))

    series = client.fetch_hourly(
        location=belgrade(),
        source=ObservationSource.ARCHIVE,
        window=DateRange(start=date(2026, 9, 23), end=date(2026, 9, 25)),
    )

    params = route.calls.last.request.url.params
    assert params["start_date"] == "2026-09-23"
    assert params["end_date"] == "2026-09-25"
    assert "past_days" not in params
    assert series.source is ObservationSource.ARCHIVE
    assert series.hours > 0


def test_a_malformed_endpoint_cannot_even_be_configured() -> None:
    """Invalid endpoints fail at configuration time, before any request exists."""
    with pytest.raises(ValidationError):
        Settings(_env_file=None, open_meteo_forecast_url="not-a-url")


def test_read_timeout_is_retried_and_then_reported(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
    logs: io.StringIO,
) -> None:
    route = failing(router, httpx.ReadTimeout("read timed out"))

    with pytest.raises(UpstreamTimeoutError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 3, "max_attempts is three in these tests"
    assert len(sleeps) == 2
    assert "read timed out" not in str(excinfo.value)
    assert len(events_named(logs, "api.request.retry")) == 2
    failed = events_named(logs, "api.request.failed")
    assert len(failed) == 1
    assert failed[0]["attempts"] == 3
    assert failed[0]["error_type"] == "UpstreamTimeoutError"


def test_connection_failure_is_retried(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    route = failing(router, httpx.ConnectError("connection refused"))

    with pytest.raises(UpstreamConnectionError):
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 3
    assert len(sleeps) == 2


def test_transient_server_error_then_success(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
    logs: io.StringIO,
) -> None:
    route = router.get(FORECAST_URL).mock(
        side_effect=[
            httpx.Response(503, json={"error": True, "reason": "temporarily unavailable"}),
            httpx.Response(200, json=open_meteo_payload("forecast_single_location.json")),
        ],
    )

    series = client.fetch_hourly(**forecast_request())

    assert route.call_count == 2
    assert len(sleeps) == 1
    assert series.hours == 72
    assert events_named(logs, "api.request.failed") == []
    retries = events_named(logs, "api.request.retry")
    assert len(retries) == 1
    assert retries[0]["http_status"] == 503
    assert retries[0]["error_type"] == "UpstreamServerError"
    assert retries[0]["attempt"] == 1


def test_server_error_exhausts_the_retry_policy(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    route = failing(router, httpx.Response(500, text="kaboom"))

    with pytest.raises(UpstreamServerError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 3
    assert len(sleeps) == 2
    assert excinfo.value.status_code == 500


def test_rate_limit_honours_retry_after(
    router: respx.Router,
    sleeps: list[float],
) -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=3,
        open_meteo_backoff_initial_s=0.001,
        open_meteo_backoff_max_s=30.0,
        open_meteo_timeout_total_s=120.0,
    )
    route = router.get(FORECAST_URL).mock(
        side_effect=[
            httpx.Response(
                429,
                json={"error": True, "reason": "Minutely API request limit exceeded."},
                headers={"Retry-After": "42"},
            ),
            httpx.Response(200, json=open_meteo_payload("forecast_single_location.json")),
        ],
    )

    with OpenMeteoClient(settings, sleep=sleeps.append, now=lambda: FROZEN_NOW) as source:
        series = source.fetch_hourly(**forecast_request())

    assert route.call_count == 2
    assert sleeps == [30.0], "the server hint is capped by the backoff maximum"
    assert series.hours == 72


def test_rate_limit_hint_below_the_cap_is_used_as_is(
    router: respx.Router,
    sleeps: list[float],
) -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=2,
        open_meteo_backoff_max_s=30.0,
        open_meteo_timeout_total_s=120.0,
    )
    route = router.get(FORECAST_URL).mock(
        side_effect=[
            httpx.Response(429, json={"error": True}, headers={"Retry-After": "5"}),
            httpx.Response(200, json=open_meteo_payload("forecast_single_location.json")),
        ],
    )

    with OpenMeteoClient(settings, sleep=sleeps.append) as source:
        source.fetch_hourly(**forecast_request())

    assert route.call_count == 2
    assert sleeps == [5.0]


def test_rate_limited_error_carries_the_capped_hint(
    router: respx.Router,
    sleeps: list[float],
) -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=1,
        open_meteo_backoff_max_s=20.0,
    )
    failing(
        router,
        httpx.Response(429, json={"error": True}, headers={"Retry-After": "900"}),
    )

    with (
        OpenMeteoClient(settings, sleep=sleeps.append) as source,
        pytest.raises(UpstreamRateLimitedError) as excinfo,
    ):
        source.fetch_hourly(**forecast_request())

    assert excinfo.value.retry_after_s == 20.0
    assert excinfo.value.status_code == 429


def test_permanent_client_error_is_not_retried(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    recorded = open_meteo_payload("error_invalid_coordinates.json")
    route = failing(
        router,
        httpx.Response(recorded["http_status"], json=recorded["body"]),
    )

    with pytest.raises(UpstreamClientError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1, "a 4xx cannot become true by waiting"
    assert sleeps == []
    assert excinfo.value.status_code == 400
    assert "Latitude must be in range" in str(excinfo.value)


def test_malformed_json_is_not_retried(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    route = failing(
        router,
        httpx.Response(200, content=open_meteo_text("error_malformed_json.json")),
    )

    with pytest.raises(MalformedJsonError):
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("fixture", "message"),
    [
        ("forecast_missing_hourly.json", "hourly"),
        ("forecast_missing_variable.json", "wind_gusts_10m"),
        ("forecast_ragged_arrays.json", "must all have"),
        ("forecast_wrong_field_type.json", "temperature_2m"),
        ("forecast_invalid_timestamp.json", "ISO-8601"),
        ("forecast_non_monotonic.json", "strictly increasing"),
        ("forecast_invalid_location_metadata.json", "latitude"),
        ("forecast_units_changed.json", "hourly_units"),
    ],
)
def test_payload_anomalies_become_structured_schema_errors(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
    fixture: str,
    message: str,
) -> None:
    route = ok(router, payload=open_meteo_payload(fixture))

    with pytest.raises(SchemaError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1, "schema violations are never retried"
    assert sleeps == []
    assert message in str(excinfo.value)
    assert excinfo.value.field_paths, "the failure points at the offending field"
    assert excinfo.value.endpoint is not None


def test_missing_variable_reports_the_field_path(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    ok(router, payload=open_meteo_payload("forecast_missing_variable.json"))

    with pytest.raises(SchemaError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert excinfo.value.field_paths == ("hourly.wind_gusts_10m",)


def test_nulls_are_preserved_as_missing_measurements(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    ok(router, payload=open_meteo_payload("forecast_with_nulls.json"))

    series = client.fetch_hourly(**forecast_request())

    assert series.observations[0].precipitation is None
    assert series.observations[0].snowfall is None
    assert series.observations[0].temperature_2m is not None


def test_provider_error_document_with_http_200_is_reported(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    route = ok(router, payload=open_meteo_payload("error_provider_error_document.json"))

    with pytest.raises(UpstreamResponseError) as excinfo:
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1
    assert sleeps == []
    assert "Minutely API request limit exceeded" in str(excinfo.value)


def test_declared_oversized_body_is_rejected(
    router: respx.Router,
    client: OpenMeteoClient,
    sleeps: list[float],
) -> None:
    route = failing(
        router,
        httpx.Response(
            200,
            headers={"Content-Length": str(MAX_RESPONSE_BYTES + 1)},
            json={"latitude": 1.0, "longitude": 1.0},
        ),
    )

    with pytest.raises(PayloadTooLargeError):
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1
    assert sleeps == []


def test_streamed_oversized_body_is_rejected(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    route = failing(router, httpx.Response(200, content=b"x" * (MAX_RESPONSE_BYTES + 1)))

    with pytest.raises(PayloadTooLargeError):
        client.fetch_hourly(**forecast_request())

    assert route.call_count == 1


def test_api_key_is_sent_but_never_logged(
    router: respx.Router,
    logs: io.StringIO,
) -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_api_key="secret-key-123",
        open_meteo_max_attempts=1,
    )
    route = failing(router, httpx.Response(500, json={"error": True, "reason": "boom"}))

    with (
        OpenMeteoClient(settings, sleep=lambda _delay: None) as source,
        pytest.raises(UpstreamServerError) as excinfo,
    ):
        source.fetch_hourly(**forecast_request())

    sent = route.calls.last.request.url.params
    assert sent["apikey"] == "secret-key-123"
    assert "secret-key-123" not in logs.getvalue()
    assert "secret-key-123" not in str(excinfo.value)
    assert "secret-key-123" not in repr(excinfo.value.as_details())
    assert "apikey=***" in repr(excinfo.value.as_details()["endpoint"]) or "apikey" not in str(
        excinfo.value.as_details()["endpoint"],
    )


def test_provider_echoing_the_api_key_is_scrubbed(
    router: respx.Router,
    logs: io.StringIO,
) -> None:
    """A provider that echoes the key back must not leak it into our records."""
    settings = Settings(
        _env_file=None,
        open_meteo_api_key="secret-key-123",
        open_meteo_max_attempts=1,
    )
    failing(
        router,
        httpx.Response(
            500,
            json={"error": True, "reason": "Invalid key secret-key-123 supplied"},
        ),
    )

    with (
        OpenMeteoClient(settings, sleep=lambda _delay: None) as source,
        pytest.raises(UpstreamServerError) as excinfo,
    ):
        source.fetch_hourly(**forecast_request())

    assert "secret-key-123" not in str(excinfo.value)
    assert "secret-key-123" not in logs.getvalue()
    assert excinfo.value.detail == "Invalid key *** supplied"
    assert "secret-key-123" not in str(excinfo.value.as_details())


def test_coordinate_drift_is_logged_but_tolerated(
    router: respx.Router,
    client: OpenMeteoClient,
    logs: io.StringIO,
) -> None:
    payload = open_meteo_payload("forecast_single_location.json")
    payload["latitude"] = 45.5  # ~0.7 degrees from the requested point
    ok(router, payload=payload)

    series = client.fetch_hourly(**forecast_request())

    assert series.hours == 72
    warnings = events_named(logs, "api.response.coordinate_drift")
    assert len(warnings) == 1
    assert warnings[0]["drift_deg"] == pytest.approx(0.6875, abs=1e-6)
    assert warnings[0]["location"] == "belgrade-rs"


def test_success_is_logged_with_structured_fields(
    router: respx.Router,
    client: OpenMeteoClient,
    logs: io.StringIO,
) -> None:
    ok(router)

    client.fetch_hourly(**forecast_request())

    started = events_named(logs, "api.request.started")
    completed = events_named(logs, "api.request.completed")
    assert len(started) == 1
    assert len(completed) == 1
    assert started[0]["location"] == "belgrade-rs"
    assert started[0]["source"] == "forecast"
    assert started[0]["window"] == "past_2d+forecast_1d"
    assert completed[0]["rows"] == 72
    assert completed[0]["status_code"] == 200
    assert completed[0]["duration_ms"] >= 0
    assert "apikey" not in started[0]["endpoint"]


def test_client_implements_the_weather_source_port(client: OpenMeteoClient) -> None:
    assert isinstance(client, WeatherSource)


def test_injected_http_client_is_not_closed_by_the_adapter(settings: Settings) -> None:
    injected = httpx.Client()
    adapter = OpenMeteoClient(settings, http_client=injected)
    try:
        adapter.close()
        assert not injected.is_closed, "the adapter only closes what it created"
    finally:
        injected.close()


def test_owned_http_client_is_closed(settings: Settings) -> None:
    adapter = OpenMeteoClient(settings)
    adapter.close()

    assert adapter._client.is_closed


def test_endpoint_for_rejects_an_unknown_source(settings: Settings) -> None:
    with pytest.raises(ConfigurationError, match="unsupported observation source"):
        endpoint_for(settings, cast(ObservationSource, "radiosonde"))


def test_a_json_array_instead_of_an_object_is_rejected(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    router.get(FORECAST_URL).mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    with pytest.raises(SchemaError, match="expected a JSON object"):
        client.fetch_hourly(**forecast_request())


def test_error_document_without_a_reason_is_still_reported(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    ok(router, payload={"error": True})

    with pytest.raises(UpstreamResponseError, match="no reason supplied"):
        client.fetch_hourly(**forecast_request())


def test_timestamps_with_a_non_utc_offset_are_rejected(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    payload = open_meteo_payload("forecast_single_location.json")
    payload["hourly"]["time"][0] = "2026-09-30T00:00+02:00"
    ok(router, payload=payload)

    with pytest.raises(SchemaError, match="not a UTC timestamp") as excinfo:
        client.fetch_hourly(**forecast_request())

    assert excinfo.value.field_paths == ("hourly.time[0]",)


def test_missing_units_are_reported_per_variable(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    payload = open_meteo_payload("forecast_single_location.json")
    payload["hourly_units"] = {}
    ok(router, payload=payload)

    with pytest.raises(SchemaError, match="missing unit") as excinfo:
        client.fetch_hourly(**forecast_request())

    assert excinfo.value.field_paths == ("hourly_units",)


def test_fractional_values_for_integer_columns_are_rejected(
    router: respx.Router,
    client: OpenMeteoClient,
) -> None:
    payload = open_meteo_payload("forecast_single_location.json")
    payload["hourly"]["weather_code"][0] = 3.5
    ok(router, payload=payload)

    with pytest.raises(SchemaError, match="whole number") as excinfo:
        client.fetch_hourly(**forecast_request())

    assert excinfo.value.field_paths == ("hourly.weather_code[0]",)
