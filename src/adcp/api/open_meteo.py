"""The Open-Meteo adapter: HTTP, retries, logging, and payload translation.

One instance per run (PLAN 6.3), used as a context manager so the connection pool
is closed deterministically::

    with OpenMeteoClient(settings) as source:
        series = source.fetch_hourly(
            location=location,
            source=ObservationSource.FORECAST,
            window=RecentWindow(past_days=2),
        )

The adapter owns nothing else: it performs one GET, classifies the outcome, and
returns a :class:`~adcp.models.observation.WeatherSeries`. Retry classification
lives in :mod:`adcp.resilience`, payload translation in :mod:`adcp.api.mapping`,
and persistence belongs to the database layer - which this module never imports.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Final

import httpx
from structlog.stdlib import BoundLogger
from tenacity import RetryCallState

from adcp.api.mapping import parse_hourly_response
from adcp.api.requests import HourlyRequest
from adcp.config import Settings
from adcp.errors import (
    ConfigurationError,
    MalformedJsonError,
    PayloadTooLargeError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamError,
    UpstreamRateLimitedError,
    UpstreamResponseError,
    UpstreamServerError,
    UpstreamTimeoutError,
)
from adcp.logging import get_logger, mask_credentials_in_text
from adcp.models.location import Location
from adcp.models.observation import ObservationSource, WeatherSeries
from adcp.models.run import RequestStats
from adcp.models.window import HourlyWindow
from adcp.resilience import build_limits, build_retrying, build_timeout, parse_retry_after

# Redirects are followed a bounded number of times (PLAN 6.3).
MAX_REDIRECTS: Final[int] = 3

# A pathological payload is rejected instead of parsed (PLAN 6.3).
MAX_RESPONSE_BYTES: Final[int] = 5 * 1024 * 1024

# Commercial API key parameter; never logged or echoed in errors.
API_KEY_PARAM: Final[str] = "apikey"

# Logged when the provider's grid cell drifts further than this from the request.
COORDINATE_DRIFT_TOLERANCE_DEG: Final[Decimal] = Decimal("0.25")

# How much of a provider error reason is echoed into an error record.
DETAIL_EXCERPT_CHARS: Final[int] = 500

_ENDPOINT_SETTINGS: Final[dict[ObservationSource, str]] = {
    ObservationSource.FORECAST: "open_meteo_forecast_url",
    ObservationSource.HISTORICAL_FORECAST: "open_meteo_historical_forecast_url",
    ObservationSource.ARCHIVE: "open_meteo_archive_url",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def endpoint_for(settings: Settings, source: ObservationSource) -> str:
    """Resolve the configured endpoint for an observation source."""
    attribute = _ENDPOINT_SETTINGS.get(source)
    if attribute is None:
        msg = f"unsupported observation source {source!r}"
        raise ConfigurationError(msg)
    return str(getattr(settings, attribute))


class ClientStats:
    """Thread-safe counters shared by every fetch a client performs.

    The collection service snapshots this around a run to fill
    ``requests_made``/``requests_retried`` in ``ingestion_runs`` (PLAN section 5.5).
    A lock is used rather than a bare integer because per-location work is
    concurrent (ADR-006).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_made = 0
        self._requests_retried = 0

    def record_attempt(self) -> None:
        with self._lock:
            self._requests_made += 1

    def record_retry(self) -> None:
        with self._lock:
            self._requests_retried += 1

    def snapshot(self) -> RequestStats:
        with self._lock:
            return RequestStats(
                requests_made=self._requests_made,
                requests_retried=self._requests_retried,
            )


class OpenMeteoClient:
    """Synchronous Open-Meteo client implementing ``adcp.ports.WeatherSource``."""

    def __init__(  # noqa: PLR0913 - explicit injection points, all keyword-only
        self,
        settings: Settings,
        *,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = _utc_now,
        logger: BoundLogger | None = None,
        stats: ClientStats | None = None,
    ) -> None:
        self._settings = settings
        self._logger = logger if logger is not None else get_logger(__name__)
        self._sleep = sleep
        self._clock = clock
        self._now = now
        self._stats = stats if stats is not None else ClientStats()
        self._owns_client = http_client is None
        self._client = http_client if http_client is not None else self._build_client()

    def _build_client(self) -> httpx.Client:
        return httpx.Client(
            timeout=build_timeout(self._settings),
            limits=build_limits(self._settings),
            follow_redirects=True,
            max_redirects=MAX_REDIRECTS,
            headers={
                "User-Agent": self._settings.open_meteo_user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
            },
        )

    def __enter__(self) -> OpenMeteoClient:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying client if this instance created it."""
        if self._owns_client:
            self._client.close()

    @property
    def stats(self) -> ClientStats:
        """Request counters, for run-level accounting."""
        return self._stats

    def fetch_hourly(
        self,
        *,
        location: Location,
        source: ObservationSource,
        window: HourlyWindow,
    ) -> WeatherSeries:
        """Port-shaped convenience wrapper around :meth:`fetch`."""
        return self.fetch(HourlyRequest(location=location, source=source, window=window))

    def fetch(self, request: HourlyRequest) -> WeatherSeries:
        """Fetch one location/window, retrying only transient failures.

        Raises:
            ConfigurationError: the request or endpoint is invalid (never retried).
            SchemaError: the payload violates the provider contract (never retried).
            UpstreamError: any other upstream failure, after the retry policy is
                exhausted.
        """
        endpoint = endpoint_for(self._settings, request.source)
        retrying = build_retrying(
            self._settings,
            sleep=self._sleep,
            clock=self._clock,
            on_retry=lambda state: self._log_retry(state, request, endpoint),
        )
        try:
            for attempt in retrying:
                with attempt:
                    return self._fetch_once(request, endpoint)
        except UpstreamError as exc:
            self._logger.error(  # noqa: TRY400 - structlog API, not stdlib logging
                "api.request.failed",
                attempts=int(retrying.statistics.get("attempt_number", 1)),
                **request.describe(),
                **exc.as_details(),
            )
            raise
        msg = "retry loop finished without a result"  # pragma: no cover - defensive
        raise UpstreamError(msg)  # pragma: no cover - defensive

    def _fetch_once(self, request: HourlyRequest, endpoint: str) -> WeatherSeries:
        self._stats.record_attempt()
        params = self._query_params(request)
        display_url = _display_url(endpoint, params)
        started = time.perf_counter()
        self._logger.info(
            "api.request.started",
            endpoint=display_url,
            param_names=sorted(params),
            timeout_s=self._settings.open_meteo_timeout_total_s,
            **request.describe(),
        )

        try:
            with self._client.stream("GET", endpoint, params=params) as response:
                status = response.status_code
                raw = self._read_body(response, display_url)
                self._raise_for_status(status, response.headers, raw, display_url)
                series = parse_hourly_response(
                    self._decode_json(raw, display_url, status),
                    request=request,
                    fetched_at=self._now().astimezone(UTC),
                    endpoint=display_url,
                    status_code=status,
                )
        except httpx.TimeoutException as exc:
            msg = f"timeout talking to {display_url} ({exc.__class__.__name__})"
            raise UpstreamTimeoutError(
                msg,
                endpoint=display_url,
                detail=self._redact(str(exc)),
            ) from exc
        except (httpx.InvalidURL, httpx.UnsupportedProtocol) as exc:  # pragma: no cover
            # Settings validate the endpoint as an http(s) URL long before it
            # reaches httpx; this is defence in depth, and it must stay
            # non-retryable.
            msg = f"invalid Open-Meteo endpoint {display_url}: {exc}"
            raise ConfigurationError(msg) from exc
        except httpx.TransportError as exc:
            msg = f"connection failure talking to {display_url} ({exc.__class__.__name__})"
            raise UpstreamConnectionError(
                msg,
                endpoint=display_url,
                detail=self._redact(str(exc)),
            ) from exc
        except httpx.HTTPError as exc:  # pragma: no cover - defensive
            msg = f"HTTP failure talking to {display_url} ({exc.__class__.__name__})"
            raise UpstreamConnectionError(
                msg,
                endpoint=display_url,
                detail=self._redact(str(exc)),
            ) from exc

        duration_ms = round((time.perf_counter() - started) * 1_000, 2)
        self._logger.info(
            "api.request.completed",
            endpoint=display_url,
            status_code=status,
            rows=series.hours,
            bytes=len(raw),
            duration_ms=duration_ms,
            **request.describe(),
        )
        self._warn_on_coordinate_drift(series, request, display_url)
        return series

    def _query_params(self, request: HourlyRequest) -> dict[str, str]:
        params = request.params()
        api_key = self._settings.open_meteo_api_key
        if api_key is not None and api_key.get_secret_value():
            params[API_KEY_PARAM] = api_key.get_secret_value()
        return params

    def _redact(self, text: str) -> str:
        """Remove credentials from provider text before it reaches a log or error.

        Two passes: the configured API key *value* (so even a bare echo of the key
        is scrubbed) and the generic credential patterns handled by
        :func:`adcp.logging.mask_credentials_in_text`.
        """
        api_key = self._settings.open_meteo_api_key
        if api_key is not None:
            secret = api_key.get_secret_value()
            if secret:
                text = text.replace(secret, "***")
        return mask_credentials_in_text(text)

    def _read_body(self, response: httpx.Response, display_url: str) -> bytes:
        """Read the body with a hard size cap, streaming so the cap is real."""
        declared = response.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
            msg = f"response declares {declared} bytes, above the {MAX_RESPONSE_BYTES} byte cap"
            raise PayloadTooLargeError(
                msg,
                endpoint=display_url,
                status_code=response.status_code,
            )

        body = bytearray()
        for chunk in response.iter_bytes():
            body.extend(chunk)
            if len(body) > MAX_RESPONSE_BYTES:
                msg = f"response body exceeded the {MAX_RESPONSE_BYTES} byte cap"
                raise PayloadTooLargeError(
                    msg,
                    endpoint=display_url,
                    status_code=response.status_code,
                )
        return bytes(body)

    def _raise_for_status(
        self,
        status: int,
        headers: Mapping[str, str],
        raw: bytes,
        display_url: str,
    ) -> None:
        """Classify a non-2xx response into the retry taxonomy (PLAN 8.2)."""
        if 200 <= status < 300:
            return
        reason = _provider_reason(raw)
        detail = None if reason is None else self._redact(reason)
        if status == 429:
            hint = parse_retry_after(headers.get("retry-after"))
            capped = None if hint is None else min(hint, self._settings.open_meteo_backoff_max_s)
            msg = "Open-Meteo rate limited the request (HTTP 429)"
            raise UpstreamRateLimitedError(
                msg,
                retry_after_s=capped,
                endpoint=display_url,
                status_code=status,
                detail=detail,
            )
        if status >= 500:
            msg = f"Open-Meteo returned a server error (HTTP {status})"
            raise UpstreamServerError(
                msg,
                endpoint=display_url,
                status_code=status,
                detail=detail,
            )
        if status >= 400:
            msg = f"Open-Meteo rejected the request (HTTP {status})"
            if detail:
                msg = f"{msg}: {detail}"
            raise UpstreamClientError(
                msg,
                endpoint=display_url,
                status_code=status,
                detail=detail,
            )
        msg = f"unexpected HTTP status {status}"
        raise UpstreamResponseError(
            msg,
            endpoint=display_url,
            status_code=status,
            detail=detail,
        )

    def _decode_json(self, raw: bytes, display_url: str, status: int) -> object:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            msg = f"response body is not valid JSON: {exc}"
            raise MalformedJsonError(
                msg,
                endpoint=display_url,
                status_code=status,
            ) from exc

    def _log_retry(
        self,
        retry_state: RetryCallState,
        request: HourlyRequest,
        endpoint: str,
    ) -> None:
        outcome = retry_state.outcome
        exception = outcome.exception() if outcome is not None else None
        self._stats.record_retry()
        self._logger.warning(
            "api.request.retry",
            endpoint=endpoint,
            attempt=retry_state.attempt_number,
            max_attempts=self._settings.open_meteo_max_attempts,
            delay_s=round(float(retry_state.upcoming_sleep), 3),
            error_type=type(exception).__name__ if exception is not None else None,
            http_status=getattr(exception, "status_code", None),
            **request.describe(),
        )

    def _warn_on_coordinate_drift(
        self,
        series: WeatherSeries,
        request: HourlyRequest,
        display_url: str,
    ) -> None:
        latitude_drift = abs(series.grid_latitude - request.location.latitude)
        longitude_drift = abs(series.grid_longitude - request.location.longitude)
        drift = max(latitude_drift, longitude_drift)
        if drift <= COORDINATE_DRIFT_TOLERANCE_DEG:
            return
        self._logger.warning(
            "api.response.coordinate_drift",
            endpoint=display_url,
            drift_deg=float(drift),
            requested_latitude=str(request.location.latitude),
            requested_longitude=str(request.location.longitude),
            grid_latitude=str(series.grid_latitude),
            grid_longitude=str(series.grid_longitude),
            **request.describe(),
        )


def _display_url(endpoint: str, params: Mapping[str, str]) -> str:
    """Request URL with credentials removed - the only URL that may be logged."""
    safe_params = {key: value for key, value in params.items() if key != API_KEY_PARAM}
    return str(httpx.URL(endpoint, params=safe_params))


def _provider_reason(raw: bytes) -> str | None:
    """Extract a provider error reason from a response body, if there is one."""
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    reason = payload.get("reason")
    if not isinstance(reason, str):
        return None
    return reason[:DETAIL_EXCERPT_CHARS]


__all__ = [
    "API_KEY_PARAM",
    "COORDINATE_DRIFT_TOLERANCE_DEG",
    "MAX_REDIRECTS",
    "MAX_RESPONSE_BYTES",
    "ClientStats",
    "OpenMeteoClient",
    "endpoint_for",
]
