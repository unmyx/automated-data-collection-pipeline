"""Retry classification, backoff bounds, and the request budget."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import NoReturn

import httpx
import pytest
from tenacity import RetryCallState, Retrying

from adcp.config import Settings
from adcp.errors import (
    ConfigurationError,
    MalformedJsonError,
    PayloadTooLargeError,
    RetryableUpstreamError,
    SchemaError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamRateLimitedError,
    UpstreamResponseError,
    UpstreamServerError,
    UpstreamTimeoutError,
)
from adcp.resilience import (
    JitteredBackoff,
    RetryBudget,
    build_limits,
    build_retrying,
    build_timeout,
    is_retryable,
    parse_retry_after,
)

pytestmark = pytest.mark.unit


class FakeClock:
    """Monotonic clock whose `sleep` advances time, so budgets are deterministic."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def _retry_state(exception: BaseException, attempt_number: int = 1) -> RetryCallState:
    """A real tenacity state carrying a failed outcome, as before_sleep would see."""
    state = RetryCallState(retry_object=Retrying(), fn=None, args=(), kwargs={})
    state.attempt_number = attempt_number
    state.set_exception((type(exception), exception, exception.__traceback__))
    return state


def test_timeouts_are_built_from_settings_per_phase() -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_timeout_connect_s=3.0,
        open_meteo_timeout_read_s=11.0,
        open_meteo_timeout_write_s=7.0,
    )

    timeout = build_timeout(settings)

    assert timeout.connect == 3.0
    assert timeout.read == 11.0
    assert timeout.write == 7.0
    assert timeout.pool == 3.0


def test_connection_limits_follow_the_concurrency_setting() -> None:
    limits = build_limits(Settings(_env_file=None, open_meteo_max_concurrency=3))

    assert limits.max_connections == 3
    assert limits.max_keepalive_connections == 3


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (UpstreamTimeoutError("t"), True),
        (UpstreamConnectionError("c"), True),
        (UpstreamServerError("s"), True),
        (UpstreamRateLimitedError("r"), True),
        (UpstreamClientError("bad request"), False),
        (UpstreamResponseError("odd"), False),
        (SchemaError("ragged"), False),
        (MalformedJsonError("truncated"), False),
        (PayloadTooLargeError("huge"), False),
        (ConfigurationError("no endpoint"), False),
        (ValueError("plain bug"), False),
        (httpx.ConnectError("connect"), False),
        (httpx.ReadTimeout("read"), False),
        (RuntimeError("unexpected"), False),
    ],
)
def test_retry_classification(error: BaseException, expected: bool) -> None:
    assert is_retryable(error) is expected
    assert isinstance(error, RetryableUpstreamError) is expected


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("42", 42.0),
        ("0", 0.0),
        ("not-a-delay", None),
    ],
)
def test_retry_after_parsing(header: str | None, expected: float | None) -> None:
    assert parse_retry_after(header) == expected


def test_retry_after_accepts_http_dates() -> None:
    now = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
    header = "Fri, 02 Oct 2026 12:00:30 GMT"

    assert parse_retry_after(header, now=now) == pytest.approx(30.0)


def test_retry_after_in_the_past_clamps_to_zero() -> None:
    now = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
    header = "Fri, 02 Oct 2026 11:59:00 GMT"

    assert parse_retry_after(header, now=now) == 0.0


def test_retry_budget_tracks_elapsed_and_clamps() -> None:
    clock = FakeClock()
    budget = RetryBudget(5.0, clock)

    assert budget.remaining_s() == 5.0
    assert not budget.exhausted()
    clock.now = 4.0
    assert budget.clamp(10.0) == 1.0
    clock.now = 5.0
    assert budget.exhausted()
    assert budget.clamp(1.0) == 0.0


def test_backoff_honours_retry_after_and_the_cap() -> None:
    clock = FakeClock()
    backoff = JitteredBackoff(multiplier=1.0, max_s=30.0, budget=RetryBudget(600.0, clock))

    state = _retry_state(UpstreamRateLimitedError("slow down", retry_after_s=42.0))

    delay = backoff(state)

    assert delay == 30.0, "the server hint is capped by ADCP_OPEN_METEO_BACKOFF_MAX_S"


def test_backoff_is_bounded_and_jittered() -> None:
    clock = FakeClock()
    backoff = JitteredBackoff(multiplier=1.0, max_s=8.0, budget=RetryBudget(600.0, clock))

    delays = [
        backoff(_retry_state(UpstreamServerError("boom"), attempt)) for attempt in range(1, 5)
    ]

    assert all(0.0 <= delay <= 8.0 for delay in delays)
    assert len(set(delays)) > 1, "full jitter should not produce one fixed delay"


def _drain(
    retrying: Retrying,
    factory: Callable[[], BaseException],
    attempts: list[int],
) -> None:
    """Run a failing call through the retry policy, recording each attempt."""
    for attempt in retrying:
        with attempt:
            attempts.append(1)
            raise factory()


def test_attempts_are_bounded_by_max_attempts() -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=3,
        open_meteo_backoff_initial_s=0.01,
        open_meteo_backoff_max_s=0.05,
        open_meteo_timeout_total_s=30.0,
    )
    sleeps: list[float] = []
    retrying = build_retrying(settings, sleep=sleeps.append, clock=time.monotonic)
    attempts: list[int] = []

    with pytest.raises(UpstreamServerError):
        _drain(retrying, lambda: UpstreamServerError("boom"), attempts)

    assert len(attempts) == 3
    assert len(sleeps) == 2
    assert all(0.0 <= delay <= 0.05 for delay in sleeps)


def test_request_budget_stops_retrying_before_max_attempts() -> None:
    """A five-second budget with two-second server hints allows four attempts."""
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=9,
        open_meteo_backoff_initial_s=1.0,
        open_meteo_backoff_max_s=30.0,
        open_meteo_timeout_total_s=5.0,
    )
    clock = FakeClock()
    retrying = build_retrying(settings, sleep=clock.sleep, clock=clock)
    attempts: list[int] = []

    with pytest.raises(UpstreamRateLimitedError):
        _drain(
            retrying,
            lambda: UpstreamRateLimitedError("slow down", retry_after_s=2.0),
            attempts,
        )

    assert len(attempts) == 4
    assert clock.slept == [2.0, 2.0, 1.0], "the last sleep is clamped to the remaining budget"
    assert clock.now == 5.0


def test_non_retryable_failures_are_not_retried() -> None:
    settings = Settings(_env_file=None, open_meteo_max_attempts=5)
    sleeps: list[float] = []
    retrying = build_retrying(settings, sleep=sleeps.append, clock=time.monotonic)
    attempts: list[int] = []

    with pytest.raises(SchemaError):
        _drain(retrying, lambda: SchemaError("ragged arrays"), attempts)

    assert len(attempts) == 1
    assert sleeps == []


def test_retrying_reports_statistics_for_logging() -> None:
    settings = Settings(
        _env_file=None,
        open_meteo_max_attempts=2,
        open_meteo_backoff_initial_s=0.01,
        open_meteo_backoff_max_s=0.02,
    )
    retrying = build_retrying(settings, sleep=lambda _delay: None, clock=time.monotonic)

    def _fail() -> NoReturn:
        raise UpstreamTimeoutError("nope")

    with pytest.raises(UpstreamTimeoutError):
        retrying(_fail)

    assert retrying.statistics["attempt_number"] == 2
