"""Retry, backoff, and timeout policy for upstream calls.

Everything here is a pure function of settings plus injectable time, so the
policy can be tested without a network or a wall clock (PLAN sections 8.1-8.3
and 14.6).

The classification rule is deliberately boring: only
:class:`~adcp.errors.RetryableUpstreamError` is retried. Malformed requests,
configuration errors, 4xx responses, oversized bodies, malformed JSON, and schema
violations are never retried because they cannot become true by waiting.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx
from tenacity import RetryCallState, Retrying, retry_if_exception_type, stop_after_attempt
from tenacity.stop import stop_base
from tenacity.wait import wait_base, wait_random_exponential

from adcp.config import Settings
from adcp.errors import RetryableUpstreamError

#: How long a pooled connection may sit idle before being discarded.
KEEPALIVE_EXPIRY_S = 30.0


def build_timeout(settings: Settings) -> httpx.Timeout:
    """Per-phase timeouts, constructed once per client and shared by every request.

    ``pool`` uses the connect timeout: waiting for a free slot in our own
    connection pool should never be slower than establishing a connection.
    """
    return httpx.Timeout(
        connect=settings.open_meteo_timeout_connect_s,
        read=settings.open_meteo_timeout_read_s,
        write=settings.open_meteo_timeout_write_s,
        pool=settings.open_meteo_timeout_connect_s,
    )


def build_limits(settings: Settings) -> httpx.Limits:
    """Bound the client's connection pool to the configured concurrency."""
    return httpx.Limits(
        max_connections=settings.open_meteo_max_concurrency,
        max_keepalive_connections=settings.open_meteo_max_concurrency,
        keepalive_expiry=KEEPALIVE_EXPIRY_S,
    )


def is_retryable(exc: BaseException) -> bool:
    """Whether an exception is classified as transient (PLAN section 8.2)."""
    return isinstance(exc, RetryableUpstreamError)


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse a ``Retry-After`` header into seconds.

    Accepts both documented forms - delay-seconds and an HTTP-date - and returns
    ``None`` when the header is absent or unparseable. Negative delays (a date in
    the past) clamp to zero.
    """
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.isdigit():
        return float(candidate)
    try:
        parsed = parsedate_to_datetime(candidate)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    reference = now or datetime.now(UTC)
    return max((parsed - reference).total_seconds(), 0.0)


class RetryBudget:
    """Wall-clock budget shared by the attempts and the sleeps of one request."""

    def __init__(self, budget_s: float, clock: Callable[[], float]) -> None:
        self._budget_s = budget_s
        self._clock = clock
        self._started = clock()

    @property
    def budget_s(self) -> float:
        return self._budget_s

    def elapsed_s(self) -> float:
        return self._clock() - self._started

    def remaining_s(self) -> float:
        return max(self._budget_s - self.elapsed_s(), 0.0)

    def exhausted(self) -> bool:
        return self.remaining_s() <= 0.0

    def clamp(self, delay_s: float) -> float:
        """Never sleep past the budget; a late attempt is worse than a fast failure."""
        return max(min(delay_s, self.remaining_s()), 0.0)


class _StopWhenBudgetExhausted(stop_base):
    """Stop retrying once the request budget is spent."""

    def __init__(self, budget: RetryBudget) -> None:
        self._budget = budget

    def __call__(self, _retry_state: RetryCallState) -> bool:
        return self._budget.exhausted()


class JitteredBackoff(wait_base):
    """Full-jitter exponential backoff that prefers a server's ``Retry-After``.

    A rate-limited response carries an explicit delay, so it wins over the
    computed backoff - capped by ``ADCP_OPEN_METEO_BACKOFF_MAX_S`` and by the
    remaining request budget.
    """

    def __init__(self, *, multiplier: float, max_s: float, budget: RetryBudget) -> None:
        self._max_s = max_s
        self._budget = budget
        self._jitter = wait_random_exponential(multiplier=multiplier, max=max_s)

    def __call__(self, retry_state: RetryCallState) -> float:
        hint = self._server_hint(retry_state)
        delay = hint if hint is not None else min(self._jitter(retry_state), self._max_s)
        return self._budget.clamp(delay)

    def _server_hint(self, retry_state: RetryCallState) -> float | None:
        outcome = retry_state.outcome
        if outcome is None:
            return None
        exception = outcome.exception()
        hint = getattr(exception, "retry_after_s", None)
        if hint is None:
            return None
        return min(float(hint), self._max_s)


def build_retrying(
    settings: Settings,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    on_retry: Callable[[RetryCallState], None] | None = None,
) -> Retrying:
    """Build the retry policy from settings.

    Args:
        sleep: injected so tests can assert delays without waiting (PLAN 14.6).
        clock: monotonic clock used for the request budget.
        on_retry: structured-logging hook, called after the stop check.
    """
    budget = RetryBudget(settings.open_meteo_timeout_total_s, clock)
    return Retrying(
        stop=stop_after_attempt(settings.open_meteo_max_attempts)
        | _StopWhenBudgetExhausted(budget),
        wait=JitteredBackoff(
            multiplier=settings.open_meteo_backoff_initial_s,
            max_s=settings.open_meteo_backoff_max_s,
            budget=budget,
        ),
        retry=retry_if_exception_type(RetryableUpstreamError),
        before_sleep=on_retry,
        sleep=sleep,
        reraise=True,
    )


__all__ = [
    "KEEPALIVE_EXPIRY_S",
    "JitteredBackoff",
    "RetryBudget",
    "build_limits",
    "build_retrying",
    "build_timeout",
    "is_retryable",
    "parse_retry_after",
]
