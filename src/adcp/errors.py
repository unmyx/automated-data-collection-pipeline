"""Exception hierarchy for ADCP.

Every failure the application can anticipate has a named type so callers (the CLI,
the pipeline, the scheduler) can map it to the documented exit codes in
``docs/PLAN.md`` section 11.4 without inspecting driver-specific exceptions.

The hierarchy is intentionally small and covers only failures the application can
actually produce. The upstream types follow the retry taxonomy in
``docs/PLAN.md`` section 8.2:

```
AdcpError
|-- ConfigurationError            # invalid settings or an invalid request
`-- UpstreamError                 # anything that happened while talking to a provider
    |-- RetryableUpstreamError    # marker: transient, safe to retry
    |   |-- UpstreamTimeoutError
    |   |-- UpstreamConnectionError
    |   |-- UpstreamServerError
    |   `-- UpstreamRateLimitedError
    `-- UpstreamResponseError     # permanent for this attempt
        |-- UpstreamClientError
        |-- PayloadTooLargeError
        |-- MalformedJsonError
        `-- SchemaError
```

Retry classification is therefore a type check - ``RetryableUpstreamError`` - and
never a string comparison or a status-code table duplicated at the call site.
"""

from __future__ import annotations


class AdcpError(Exception):
    """Base class for every error raised deliberately by ADCP."""


class DatabaseError(AdcpError):
    """Base class for database failures."""


class DatabaseUnavailableError(DatabaseError):
    """The database could not be reached, or the connection was lost.

    Maps to exit code 1 (hard failure) in the CLI. Constraint violations and other
    *server-side* rejections are **not** this error: they surface as SQLAlchemy's
    ``IntegrityError`` because they indicate a bug or bad data, not an outage.
    """


class MigrationError(DatabaseError):
    """A schema migration could not be applied or inspected.

    Maps to exit code 1. A schema that is merely *behind* head is reported by
    ``adcp db current`` and aborts a collection run with exit code 2 (PLAN F3).
    """


class LocationNotFoundError(AdcpError):
    """A location slug does not exist in the ``locations`` table."""


class RunNotFoundError(AdcpError):
    """An ``ingestion_runs`` row could not be found or updated."""


class ConfigurationError(AdcpError):
    """Invalid configuration, or a request that cannot be built from it.

    Raised before any network call: bad coordinates, an inverted date range, an
    unsupported variable, or a missing endpoint. Maps to exit code 2 and is never
    retried.
    """


class UpstreamError(AdcpError):
    """Base class for every failure while interacting with an upstream provider."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str | None = None,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        #: Request URL with credentials removed - safe to log or persist.
        self.endpoint = endpoint
        self.status_code = status_code
        self.detail = detail

    def as_details(self) -> dict[str, object]:
        """Structured fields for logging and ``ingestion_run_errors``."""
        return {
            "error_type": type(self).__name__,
            "message": self.message,
            "endpoint": self.endpoint,
            "http_status": self.status_code,
            "detail": self.detail,
        }


class RetryableUpstreamError(UpstreamError):
    """Marker base class for transient upstream failures (PLAN section 8.2)."""


class UpstreamTimeoutError(RetryableUpstreamError):
    """A connect, read, write, or pool timeout elapsed."""


class UpstreamConnectionError(RetryableUpstreamError):
    """The connection failed or was interrupted before a response was complete."""


class UpstreamServerError(RetryableUpstreamError):
    """The provider returned a 5xx response."""


class UpstreamRateLimitedError(RetryableUpstreamError):
    """The provider returned 429 (or an equivalent throttling signal)."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_s: float | None = None,
        endpoint: str | None = None,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(
            message,
            endpoint=endpoint,
            status_code=status_code,
            detail=detail,
        )
        #: Server-suggested delay, already capped by the caller.
        self.retry_after_s = retry_after_s

    def as_details(self) -> dict[str, object]:
        return {**super().as_details(), "retry_after_s": self.retry_after_s}


class UpstreamResponseError(UpstreamError):
    """The provider answered, but the answer cannot be used."""


class UpstreamClientError(UpstreamResponseError):
    """The provider rejected the request (4xx). A retry cannot help."""


class PayloadTooLargeError(UpstreamResponseError):
    """The response body exceeded the configured size cap."""


class MalformedJsonError(UpstreamResponseError):
    """The response body was not valid JSON."""


class SchemaError(UpstreamResponseError):
    """The payload is valid JSON but violates the provider contract.

    Missing fields, unexpected types, ragged hourly arrays, unit changes, invalid
    timestamps, and inconsistent location metadata all raise this, with the
    offending field paths in :attr:`field_paths`.
    """

    def __init__(
        self,
        message: str,
        *,
        field_paths: tuple[str, ...] = (),
        endpoint: str | None = None,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(
            message,
            endpoint=endpoint,
            status_code=status_code,
            detail=detail,
        )
        self.field_paths = field_paths

    def as_details(self) -> dict[str, object]:
        return {**super().as_details(), "field_paths": list(self.field_paths)}


__all__ = [
    "AdcpError",
    "ConfigurationError",
    "DatabaseError",
    "DatabaseUnavailableError",
    "LocationNotFoundError",
    "MalformedJsonError",
    "MigrationError",
    "PayloadTooLargeError",
    "RetryableUpstreamError",
    "RunNotFoundError",
    "SchemaError",
    "UpstreamClientError",
    "UpstreamConnectionError",
    "UpstreamError",
    "UpstreamRateLimitedError",
    "UpstreamResponseError",
    "UpstreamServerError",
    "UpstreamTimeoutError",
]
