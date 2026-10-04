"""Structured logging configuration built on ``structlog``.

Design goals (see ``docs/PLAN.md`` section 12):

- one JSON object per line in production, human-readable console output locally;
- third-party log records (``httpx``, ``asyncio``, ...) flow through the same
  formatter, so there is only ever one output format;
- run/location context is bound once and appears on every subsequent line;
- secrets never reach the log stream.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any

import structlog
from structlog.typing import EventDict, Processor

REDACTED = "***REDACTED***"
"""Placeholder written in place of a secret value."""

SENSITIVE_KEY_FRAGMENTS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "dsn",
        "database_url",
        "connection_string",
    },
)

_URL_CREDENTIALS_RE = re.compile(r"://(?P<user>[^:/@\s]+):(?P<password>[^@/\s]+)@")
#: Credentials that travel as query parameters (an API key, a token).
_QUERY_CREDENTIALS_RE = re.compile(
    r"(?i)\b(apikey|api_key|access_token|token|password)=([^&\s\"']+)",
)
_LOG_LEVELS: frozenset[str] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_LOG_FORMATS: frozenset[str] = frozenset({"json", "console", "auto"})
_HANDLER_NAME = "adcp-structured"


def _is_sensitive_key(key: object) -> bool:
    if not isinstance(key, str):
        return False
    lowered = key.lower()
    return any(fragment in lowered for fragment in SENSITIVE_KEY_FRAGMENTS)


def redact_secrets(_logger: Any, _method_name: str, event_dict: EventDict) -> EventDict:
    """structlog processor that masks secret values and URL credentials.

    Recurses into nested mappings and lists so a config dump nested one level deep
    cannot leak either.
    """
    for key in list(event_dict.keys()):
        value = event_dict[key]
        if _is_sensitive_key(key) and value is not None:
            event_dict[key] = REDACTED
        else:
            event_dict[key] = _redact_value(value)
    return event_dict


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return mask_credentials_in_text(value)
    if isinstance(value, dict):
        return {
            key: (REDACTED if _is_sensitive_key(key) and item is not None else _redact_value(item))
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_redact_value(item) for item in value]
    return value


def _resolve_format(log_format: str, stream: Any) -> str:
    """Turn ``auto`` into ``json`` or ``console`` based on the output stream."""
    if log_format != "auto":
        return log_format
    target = stream if stream is not None else sys.stderr
    is_tty = bool(getattr(target, "isatty", lambda: False)())
    return "console" if is_tty else "json"


def mask_credentials_in_text(value: str) -> str:
    """Mask credentials embedded anywhere in a string.

    Handles both ``scheme://user:password@host`` DSNs and query parameters such as
    ``?apikey=...``. Used by the log processor and by the database and API layers,
    which wrap driver/provider messages that can echo a URL back to the caller.
    """
    masked = _URL_CREDENTIALS_RE.sub(r"://\g<user>:***@", value)
    return _QUERY_CREDENTIALS_RE.sub(r"\1=***", masked)


def _use_colors(stream: Any) -> bool:
    """Only colourise output when a human is actually looking at it."""
    target = sys.stderr if stream is None else stream
    return bool(getattr(target, "isatty", lambda: False)())


def _shared_processors(include_caller: bool) -> list[Processor]:
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Redaction runs last, after the traceback and stack are rendered into
        # strings: an exception message can quote a DSN or an API key, and the
        # formatted traceback would otherwise bypass the masking entirely.
        redact_secrets,
    ]
    if include_caller:
        processors.append(
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.LINENO,
                ],
            ),
        )
    return processors


def configure_logging(  # noqa: PLR0913 - explicit keyword-only knobs beat a config object here
    *,
    level: str = "INFO",
    log_format: str = "console",
    service: str = "adcp",
    environment: str = "local",
    include_caller: bool = False,
    stream: Any | None = None,
) -> None:
    """Configure structlog and the stdlib logging pipeline. Idempotent.

    Args:
        level: minimum level emitted (stdlib names, e.g. ``"INFO"``).
        log_format: ``"json"``, ``"console"``, or ``"auto"`` (JSON when the output
            stream is not a TTY).
        service: value bound to every record as ``service``.
        environment: value bound to every record as ``env``.
        include_caller: add ``module`` and ``lineno`` to every record.
        stream: destination stream; defaults to ``sys.stderr``. Tests pass a
            ``io.StringIO`` to capture output.
    """
    resolved_level = str(level).upper()
    if resolved_level not in _LOG_LEVELS:
        msg = f"unknown log level {level!r}; expected one of {sorted(_LOG_LEVELS)}"
        raise ValueError(msg)

    requested_format = str(log_format).lower()
    if requested_format not in _LOG_FORMATS:
        msg = f"unknown log format {log_format!r}; expected one of {sorted(_LOG_FORMATS)}"
        raise ValueError(msg)

    resolved_format = _resolve_format(requested_format, stream)
    shared = _shared_processors(include_caller)

    renderer: Processor = (
        structlog.processors.JSONRenderer(sort_keys=True)
        if resolved_format == "json"
        else structlog.dev.ConsoleRenderer(colors=_use_colors(stream))
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stderr if stream is None else stream)
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Only ever remove *our* handler so pytest's log capture handlers survive.
    for existing in [item for item in root.handlers if item.get_name() == _HANDLER_NAME]:
        root.removeHandler(existing)
        existing.close()
    root.addHandler(handler)
    root.setLevel(resolved_level)

    # Third-party libraries are noisy on our formatter's default pipeline:
    # httpx/httpcore/asyncio log at DEBUG, and Alembic announces every migration
    # step at INFO, which would interleave with command output.
    for noisy in ("httpx", "httpcore", "asyncio", "urllib3", "alembic", "apscheduler"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service, env=environment)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound logger carrying any context bound by :func:`configure_logging`."""
    return structlog.stdlib.get_logger(name)


__all__ = [
    "REDACTED",
    "configure_logging",
    "get_logger",
    "mask_credentials_in_text",
    "redact_secrets",
]
