"""Engine, connection, and health handling.

The application uses **synchronous** SQLAlchemy with the psycopg 3 driver. See
``docs/PLAN.md`` section 3.5 (ADR-006) for why the pipeline is synchronous and how
per-location concurrency is bounded by a thread pool instead of asyncio.

Pool sizing maps the two ``ADCP_DB_POOL_*`` settings onto SQLAlchemy's
``QueuePool``:

``ADCP_DB_POOL_MIN_SIZE``
    ``pool_size`` - how many connections the pool keeps checked in at steady state.
``ADCP_DB_POOL_MAX_SIZE``
    the hard ceiling; implemented as ``pool_size + max_overflow`` so the pool can
    burst above the steady-state size under load and then shrink back.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from adcp.config import Settings, get_settings
from adcp.errors import DatabaseUnavailableError
from adcp.logging import get_logger, mask_credentials_in_text

#: Seconds a pooled connection may live before SQLAlchemy recycles it. Protects
#: against server-side idle timeouts and half-open TCP connections.
POOL_RECYCLE_S = 1_800

#: How much of a slow statement is quoted in the log line.
SLOW_QUERY_EXCERPT_CHARS = 200

_logger = get_logger(__name__)

_PING_SQL = """
SELECT
    current_database()                              AS database,
    current_user                                    AS username,
    current_setting('server_version')                AS server_version,
    current_setting('server_version_num')::int       AS server_version_num,
    pg_is_in_recovery()                             AS in_recovery,
    coalesce(inet_server_addr()::text, 'local')      AS server_address,
    inet_server_port()                              AS server_port,
    now()                                           AS server_time
"""


@dataclass(frozen=True, slots=True)
class PoolSizing:
    """Resolved pool dimensions, kept testable without inspecting a live pool."""

    pool_size: int
    max_overflow: int
    max_connections: int


@dataclass(frozen=True, slots=True)
class PingResult:
    """Outcome of a connectivity check against PostgreSQL."""

    database: str
    username: str
    server_version: str
    server_version_num: int
    server_address: str
    server_port: int
    in_recovery: bool
    latency_ms: float


def pool_sizing(settings: Settings) -> PoolSizing:
    """Map the pool settings onto SQLAlchemy's ``QueuePool`` parameters."""
    pool_size = settings.db_pool_min_size
    max_overflow = settings.db_pool_max_size - settings.db_pool_min_size
    return PoolSizing(
        pool_size=pool_size,
        max_overflow=max_overflow,
        max_connections=settings.db_pool_max_size,
    )


def connect_args(settings: Settings) -> dict[str, Any]:
    """libpq-level connection arguments shared by every connection in the pool."""
    arguments: dict[str, Any] = {
        "application_name": settings.service_name,
        "connect_timeout": settings.db_connect_timeout_s,
    }
    if settings.db_statement_timeout_ms:
        arguments["options"] = f"-c statement_timeout={settings.db_statement_timeout_ms}"
    return arguments


def mask_engine_url(engine: Engine) -> str:
    """Return the engine's URL with the password replaced by ``***``."""
    return mask_credentials_in_text(engine.url.render_as_string(hide_password=True))


def create_engine_from_settings(settings: Settings) -> Engine:
    """Build a pooled engine from validated settings.

    No connection is opened here; the first checkout does that. ``db ping``,
    ``db upgrade``, and the pipeline each dispose the engine they create.
    """
    sizing = pool_sizing(settings)
    engine = sa.create_engine(
        str(settings.database_url),
        poolclass=QueuePool,
        pool_size=sizing.pool_size,
        max_overflow=sizing.max_overflow,
        pool_timeout=float(settings.db_connect_timeout_s),
        pool_recycle=POOL_RECYCLE_S,
        pool_pre_ping=True,
        connect_args=connect_args(settings),
        # Never interpolate bound parameters into logged SQL.
        hide_parameters=True,
    )
    install_slow_query_logging(engine, threshold_ms=settings.db_slow_query_ms)
    return engine


def summarise_statement(statement: str) -> str:
    """Collapse a statement to one loggable line.

    Bound parameters are never part of ``statement`` (SQLAlchemy keeps them
    separate, and the engine is built with ``hide_parameters``), so this is about
    size, not secrecy.
    """
    collapsed = " ".join(statement.split())
    if len(collapsed) <= SLOW_QUERY_EXCERPT_CHARS:
        return collapsed
    return f"{collapsed[: SLOW_QUERY_EXCERPT_CHARS - 3]}..."


def install_slow_query_logging(engine: Engine, *, threshold_ms: int) -> None:
    """Log statements slower than ``threshold_ms`` as ``db.query.slow``.

    Instrumentation must never break a query, so the listener swallows its own
    errors and only ever reports the statement text (never the parameters).
    """

    @event.listens_for(engine, "before_cursor_execute")
    def _record_start(  # pragma: no cover - exercised through the engine
        conn: Connection,
        _cursor: object,
        _statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        try:
            conn.info.setdefault("adcp_query_started", []).append(time.perf_counter())
        except Exception:  # pragma: no cover - defensive
            return

    @event.listens_for(engine, "after_cursor_execute")
    def _record_finish(
        conn: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        try:
            started = conn.info.get("adcp_query_started")
            if not started:
                return
            elapsed_ms = (time.perf_counter() - started.pop()) * 1_000
            if elapsed_ms < threshold_ms:
                return
            _logger.warning(
                "db.query.slow",
                duration_ms=round(elapsed_ms, 1),
                threshold_ms=threshold_ms,
                statement=summarise_statement(statement),
            )
        except Exception:  # pragma: no cover - instrumentation must never fail a query
            return


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return the process-wide engine built from :func:`adcp.config.get_settings`."""
    return create_engine_from_settings(get_settings())


def dispose_engine() -> None:
    """Dispose the cached engine, if one was created. Safe to call repeatedly."""
    if get_engine.cache_info().currsize:
        get_engine().dispose()
    get_engine.cache_clear()


def _unavailable(engine: Engine, exc: BaseException) -> DatabaseUnavailableError:
    detail = mask_credentials_in_text(str(exc))
    masked_url = mask_engine_url(engine)
    return DatabaseUnavailableError(
        f"cannot reach PostgreSQL at {masked_url} ({exc.__class__.__name__}): {detail}",
    )


@contextmanager
def connection_scope(engine: Engine) -> Iterator[Connection]:
    """Yield a connection inside one transaction, committing on clean exit.

    Connection failures are translated into :class:`DatabaseUnavailableError`;
    statements executed inside the block keep their own exception types (an
    ``IntegrityError`` is a data problem, not an outage).
    """
    try:
        connection = engine.connect()
    except SQLAlchemyError as exc:
        raise _unavailable(engine, exc) from exc
    try:
        with connection.begin():
            yield connection
    finally:
        connection.close()


def ping_database(engine: Engine) -> PingResult:
    """Check connectivity and return server metadata.

    Raises:
        DatabaseUnavailableError: the server could not be reached or the query
            failed. The message never contains credentials.
    """
    started = time.perf_counter()
    try:
        with engine.connect() as connection:
            row = connection.execute(sa.text(_PING_SQL)).mappings().one()
    except SQLAlchemyError as exc:
        raise _unavailable(engine, exc) from exc
    latency_ms = round((time.perf_counter() - started) * 1_000, 2)

    return PingResult(
        database=str(row["database"]),
        username=str(row["username"]),
        server_version=str(row["server_version"]),
        server_version_num=int(row["server_version_num"]),
        server_address=str(row["server_address"]),
        server_port=int(row["server_port"]),
        in_recovery=bool(row["in_recovery"]),
        latency_ms=latency_ms,
    )


__all__ = [
    "POOL_RECYCLE_S",
    "PingResult",
    "PoolSizing",
    "connect_args",
    "connection_scope",
    "create_engine_from_settings",
    "dispose_engine",
    "get_engine",
    "install_slow_query_logging",
    "mask_engine_url",
    "ping_database",
    "pool_sizing",
    "summarise_statement",
]
