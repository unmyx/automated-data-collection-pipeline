"""PostgreSQL advisory locking: the single-runner guarantee.

Two containers, an overrunning schedule, or a manual ``adcp collect`` during a
scheduled run can all try to collect at the same moment. The pipeline therefore
takes a **session-level advisory lock** for the lifetime of a run
(``docs/PLAN.md`` sections 7.4 and 10.5):

- ``pg_try_advisory_lock`` is atomic, so exactly one session wins;
- the lock is keyed by a fixed ADCP namespace plus a text key, so it cannot be
  confused with single-argument advisory locks taken by other applications in the
  same database;
- session-level (not transaction-level) means it survives the many short
  per-location transactions a run performs;
- PostgreSQL releases it when the session ends - including a hard crash - so a
  killed process can never leave the pipeline permanently locked;
- the lock is released explicitly before the connection returns to the pool, and
  if that release ever fails the connection is invalidated so PostgreSQL drops the
  lock with it.

Contention is an expected outcome, not an error: callers receive
``LockResult(acquired=False)`` and decide what to do - the collection service logs
``ingest.run.skipped`` and exits 0.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from adcp.db.engine import mask_engine_url
from adcp.errors import DatabaseUnavailableError
from adcp.logging import get_logger, mask_credentials_in_text

#: Fixed namespace for ADCP advisory locks (``0x41444350`` spells "ADCP").
LOCK_NAMESPACE = 0x41444350

#: Key identifying "one collection run at a time".
COLLECTION_LOCK_KEY = "adcp:collect"

_TRY_LOCK_SQL = sa.text("SELECT pg_try_advisory_lock(:namespace, hashtext(:key))")
_UNLOCK_SQL = sa.text("SELECT pg_advisory_unlock(:namespace, hashtext(:key))")

_logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class LockResult:
    """Whether the advisory lock was acquired, and how long the caller waited."""

    key: str
    acquired: bool
    waited_s: float


def _connect(engine: Engine) -> Connection:
    try:
        return engine.connect()
    except SQLAlchemyError as exc:
        detail = mask_credentials_in_text(str(exc))
        msg = (
            f"cannot acquire advisory lock: PostgreSQL at {mask_engine_url(engine)} "
            f"is unreachable ({exc.__class__.__name__}): {detail}"
        )
        raise DatabaseUnavailableError(msg) from exc


def _try_lock(connection: Connection, key: str, namespace: int) -> bool:
    result = connection.execute(_TRY_LOCK_SQL, {"namespace": namespace, "key": key})
    return bool(result.scalar_one())


def _unlock(connection: Connection, key: str, namespace: int) -> bool:
    result = connection.execute(_UNLOCK_SQL, {"namespace": namespace, "key": key})
    return bool(result.scalar_one())


@contextmanager
def advisory_lock(
    engine: Engine,
    key: str = COLLECTION_LOCK_KEY,
    *,
    namespace: int = LOCK_NAMESPACE,
    wait_s: float = 0.0,
    poll_interval_s: float = 0.25,
) -> Iterator[LockResult]:
    """Hold a PostgreSQL advisory lock for the duration of the ``with`` block.

    Args:
        engine: pool to take a connection from.
        key: logical lock name, hashed server-side with ``hashtext``.
        namespace: first half of the two-key advisory lock; keep the default.
        wait_s: seconds to keep retrying before reporting contention. ``0`` means
            "try once" - the scheduled-run behaviour, where a second run should
            simply skip rather than queue up.
        poll_interval_s: delay between retries while waiting.

    Yields:
        LockResult: ``acquired`` is ``False`` when another session holds the lock;
        the body of the ``with`` block should then do nothing.

    Raises:
        DatabaseUnavailableError: the database could not be reached.
        ValueError: ``wait_s`` or ``poll_interval_s`` is negative / non-positive.
    """
    if wait_s < 0:
        msg = f"wait_s must be >= 0, got {wait_s}"
        raise ValueError(msg)
    if poll_interval_s <= 0:
        msg = f"poll_interval_s must be > 0, got {poll_interval_s}"
        raise ValueError(msg)

    started = time.monotonic()
    connection = _connect(engine)
    acquired = False
    try:
        acquired = _try_lock(connection, key, namespace)
        if not acquired and wait_s > 0:
            deadline = started + wait_s
            while not acquired and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                time.sleep(min(poll_interval_s, max(remaining, 0.0)))
                acquired = _try_lock(connection, key, namespace)
        yield LockResult(
            key=key,
            acquired=acquired,
            waited_s=round(time.monotonic() - started, 4),
        )
    finally:
        if acquired:
            try:
                _unlock(connection, key, namespace)
            except SQLAlchemyError as exc:  # pragma: no cover - defensive path
                # Never return a connection that still holds the lock to the pool:
                # invalidating closes the session, and PostgreSQL releases the lock.
                connection.invalidate()
                _logger.warning(
                    "db.advisory_lock.release_failed",
                    lock_key=key,
                    error_type=exc.__class__.__name__,
                    error=mask_credentials_in_text(str(exc)),
                )
        connection.close()


__all__ = [
    "COLLECTION_LOCK_KEY",
    "LOCK_NAMESPACE",
    "LockResult",
    "advisory_lock",
]
