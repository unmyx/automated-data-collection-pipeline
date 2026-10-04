"""Advisory locking: the single-runner guarantee from PLAN section 7.4."""

from __future__ import annotations

import threading
import time

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from adcp.db.lock import COLLECTION_LOCK_KEY, LOCK_NAMESPACE, advisory_lock
from adcp.errors import DatabaseUnavailableError

pytestmark = pytest.mark.integration


def test_a_second_session_cannot_hold_the_same_lock(db_engine: Engine) -> None:
    with advisory_lock(db_engine) as first:
        assert first.acquired is True
        assert first.key == COLLECTION_LOCK_KEY

        with advisory_lock(db_engine) as second:
            assert second.acquired is False, "the lock must exclude a second runner"
            assert second.waited_s >= 0


def test_lock_is_released_when_the_context_exits(db_engine: Engine) -> None:
    with advisory_lock(db_engine) as first:
        assert first.acquired

    with advisory_lock(db_engine) as again:
        assert again.acquired, "leaving the context must release the lock"


def test_lock_is_released_when_the_body_raises(db_engine: Engine) -> None:

    with pytest.raises(RuntimeError), advisory_lock(db_engine) as held:  # noqa: PT012
        assert held.acquired
        raise RuntimeError("boom")

    with advisory_lock(db_engine) as again:
        assert again.acquired, "an exception must not leak the lock"


def test_lock_is_session_level_not_transaction_level(db_engine: Engine) -> None:
    """A ROLLBACK must not release it - that is what makes it safe across the
    many short per-location transactions a run performs."""
    try_sql = sa.text("SELECT pg_try_advisory_lock(:namespace, hashtext(:key))")
    unlock_sql = sa.text("SELECT pg_advisory_unlock(:namespace, hashtext(:key))")
    parameters = {"namespace": LOCK_NAMESPACE, "key": COLLECTION_LOCK_KEY}

    with db_engine.connect() as connection:
        assert connection.execute(try_sql, parameters).scalar_one() is True
        connection.rollback()
        unlocked = connection.execute(unlock_sql, parameters).scalar_one()
        assert unlocked is True, "the lock outlived the transaction, as designed"

    with advisory_lock(db_engine) as after:  # released above, so it is available
        assert after.acquired


def test_waiting_reports_contention_after_the_timeout(db_engine: Engine) -> None:
    with (
        advisory_lock(db_engine),
        advisory_lock(db_engine, wait_s=0.3, poll_interval_s=0.05) as blocked,
    ):
        assert blocked.acquired is False
        assert blocked.waited_s >= 0.3


def test_waiting_acquires_the_lock_once_it_is_released(db_engine: Engine) -> None:
    holder_ready = threading.Event()

    def hold_briefly() -> None:
        with advisory_lock(db_engine) as lock:
            assert lock.acquired
            holder_ready.set()
            # Long enough for the waiter below to observe contention, short
            # enough to keep the suite fast.
            time.sleep(0.4)

    thread = threading.Thread(target=hold_briefly, name="lock-holder")
    thread.start()
    try:
        assert holder_ready.wait(5), "holder thread never took the lock"
        with advisory_lock(db_engine, wait_s=10.0, poll_interval_s=0.05) as acquired:
            assert acquired.acquired is True
    finally:
        thread.join(5)


def test_lock_key_is_namespaced(db_engine: Engine) -> None:
    """A different key in the same namespace is a different lock."""
    with (
        advisory_lock(db_engine, "adcp:backfill") as backfill,
        advisory_lock(db_engine, COLLECTION_LOCK_KEY) as collection,
        advisory_lock(db_engine, "adcp:backfill") as same_key,
    ):
        assert backfill.acquired
        assert collection.acquired, "unrelated keys must not exclude each other"
        assert same_key.acquired is False

    assert LOCK_NAMESPACE == 0x41444350


def test_other_namespaces_do_not_collide(db_engine: Engine) -> None:
    with advisory_lock(db_engine, COLLECTION_LOCK_KEY) as collection:
        assert collection.acquired
        with advisory_lock(
            db_engine,
            COLLECTION_LOCK_KEY,
            namespace=LOCK_NAMESPACE + 1,
        ) as foreign:
            assert foreign.acquired, "a foreign namespace must not be blocked by ADCP"


def test_connection_failure_is_reported_cleanly() -> None:
    engine = sa.create_engine(
        "postgresql+psycopg://adcp:topsecret@127.0.0.1:59999/adcp",
        poolclass=NullPool,
        connect_args={"connect_timeout": 1},
    )
    try:
        with pytest.raises(DatabaseUnavailableError) as excinfo, advisory_lock(engine):
            pass
    finally:
        engine.dispose()

    assert "topsecret" not in str(excinfo.value)


@pytest.mark.parametrize(
    ("wait_s", "poll_interval_s"),
    [(-1.0, 0.25), (0.0, 0.0), (0.0, -0.5)],
)
def test_invalid_wait_configuration_is_rejected(
    wait_s: float,
    poll_interval_s: float,
) -> None:
    engine = sa.create_engine("postgresql+psycopg://localhost:5432/none")
    try:
        with (
            pytest.raises(ValueError, match="must be"),
            advisory_lock(
                engine,
                wait_s=wait_s,
                poll_interval_s=poll_interval_s,
            ),
        ):
            pass
    finally:
        engine.dispose()
