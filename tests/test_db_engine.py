"""Engine construction - pure unit tests that never open a connection."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy.pool import QueuePool

from adcp.config import Settings
from adcp.db import engine as engine_module
from adcp.db.engine import (
    connect_args,
    create_engine_from_settings,
    dispose_engine,
    get_engine,
    mask_engine_url,
    pool_sizing,
)

pytestmark = pytest.mark.unit

URL = "postgresql+psycopg://adcp:topsecret@localhost:55432/adcp"


@pytest.fixture(autouse=True)
def _no_cached_engine() -> Iterator[None]:
    yield
    dispose_engine()


def test_pool_settings_map_onto_queue_pool() -> None:
    settings = Settings(_env_file=None, db_pool_min_size=2, db_pool_max_size=7)

    sizing = pool_sizing(settings)

    assert sizing.pool_size == 2, "min size is the steady-state pool size"
    assert sizing.max_overflow == 5, "max size is the hard ceiling"
    assert sizing.max_connections == 7


def test_pool_sizing_is_applied_to_the_engine() -> None:
    settings = Settings(_env_file=None, db_pool_min_size=3, db_pool_max_size=4, database_url=URL)
    engine = create_engine_from_settings(settings)
    try:
        pool = engine.pool
        assert isinstance(pool, QueuePool), "the plan commits to a queue pool"
        assert pool.size() == 3
        assert pool._max_overflow == 1  # the only way to read the ceiling back
    finally:
        engine.dispose()


def test_connect_args_carry_timeouts_and_statement_budget() -> None:
    settings = Settings(
        _env_file=None,
        db_connect_timeout_s=7,
        db_statement_timeout_ms=1234,
        service_name="adcp-test",
    )

    arguments = connect_args(settings)

    assert arguments["connect_timeout"] == 7
    assert arguments["application_name"] == "adcp-test"
    assert arguments["options"] == "-c statement_timeout=1234"


def test_engine_construction_does_not_require_a_database() -> None:
    settings = Settings(_env_file=None, database_url="postgresql+psycopg://nobody@127.0.0.1:1/none")

    engine = create_engine_from_settings(settings)

    try:
        assert engine.pool.status()
    finally:
        engine.dispose()


def test_masked_url_hides_the_password_but_the_engine_keeps_it() -> None:
    settings = Settings(_env_file=None, database_url=URL)
    engine = create_engine_from_settings(settings)

    try:
        masked = mask_engine_url(engine)
    finally:
        engine.dispose()

    assert "topsecret" not in masked
    assert masked == "postgresql+psycopg://adcp:***@localhost:55432/adcp"
    assert engine.url.password == "topsecret"


def test_get_engine_is_cached_and_disposable() -> None:
    first = get_engine()
    assert get_engine() is first

    dispose_engine()

    second = get_engine()
    try:
        assert second is not first
        assert get_engine.cache_info().currsize == 1
    finally:
        dispose_engine()
    assert get_engine.cache_info().currsize == 0


def test_dispose_engine_without_a_cached_engine_is_a_no_op() -> None:
    engine_module.get_engine.cache_clear()

    dispose_engine()

    assert get_engine.cache_info().currsize == 0
