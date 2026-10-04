"""Connectivity checks and credential hygiene."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings, ping_database
from adcp.errors import DatabaseUnavailableError

pytestmark = pytest.mark.integration

#: A TCP port nothing listens on; the driver fails after the connect timeout.
UNREACHABLE_URL = "postgresql+psycopg://adcp:topsecret@127.0.0.1:59999/adcp"


def test_ping_reports_server_metadata(db_engine: Engine, migrated_database: str) -> None:
    settings = Settings(
        _env_file=None,
        database_url=migrated_database,
        db_connect_timeout_s=5,
    )
    engine = create_engine_from_settings(settings)
    try:
        result = ping_database(engine)
    finally:
        engine.dispose()

    assert result.database == sa.engine.make_url(migrated_database).database
    assert result.server_version_num >= 130_000
    assert result.server_version.startswith(str(result.server_version_num)[:2])
    assert result.in_recovery is False
    assert result.latency_ms >= 0
    assert result.username


def test_ping_against_an_unreachable_database_is_a_clean_failure() -> None:
    settings = Settings(
        _env_file=None,
        database_url=UNREACHABLE_URL,
        db_connect_timeout_s=1,
    )
    engine = create_engine_from_settings(settings)
    try:
        with pytest.raises(DatabaseUnavailableError) as excinfo:
            ping_database(engine)
    finally:
        engine.dispose()

    message = str(excinfo.value)
    assert "topsecret" not in message
    assert "***" in message
    assert "127.0.0.1:59999" in message
