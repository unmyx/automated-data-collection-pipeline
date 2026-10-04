"""PostgreSQL fixtures for integration tests.

The test database is resolved in this order:

1. ``ADCP_TEST_DATABASE_URL`` - used by CI (service container) and by developers
   who already have a PostgreSQL they want to point at;
2. Testcontainers - a throwaway ``postgres:17-alpine``, when Docker is available;
3. the Compose stack from ``docker-compose.yml`` - the suite creates a sibling
   ``<database>_test`` database on the same server so the application database is
   left untouched;
4. otherwise the integration tests **skip**, so ``pytest`` still succeeds on a
   machine with no Docker and no local PostgreSQL.

Because the suite truncates tables between tests, the target must be disposable:
it refuses to run against a database whose name does not contain ``test`` unless
``ADCP_TEST_ALLOW_ANY_DATABASE=true`` is set explicitly.

``Settings`` is read with ``_env_file=None`` (the default project DSN) rather than
through ``get_settings()`` so the developer's ``.env`` cannot redirect the tests.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from adcp.config import Settings, get_settings
from adcp.db.migrations.runner import schema_revision, upgrade

#: Tables truncated before every integration test. ``alembic_version`` is never
#: touched. ``RESTART IDENTITY`` keeps generated ids deterministic per test.
TRUNCATE_SQL = sa.text(
    "TRUNCATE TABLE weather_hourly, ingestion_run_errors, ingestion_watermarks, "
    "ingestion_runs, locations RESTART IDENTITY CASCADE",
)

TEST_TIMEOUT_S = 5


@dataclass
class DatabaseTarget:
    """A disposable database plus how to get rid of it when the session ends."""

    url: str
    source: str
    stop: Callable[[], None] | None = field(default=None, repr=False)

    def close(self) -> None:
        if self.stop is not None:
            self.stop()


def _is_disabled(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"0", "false", "no", "off"}


def _assert_disposable(url: str) -> None:
    """Refuse to truncate a database that does not look disposable."""
    database = sa.engine.make_url(url).database or ""
    allow_any = os.environ.get("ADCP_TEST_ALLOW_ANY_DATABASE", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if not allow_any and "test" not in database.lower():
        pytest.fail(
            f"refusing to run destructive tests against database {database!r}: the name "
            "must contain 'test', or set ADCP_TEST_ALLOW_ANY_DATABASE=true to override",
        )


def _from_environment() -> DatabaseTarget | None:
    url = os.environ.get("ADCP_TEST_DATABASE_URL", "").strip()
    return DatabaseTarget(url=url, source="ADCP_TEST_DATABASE_URL") if url else None


def _from_testcontainers() -> DatabaseTarget | None:
    if _is_disabled("ADCP_TEST_USE_TESTCONTAINERS"):
        return None
    try:
        # Imported lazily so unit tests never pay for (or depend on) testcontainers.
        from testcontainers.community.postgres import PostgresContainer  # noqa: PLC0415
    except ImportError:  # pragma: no cover - dev extra not installed
        return None

    try:
        container = PostgresContainer(
            "postgres:17-alpine",
            driver="psycopg",
            username="adcp_test",
            # Deliberately different from the database/user name so tests can
            # assert that the password never appears in CLI output.
            password="adcp-ci-password",
            dbname="adcp_test",
        )
        container.start()
    except Exception:  # Docker missing or not running: fall back to the next option
        return None
    return DatabaseTarget(
        url=container.get_connection_url(),
        source="testcontainers",
        stop=container.stop,
    )


def _ensure_database(application_url: str, database_name: str) -> str | None:
    """Create ``database_name`` on the application server and return its URL."""
    url = sa.engine.make_url(application_url)
    admin_url = url.set(database="postgres")
    engine = sa.create_engine(
        admin_url,
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
        connect_args={"connect_timeout": TEST_TIMEOUT_S},
    )
    try:
        with engine.connect() as connection:
            exists = connection.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": database_name},
            ).scalar()
            if not exists:
                connection.execute(sa.text(f'CREATE DATABASE "{database_name}"'))
    except SQLAlchemyError:
        return None
    finally:
        engine.dispose()
    return str(url.set(database=database_name))


def _from_compose() -> DatabaseTarget | None:
    """Use the Compose server, but never the application database itself."""
    settings = Settings(_env_file=None)
    application_url = str(settings.database_url)
    database_name = f"{sa.engine.make_url(application_url).database}_test"
    test_url = _ensure_database(application_url, database_name)
    if test_url is None:
        return None
    return DatabaseTarget(url=test_url, source=f"compose ({database_name})")


@pytest.fixture(scope="session")
def test_database() -> Iterator[DatabaseTarget]:
    """Resolve (or skip) the database the integration tests run against."""
    target: DatabaseTarget | None = None
    for discover in (_from_environment, _from_testcontainers, _from_compose):
        target = discover()
        if target is not None:
            break

    if target is None:
        pytest.skip(
            "no PostgreSQL available: set ADCP_TEST_DATABASE_URL, start Docker "
            "(testcontainers), or run `docker compose up -d postgres`",
        )

    _assert_disposable(target.url)
    try:
        yield target
    finally:
        target.close()


@pytest.fixture(scope="session")
def database_url(test_database: DatabaseTarget) -> str:
    """URL of the throwaway database, with the schema migrated to head."""
    return test_database.url


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Bring the throwaway database to head once per session."""
    upgrade(database_url=database_url, connect_timeout_s=TEST_TIMEOUT_S)
    return database_url


@pytest.fixture(scope="session")
def db_engine(migrated_database: str) -> Iterator[Engine]:
    """Engine for the test database.

    A regular pooled engine (not ``NullPool``) so tests can hold several
    independent sessions at once - the advisory-lock tests depend on that.
    """
    engine = sa.create_engine(
        migrated_database,
        pool_size=5,
        max_overflow=5,
        connect_args={"connect_timeout": TEST_TIMEOUT_S},
        hide_parameters=True,
    )
    yield engine
    engine.dispose()


def _ensure_schema_is_current(database_url: str) -> None:
    revision = schema_revision(database_url=database_url, connect_timeout_s=TEST_TIMEOUT_S)
    if not revision.is_current:
        upgrade(database_url=database_url, connect_timeout_s=TEST_TIMEOUT_S)


@pytest.fixture(autouse=True)
def clean_database(db_engine: Engine, migrated_database: str) -> None:
    """Start every integration test from the head schema and empty tables."""
    _ensure_schema_is_current(migrated_database)
    with db_engine.begin() as connection:
        connection.execute(TRUNCATE_SQL)


@pytest.fixture
def cli_database_env(monkeypatch: pytest.MonkeyPatch, migrated_database: str) -> Iterator[str]:
    """Point the CLI at the test database for the duration of one test."""
    monkeypatch.setenv("ADCP_DATABASE_URL", migrated_database)
    get_settings.cache_clear()
    yield migrated_database
    get_settings.cache_clear()
