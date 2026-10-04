"""Programmatic Alembic entry points.

``adcp db upgrade`` / ``adcp db current`` and the test suite drive migrations
through this module rather than shelling out to the ``alembic`` binary, so the
commands work from any working directory and always target the database the
application is configured to use.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy.engine import Engine
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import NullPool

from adcp.db.migrations import MIGRATIONS_DIR
from adcp.errors import DatabaseUnavailableError, MigrationError
from adcp.logging import mask_credentials_in_text

DEFAULT_CONNECT_TIMEOUT_S = 5


@dataclass(frozen=True, slots=True)
class SchemaRevision:
    """Where the database is relative to the migration scripts."""

    revision: str | None
    head: str | None
    pending: tuple[str, ...]

    @property
    def is_current(self) -> bool:
        """Whether the database is at the head revision."""
        return self.head is not None and self.revision == self.head


@dataclass(frozen=True, slots=True)
class MigrationOutcome:
    """What a single upgrade or downgrade call did."""

    direction: str
    from_revision: str | None
    to_revision: str | None
    head: str | None
    applied: tuple[str, ...]


def build_config(
    *,
    database_url: str | None = None,
    connect_timeout_s: int | None = None,
) -> Config:
    """Build an Alembic config pointing at the packaged migration scripts.

    The database URL travels through ``config.attributes`` rather than
    ``sqlalchemy.url`` so a password containing a percent sign cannot trip
    configparser interpolation.
    """
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.attributes["database_url"] = database_url
    config.attributes["connect_timeout_s"] = connect_timeout_s
    return config


def script_directory(config: Config) -> ScriptDirectory:
    """Return the revision graph described by the ``versions`` directory."""
    return ScriptDirectory.from_config(config)


def head_revision(config: Config | None = None) -> str | None:
    """Return the head revision id, or None when no migrations exist."""
    return script_directory(config or build_config()).get_current_head()


def _timeout(config: Config) -> int:
    value = config.attributes.get("connect_timeout_s")
    return DEFAULT_CONNECT_TIMEOUT_S if value is None else int(value)


def _engine(database_url: str, connect_timeout_s: int) -> Engine:
    return sa.create_engine(
        database_url,
        poolclass=NullPool,
        connect_args={"connect_timeout": connect_timeout_s},
        hide_parameters=True,
    )


def _unreachable(database_url: str, exc: BaseException) -> DatabaseUnavailableError:
    masked = mask_credentials_in_text(database_url)
    detail = mask_credentials_in_text(str(exc))
    return DatabaseUnavailableError(
        f"cannot reach PostgreSQL at {masked} ({exc.__class__.__name__}): {detail}",
    )


def _current_revision(engine: Engine, database_url: str) -> str | None:
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection, opts={"compare_type": True})
            return context.get_current_revision()
    except (OperationalError, InterfaceError) as exc:
        raise _unreachable(database_url, exc) from exc
    except SQLAlchemyError as exc:
        masked = mask_credentials_in_text(database_url)
        msg = f"cannot read the schema revision from {masked}: {exc}"
        raise MigrationError(msg) from exc


def schema_revision(
    *,
    database_url: str,
    connect_timeout_s: int = DEFAULT_CONNECT_TIMEOUT_S,
) -> SchemaRevision:
    """Read the applied revision and the revisions still to apply.

    Raises:
        DatabaseUnavailableError: the database could not be reached.
        MigrationError: the applied revision is not part of the script graph.
    """
    config = build_config(database_url=database_url, connect_timeout_s=connect_timeout_s)
    scripts = script_directory(config)
    head = scripts.get_current_head()

    engine = _engine(database_url, connect_timeout_s)
    try:
        current = _current_revision(engine, database_url)
    finally:
        engine.dispose()

    try:
        if current is None:
            revisions = list(scripts.iterate_revisions(head, None)) if head else []
        else:
            revisions = list(scripts.iterate_revisions(head, current))
    except CommandError as exc:
        msg = (
            f"the database reports revision {current!r}, which is not part of the "
            f"migration graph: {exc}"
        )
        raise MigrationError(msg) from exc

    return SchemaRevision(
        revision=current,
        head=head,
        pending=tuple(reversed([revision.revision for revision in revisions])),
    )


def _revisions_between(
    scripts: ScriptDirectory,
    upper: str | None,
    lower: str | None,
) -> tuple[str, ...]:
    if upper is None:
        return ()
    revisions = list(scripts.iterate_revisions(upper, lower))
    return tuple(reversed([revision.revision for revision in revisions]))


def _run(config: Config, database_url: str, direction: str, revision: str) -> MigrationOutcome:
    scripts = script_directory(config)
    timeout = _timeout(config)

    engine = _engine(database_url, timeout)
    try:
        before = _current_revision(engine, database_url)
    finally:
        engine.dispose()

    try:
        if direction == "upgrade":
            command.upgrade(config, revision)
        else:
            command.downgrade(config, revision)
    except (OperationalError, InterfaceError) as exc:
        raise _unreachable(database_url, exc) from exc
    except (SQLAlchemyError, CommandError) as exc:
        msg = mask_credentials_in_text(f"alembic {direction} to {revision!r} failed: {exc}")
        raise MigrationError(msg) from exc

    engine = _engine(database_url, timeout)
    try:
        after = _current_revision(engine, database_url)
    finally:
        engine.dispose()

    if direction == "upgrade":
        applied = _revisions_between(scripts, after, before)
    else:
        applied = _revisions_between(scripts, before, after)

    return MigrationOutcome(
        direction=direction,
        from_revision=before,
        to_revision=after,
        head=scripts.get_current_head(),
        applied=applied,
    )


def upgrade(
    *,
    database_url: str,
    revision: str = "head",
    connect_timeout_s: int = DEFAULT_CONNECT_TIMEOUT_S,
) -> MigrationOutcome:
    """Apply migrations up to ``revision`` (default: head)."""
    config = build_config(database_url=database_url, connect_timeout_s=connect_timeout_s)
    return _run(config, database_url, "upgrade", revision)


def downgrade(
    *,
    database_url: str,
    revision: str = "-1",
    connect_timeout_s: int = DEFAULT_CONNECT_TIMEOUT_S,
) -> MigrationOutcome:
    """Roll migrations back (default: one revision). Used by tests and recovery."""
    config = build_config(database_url=database_url, connect_timeout_s=connect_timeout_s)
    return _run(config, database_url, "downgrade", revision)


__all__ = [
    "DEFAULT_CONNECT_TIMEOUT_S",
    "MigrationOutcome",
    "SchemaRevision",
    "build_config",
    "downgrade",
    "head_revision",
    "schema_revision",
    "script_directory",
    "upgrade",
]
