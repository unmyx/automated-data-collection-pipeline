"""Alembic environment.

The database URL is resolved in this order so the same ``env.py`` serves the
``alembic`` command line, the ``adcp db`` commands, and the test suite:

1. ``config.attributes["database_url"]`` - set programmatically by
   :mod:`adcp.db.migrations.runner` (this is how tests target a throwaway
   database);
2. ``sqlalchemy.url`` from ``alembic.ini``, when a developer sets it explicitly;
3. ``ADCP_DATABASE_URL`` / ``DATABASE_URL`` / ``.env`` via
   :func:`adcp.config.get_settings`.

``fileConfig`` is deliberately not called: logging is owned by
:func:`adcp.logging.configure_logging`, and reconfiguring the root logger here
would silence or duplicate every other handler in the process.
"""

from __future__ import annotations

from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool

from adcp.config import get_settings
from adcp.db.tables import metadata

config = context.config
target_metadata = metadata

# Configure Alembic's own loggers only when driven by the `alembic` command line
# (which supplies alembic.ini). The `adcp db ...` commands build a Config with no
# file, so this is skipped and our structlog pipeline stays in control.
if config.config_file_name is not None:
    from logging.config import fileConfig

    fileConfig(config.config_file_name)

#: Fallback connect timeout when settings cannot be loaded (e.g. offline SQL
#: generation with deliberately invalid configuration).
DEFAULT_CONNECT_TIMEOUT_S = 5


def resolve_database_url() -> str:
    """Return the database URL Alembic should use."""
    supplied = config.attributes.get("database_url")
    if supplied:
        return str(supplied)
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    return str(get_settings().database_url)


def _connect_args() -> dict[str, Any]:
    """Fail fast on an unreachable database instead of hanging on a TCP timeout."""
    timeout = config.attributes.get("connect_timeout_s")
    if timeout is None:
        try:
            timeout = get_settings().db_connect_timeout_s
        except Exception:  # any settings failure falls back to the default
            timeout = DEFAULT_CONNECT_TIMEOUT_S
    return {"connect_timeout": int(timeout)}


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting (``alembic upgrade head --sql``)."""
    context.configure(
        url=resolve_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database."""
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = resolve_database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=_connect_args(),
    )
    try:
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
