"""Database layer.

Contains everything that knows about PostgreSQL: the engine and connection
handling (:mod:`adcp.db.engine`), the Core table definitions (:mod:`adcp.db.tables`),
repositories (:mod:`adcp.db.repository`, :mod:`adcp.db.run_tracker`,
:mod:`adcp.db.watermark_store`), the advisory-lock primitive
(:mod:`adcp.db.lock`), and the Alembic migrations (:mod:`adcp.db.migrations`).

Nothing in this package is imported at application start-up unless a command
actually needs a database, so ``adcp config check`` keeps working with no
PostgreSQL in sight.
"""

from __future__ import annotations

from adcp.db.engine import (
    PingResult,
    PoolSizing,
    connection_scope,
    create_engine_from_settings,
    dispose_engine,
    get_engine,
    mask_engine_url,
    ping_database,
    pool_sizing,
)
from adcp.db.lock import COLLECTION_LOCK_KEY, LOCK_NAMESPACE, LockResult, advisory_lock
from adcp.db.tables import INGESTION_STATUS_VALUES, WEATHER_SOURCE_VALUES, metadata

__all__ = [
    "COLLECTION_LOCK_KEY",
    "INGESTION_STATUS_VALUES",
    "LOCK_NAMESPACE",
    "WEATHER_SOURCE_VALUES",
    "LockResult",
    "PingResult",
    "PoolSizing",
    "advisory_lock",
    "connection_scope",
    "create_engine_from_settings",
    "dispose_engine",
    "get_engine",
    "mask_engine_url",
    "metadata",
    "ping_database",
    "pool_sizing",
]
