"""Migrations: apply, roll back, and match the Core table definitions."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy.engine import Engine

from adcp.db.migrations.runner import downgrade, head_revision, schema_revision, upgrade
from adcp.db.tables import INGESTION_STATUS_VALUES, metadata

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "ingestion_run_errors",
    "ingestion_runs",
    "ingestion_watermarks",
    "locations",
    "weather_hourly",
}

EXPECTED_INDEXES = {
    "ingestion_run_errors_error_type_idx",
    "ingestion_run_errors_run_idx",
    "ingestion_runs_open_idx",
    "ingestion_runs_started_at_idx",
    "ingestion_runs_status_idx",
    "locations_active_idx",
    "weather_hourly_location_time_idx",
    "weather_hourly_observed_at_idx",
    "weather_hourly_source_idx",
}


def _tables(engine: Engine) -> set[str]:
    statement = sa.text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name <> 'alembic_version'",
    )
    with engine.connect() as connection:
        return set(connection.execute(statement).scalars().all())


def _indexes(engine: Engine) -> set[str]:
    statement = sa.text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
    with engine.connect() as connection:
        return set(connection.execute(statement).scalars().all())


def _enum_labels(engine: Engine) -> list[str]:
    statement = sa.text(
        "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid "
        "WHERE t.typname = 'ingestion_status' ORDER BY e.enumsortorder",
    )
    with engine.connect() as connection:
        return list(connection.execute(statement).scalars().all())


def test_upgrade_from_empty_database_creates_the_full_schema(
    db_engine: Engine,
    migrated_database: str,
) -> None:
    downgrade(database_url=migrated_database, revision="base", connect_timeout_s=5)
    assert _tables(db_engine) == set(), "base revision should leave no application tables"

    outcome = upgrade(database_url=migrated_database, connect_timeout_s=5)

    assert outcome.to_revision == head_revision()
    assert outcome.applied == (
        "0001_locations",
        "0002_ingestion_runs",
        "0003_weather_hourly",
        "0004_ingestion_run_errors",
        "0005_ingestion_watermarks",
    )
    assert _tables(db_engine) == EXPECTED_TABLES
    assert _indexes(db_engine) >= EXPECTED_INDEXES
    assert _enum_labels(db_engine) == list(INGESTION_STATUS_VALUES)


def test_downgrade_one_revision_removes_only_the_last_table(
    db_engine: Engine,
    migrated_database: str,
) -> None:
    outcome = downgrade(database_url=migrated_database, connect_timeout_s=5)

    assert outcome.from_revision == "0005_ingestion_watermarks"
    assert outcome.to_revision == "0004_ingestion_run_errors"
    assert "ingestion_watermarks" not in _tables(db_engine)
    assert _tables(db_engine) == EXPECTED_TABLES - {"ingestion_watermarks"}

    upgrade(database_url=migrated_database, connect_timeout_s=5)
    assert _tables(db_engine) == EXPECTED_TABLES


def test_downgrade_to_base_then_upgrade_cleanly_recreates_everything(
    db_engine: Engine,
    migrated_database: str,
) -> None:
    downgrade(database_url=migrated_database, revision="base", connect_timeout_s=5)
    assert _tables(db_engine) == set()

    upgrade(database_url=migrated_database, connect_timeout_s=5)
    assert _tables(db_engine) == EXPECTED_TABLES
    assert _indexes(db_engine) >= EXPECTED_INDEXES
    assert schema_revision(database_url=migrated_database).is_current


def test_core_tables_match_the_migrated_schema(db_engine: Engine) -> None:
    """The same check ``alembic check`` performs - no drift between code and DDL."""
    with db_engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        differences = compare_metadata(context, metadata)

    assert differences == []


def test_schema_revision_reports_pending_work(migrated_database: str) -> None:
    downgrade(database_url=migrated_database, revision="-2", connect_timeout_s=5)

    revision = schema_revision(database_url=migrated_database)

    assert revision.revision == "0003_weather_hourly"
    assert revision.head == head_revision()
    assert revision.pending == ("0004_ingestion_run_errors", "0005_ingestion_watermarks")
    assert not revision.is_current

    upgrade(database_url=migrated_database, connect_timeout_s=5)
    assert schema_revision(database_url=migrated_database).is_current is True
