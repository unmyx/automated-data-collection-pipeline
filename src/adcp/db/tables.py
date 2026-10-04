"""SQLAlchemy Core table definitions - the schema's single source of truth in code.

These are **Core tables, not ORM entities**: the plan (``docs/PLAN.md`` section
3.3) deliberately chose "SQLAlchemy 2.0 Core + lightweight repositories" over an
ORM object graph. The definitions below mirror the DDL in ``docs/PLAN.md``
section 5 exactly - column types, nullability, constraint names, index names, and
partial-index predicates. Alembic's ``env.py`` points ``target_metadata`` at
``metadata`` so ``alembic check`` can detect drift.

``create_all()`` is intentionally never called: Alembic owns all DDL.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

#: Values of the ``ingestion_status`` PostgreSQL enum (PLAN section 5.5).
INGESTION_STATUS_VALUES: tuple[str, ...] = (
    "running",
    "succeeded",
    "partial",
    "failed",
    "skipped",
)

#: Allowed values of ``weather_hourly.source`` (PLAN section 5.3).
WEATHER_SOURCE_VALUES: tuple[str, ...] = (
    "forecast",
    "historical_forecast",
    "archive",
)

#: Shared metadata; Alembic's target for autogenerate and ``alembic check``.
metadata = sa.MetaData()

#: The PostgreSQL enum type backing ``ingestion_runs.status``. Created and dropped
#: explicitly by the migration that owns it, hence ``create_type=False``.
ingestion_status = postgresql.ENUM(
    *INGESTION_STATUS_VALUES,
    name="ingestion_status",
    create_type=False,
)


locations = sa.Table(
    "locations",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
    sa.Column("slug", sa.Text(), nullable=False),
    sa.Column("name", sa.Text(), nullable=False),
    sa.Column("latitude", sa.Numeric(9, 6), nullable=False),
    sa.Column("longitude", sa.Numeric(9, 6), nullable=False),
    sa.Column("timezone", sa.Text(), nullable=False, server_default=sa.text("'UTC'")),
    sa.Column("country_code", sa.CHAR(2), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.UniqueConstraint("slug", name="locations_slug_key"),
    sa.CheckConstraint("latitude BETWEEN -90 AND 90", name="locations_lat_range"),
    sa.CheckConstraint("longitude BETWEEN -180 AND 180", name="locations_lon_range"),
    sa.CheckConstraint(
        r"slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'",
        name="locations_slug_format",
    ),
)
sa.Index("locations_active_idx", locations.c.is_active, postgresql_where=sa.text("is_active"))


ingestion_runs = sa.Table(
    "ingestion_runs",
    metadata,
    sa.Column(
        "id",
        postgresql.UUID(as_uuid=True),
        primary_key=True,
        server_default=sa.text("gen_random_uuid()"),
    ),
    sa.Column("run_type", sa.Text(), nullable=False),
    sa.Column("trigger", sa.Text(), nullable=False),
    sa.Column(
        "status",
        ingestion_status,
        nullable=False,
        server_default=sa.text("'running'::ingestion_status"),
    ),
    sa.Column("requested_from", sa.DateTime(timezone=True), nullable=True),
    sa.Column("requested_to", sa.DateTime(timezone=True), nullable=True),
    sa.Column("window_from", sa.DateTime(timezone=True), nullable=True),
    sa.Column("window_to", sa.DateTime(timezone=True), nullable=True),
    sa.Column("locations_total", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("locations_succeeded", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("locations_failed", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("requests_made", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("requests_retried", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("rows_received", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("rows_inserted", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("rows_updated", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("rows_unchanged", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("rows_rejected", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("error_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("error_summary", sa.Text(), nullable=True),
    sa.Column("app_version", sa.Text(), nullable=False),
    sa.Column("hostname", sa.Text(), nullable=True),
    sa.Column(
        "started_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("duration_ms", sa.Integer(), nullable=True),
    sa.CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="ingestion_runs_finished_after_started",
    ),
)
sa.Index("ingestion_runs_started_at_idx", ingestion_runs.c.started_at.desc())
sa.Index(
    "ingestion_runs_status_idx",
    ingestion_runs.c.status,
    postgresql_where=sa.text("status <> 'succeeded'"),
)
sa.Index(
    "ingestion_runs_open_idx",
    ingestion_runs.c.started_at,
    postgresql_where=sa.text("finished_at IS NULL"),
)


weather_hourly = sa.Table(
    "weather_hourly",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
    sa.Column(
        "location_id",
        sa.BigInteger(),
        sa.ForeignKey("locations.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("source", sa.Text(), nullable=False),
    sa.Column("temperature_2m", sa.Numeric(5, 2), nullable=True),
    sa.Column("relative_humidity_2m", sa.Numeric(5, 2), nullable=True),
    sa.Column("dew_point_2m", sa.Numeric(5, 2), nullable=True),
    sa.Column("apparent_temperature", sa.Numeric(5, 2), nullable=True),
    sa.Column("precipitation", sa.Numeric(6, 2), nullable=True),
    sa.Column("rain", sa.Numeric(6, 2), nullable=True),
    sa.Column("snowfall", sa.Numeric(6, 2), nullable=True),
    sa.Column("weather_code", sa.SmallInteger(), nullable=True),
    sa.Column("cloud_cover", sa.Numeric(5, 2), nullable=True),
    sa.Column("pressure_msl", sa.Numeric(7, 2), nullable=True),
    sa.Column("wind_speed_10m", sa.Numeric(6, 2), nullable=True),
    sa.Column("wind_direction_10m", sa.SmallInteger(), nullable=True),
    sa.Column("wind_gusts_10m", sa.Numeric(6, 2), nullable=True),
    sa.Column("row_hash", sa.Text(), nullable=False),
    sa.Column("upstream_latitude", sa.Numeric(9, 6), nullable=False),
    sa.Column("upstream_longitude", sa.Numeric(9, 6), nullable=False),
    sa.Column("upstream_elevation_m", sa.Numeric(7, 2), nullable=True),
    sa.Column("upstream_timezone", sa.Text(), nullable=True),
    sa.Column(
        "first_seen_run_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("ingestion_runs.id"),
        nullable=False,
    ),
    sa.Column(
        "last_seen_run_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("ingestion_runs.id"),
        nullable=False,
    ),
    sa.Column(
        "first_collected_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column(
        "last_collected_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column("revision_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.UniqueConstraint(
        "location_id",
        "observed_at",
        "source",
        name="weather_hourly_natural_key",
    ),
    sa.CheckConstraint(
        "date_trunc('hour', observed_at) = observed_at",
        name="weather_hourly_hour_aligned",
    ),
    sa.CheckConstraint(
        "source IN ('forecast','historical_forecast','archive')",
        name="weather_hourly_source_valid",
    ),
)
sa.Index(
    "weather_hourly_location_time_idx",
    weather_hourly.c.location_id,
    weather_hourly.c.observed_at.desc(),
)
sa.Index("weather_hourly_observed_at_idx", weather_hourly.c.observed_at.desc())
sa.Index(
    "weather_hourly_source_idx",
    weather_hourly.c.source,
    weather_hourly.c.observed_at.desc(),
)


ingestion_run_errors = sa.Table(
    "ingestion_run_errors",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
    sa.Column(
        "run_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("ingestion_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "location_id",
        sa.BigInteger(),
        sa.ForeignKey("locations.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column("phase", sa.Text(), nullable=False),
    sa.Column("error_type", sa.Text(), nullable=False),
    sa.Column("error_code", sa.Text(), nullable=True),
    sa.Column("message", sa.Text(), nullable=False),
    sa.Column("attempt", sa.SmallInteger(), nullable=True),
    sa.Column("http_status", sa.SmallInteger(), nullable=True),
    sa.Column("request_url", sa.Text(), nullable=True),
    sa.Column("payload_sample", postgresql.JSONB(), nullable=True),
    sa.Column(
        "occurred_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
)
sa.Index("ingestion_run_errors_run_idx", ingestion_run_errors.c.run_id)
sa.Index(
    "ingestion_run_errors_error_type_idx",
    ingestion_run_errors.c.error_type,
    ingestion_run_errors.c.occurred_at.desc(),
)


ingestion_watermarks = sa.Table(
    "ingestion_watermarks",
    metadata,
    sa.Column(
        "location_id",
        sa.BigInteger(),
        sa.ForeignKey("locations.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    sa.Column("source", sa.Text(), primary_key=True),
    sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "last_run_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("ingestion_runs.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
)


__all__ = [
    "INGESTION_STATUS_VALUES",
    "WEATHER_SOURCE_VALUES",
    "ingestion_run_errors",
    "ingestion_runs",
    "ingestion_status",
    "ingestion_watermarks",
    "locations",
    "metadata",
    "weather_hourly",
]
