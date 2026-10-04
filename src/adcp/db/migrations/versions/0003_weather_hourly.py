"""weather_hourly fact table

Revision ID: 0003_weather_hourly
Revises: 0002_ingestion_runs
Create Date: 2026-10-02

Source of truth: docs/PLAN.md section 5.3 - the natural key
``(location_id, observed_at, source)`` is what makes ingestion idempotent.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_weather_hourly"
down_revision: str | None = "0002_ingestion_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "weather_hourly",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("temperature_2m", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("relative_humidity_2m", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("dew_point_2m", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("apparent_temperature", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("precipitation", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("rain", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("snowfall", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("weather_code", sa.SmallInteger(), nullable=True),
        sa.Column("cloud_cover", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("pressure_msl", sa.Numeric(precision=7, scale=2), nullable=True),
        sa.Column("wind_speed_10m", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("wind_direction_10m", sa.SmallInteger(), nullable=True),
        sa.Column("wind_gusts_10m", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("row_hash", sa.Text(), nullable=False),
        sa.Column("upstream_latitude", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column("upstream_longitude", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column("upstream_elevation_m", sa.Numeric(precision=7, scale=2), nullable=True),
        sa.Column("upstream_timezone", sa.Text(), nullable=True),
        sa.Column("first_seen_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_seen_run_id", postgresql.UUID(as_uuid=True), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["locations.id"],
            name="weather_hourly_location_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["first_seen_run_id"],
            ["ingestion_runs.id"],
            name="weather_hourly_first_seen_run_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["last_seen_run_id"],
            ["ingestion_runs.id"],
            name="weather_hourly_last_seen_run_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id"),
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
    op.create_index(
        "weather_hourly_location_time_idx",
        "weather_hourly",
        ["location_id", sa.text("observed_at DESC")],
        unique=False,
    )
    op.create_index(
        "weather_hourly_observed_at_idx",
        "weather_hourly",
        [sa.text("observed_at DESC")],
        unique=False,
    )
    op.create_index(
        "weather_hourly_source_idx",
        "weather_hourly",
        ["source", sa.text("observed_at DESC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("weather_hourly_source_idx", table_name="weather_hourly")
    op.drop_index("weather_hourly_observed_at_idx", table_name="weather_hourly")
    op.drop_index("weather_hourly_location_time_idx", table_name="weather_hourly")
    op.drop_table("weather_hourly")
