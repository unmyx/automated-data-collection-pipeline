"""ingestion_run_errors table

Revision ID: 0004_ingestion_run_errors
Revises: 0003_weather_hourly
Create Date: 2026-10-02

Source of truth: docs/PLAN.md section 5.6.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_ingestion_run_errors"
down_revision: str | None = "0003_weather_hourly"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ingestion_run_errors",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=True),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("error_type", sa.Text(), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("attempt", sa.SmallInteger(), nullable=True),
        sa.Column("http_status", sa.SmallInteger(), nullable=True),
        sa.Column("request_url", sa.Text(), nullable=True),
        sa.Column("payload_sample", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["locations.id"],
            name="ingestion_run_errors_location_id_fkey",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["ingestion_runs.id"],
            name="ingestion_run_errors_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ingestion_run_errors_run_idx",
        "ingestion_run_errors",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ingestion_run_errors_error_type_idx",
        "ingestion_run_errors",
        ["error_type", sa.text("occurred_at DESC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ingestion_run_errors_error_type_idx", table_name="ingestion_run_errors")
    op.drop_index("ingestion_run_errors_run_idx", table_name="ingestion_run_errors")
    op.drop_table("ingestion_run_errors")
