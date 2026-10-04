"""ingestion_watermarks table

Revision ID: 0005_ingestion_watermarks
Revises: 0004_ingestion_run_errors
Create Date: 2026-10-02

Source of truth: docs/PLAN.md section 5.7. One row per (location, source); it is
advanced inside the same transaction as the observation upserts it accompanies.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005_ingestion_watermarks"
down_revision: str | None = "0004_ingestion_run_errors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ingestion_watermarks",
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["last_run_id"],
            ["ingestion_runs.id"],
            name="ingestion_watermarks_last_run_id_fkey",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["locations.id"],
            name="ingestion_watermarks_location_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("location_id", "source"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_watermarks")
