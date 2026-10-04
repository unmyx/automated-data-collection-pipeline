"""ingestion_status enum and ingestion_runs table

Revision ID: 0002_ingestion_runs
Revises: 0001_locations
Create Date: 2026-10-02

Source of truth: docs/PLAN.md section 5.5. The enum type is created and dropped
explicitly so that ``upgrade`` / ``downgrade -1`` round-trips cleanly.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_ingestion_runs"
down_revision: str | None = "0001_locations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ingestion_status = postgresql.ENUM(
    "running",
    "succeeded",
    "partial",
    "failed",
    "skipped",
    name="ingestion_status",
    create_type=False,
)


def upgrade() -> None:
    ingestion_status.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "ingestion_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
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
        sa.Column(
            "locations_succeeded",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
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
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name="ingestion_runs_finished_after_started",
        ),
    )
    op.create_index(
        "ingestion_runs_started_at_idx",
        "ingestion_runs",
        [sa.text("started_at DESC")],
        unique=False,
    )
    op.create_index(
        "ingestion_runs_status_idx",
        "ingestion_runs",
        ["status"],
        unique=False,
        postgresql_where=sa.text("status <> 'succeeded'"),
    )
    op.create_index(
        "ingestion_runs_open_idx",
        "ingestion_runs",
        ["started_at"],
        unique=False,
        postgresql_where=sa.text("finished_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ingestion_runs_open_idx", table_name="ingestion_runs")
    op.drop_index("ingestion_runs_status_idx", table_name="ingestion_runs")
    op.drop_index("ingestion_runs_started_at_idx", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
    ingestion_status.drop(op.get_bind(), checkfirst=True)
