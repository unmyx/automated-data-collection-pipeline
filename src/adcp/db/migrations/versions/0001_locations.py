"""locations table

Revision ID: 0001_locations
Revises:
Create Date: 2026-10-02

Source of truth: docs/PLAN.md section 5.2.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_locations"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "locations",
        sa.Column(
            "id",
            sa.BigInteger(),
            sa.Identity(always=True),
            nullable=False,
        ),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("latitude", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column("longitude", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column("timezone", sa.Text(), nullable=False, server_default=sa.text("'UTC'")),
        sa.Column("country_code", sa.CHAR(length=2), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="locations_slug_key"),
        sa.CheckConstraint("latitude BETWEEN -90 AND 90", name="locations_lat_range"),
        sa.CheckConstraint("longitude BETWEEN -180 AND 180", name="locations_lon_range"),
        sa.CheckConstraint(
            r"slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'",
            name="locations_slug_format",
        ),
    )
    op.create_index(
        "locations_active_idx",
        "locations",
        ["is_active"],
        unique=False,
        postgresql_where=sa.text("is_active"),
    )


def downgrade() -> None:
    op.drop_index("locations_active_idx", table_name="locations")
    op.drop_table("locations")
