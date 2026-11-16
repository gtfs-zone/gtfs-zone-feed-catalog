"""endpoint version

The sha256 of each URL's last downloaded body, and one row per change of it.

Revision ID: f3b6d0e2a9c4
Revises: a9d3f17c5e20
Create Date: 2026-11-16 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f3b6d0e2a9c4"
down_revision: str | Sequence[str] | None = "a9d3f17c5e20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "endpoint",
        sa.Column("sha256", sa.String(64), nullable=False, server_default=""),
    )
    op.create_table(
        "endpoint_version",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("check_run_id", sa.Integer(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["url"], ["endpoint.url"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["check_run_id"], ["check_run.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_endpoint_version_url"), "endpoint_version", ["url"], unique=False
    )
    op.create_index(
        op.f("ix_endpoint_version_changed_at"),
        "endpoint_version",
        ["changed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("endpoint_version")
    op.drop_column("endpoint", "sha256")
