"""endpoint content

What each URL's download held, from cape-flier's content report, plus one row
per change of that outcome.

Revision ID: d2c9f4a7e310
Revises: b4e81d2f6a07
Create Date: 2026-09-29 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d2c9f4a7e310"
down_revision: str | Sequence[str] | None = "b4e81d2f6a07"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "endpoint_content",
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("checked", sa.Date(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("since", sa.Date(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=True),
        sa.Column("facts", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["url"], ["endpoint.url"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("url"),
    )
    op.create_table(
        "endpoint_content_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["url"], ["endpoint.url"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_endpoint_content_state_url"),
        "endpoint_content_state",
        ["url"],
        unique=False,
    )
    op.create_index(
        op.f("ix_endpoint_content_state_changed_at"),
        "endpoint_content_state",
        ["changed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("endpoint_content_state")
    op.drop_table("endpoint_content")
