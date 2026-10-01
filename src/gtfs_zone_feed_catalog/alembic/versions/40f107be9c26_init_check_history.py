"""init check history

Revision ID: 40f107be9c26
Revises:
Create Date: 2026-09-22 22:31:44.910535

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "40f107be9c26"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "check_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sources_total", sa.Integer(), nullable=False),
        sa.Column("endpoints_checked", sa.Integer(), nullable=False),
        sa.Column("endpoints_ok", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "source",
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("catalog", sa.String(length=32), nullable=False),
        sa.Column("kind", sa.String(length=8), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("download_url", sa.Text(), nullable=False),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_in_catalog", sa.DateTime(timezone=True), nullable=False),
        sa.Column("present", sa.Boolean(), nullable=False),
        sa.Column("last_checked", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("source_id"),
    )
    op.create_index(
        "ix_source_present_last_seen",
        "source",
        ["present", "last_seen_in_catalog"],
        unique=False,
    )
    op.create_index("ix_source_state_current", "source", ["state"], unique=False)
    op.create_table(
        "source_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("check_run_id", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error_class", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["check_run_id"], ["check_run.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["source_id"], ["source.source_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_source_state_changed_at"), "source_state", ["changed_at"], unique=False
    )
    op.create_index(
        op.f("ix_source_state_source_id"), "source_state", ["source_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_source_state_source_id"), table_name="source_state")
    op.drop_index(op.f("ix_source_state_changed_at"), table_name="source_state")
    op.drop_table("source_state")
    op.drop_index("ix_source_state_current", table_name="source")
    op.drop_index("ix_source_present_last_seen", table_name="source")
    op.drop_table("source")
    op.drop_table("check_run")
