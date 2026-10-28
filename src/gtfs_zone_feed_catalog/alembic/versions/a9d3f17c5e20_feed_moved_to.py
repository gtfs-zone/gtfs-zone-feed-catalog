"""feed moved_to

The present feed that took an absent feed's members.

Revision ID: a9d3f17c5e20
Revises: e5a3b8c1d742
Create Date: 2026-10-28 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a9d3f17c5e20"
down_revision: str | Sequence[str] | None = "e5a3b8c1d742"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("feed", sa.Column("moved_to", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("feed", "moved_to")
