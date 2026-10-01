"""source name text

Revision ID: 7c2e5a1d9b30
Revises: 40f107be9c26
Create Date: 2026-09-23 13:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7c2e5a1d9b30"
down_revision: str | Sequence[str] | None = "40f107be9c26"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "source",
        "name",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "source",
        "name",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
        existing_nullable=False,
    )
