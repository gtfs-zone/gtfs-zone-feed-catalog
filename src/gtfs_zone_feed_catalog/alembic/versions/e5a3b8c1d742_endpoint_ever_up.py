"""endpoint ever_up

Whether any check has seen a URL up, backfilled from its state changes.

Revision ID: e5a3b8c1d742
Revises: d2c9f4a7e310
Create Date: 2026-10-18 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e5a3b8c1d742"
down_revision: str | Sequence[str] | None = "d2c9f4a7e310"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "endpoint",
        sa.Column("ever_up", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.execute(
        "UPDATE endpoint SET ever_up = true WHERE state = 'up'"
        " OR url IN (SELECT url FROM endpoint_state WHERE state = 'up')"
    )


def downgrade() -> None:
    op.drop_column("endpoint", "ever_up")
