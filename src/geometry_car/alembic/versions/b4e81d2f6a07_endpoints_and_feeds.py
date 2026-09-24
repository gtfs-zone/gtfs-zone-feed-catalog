"""endpoints and feeds

History moves from the catalog row to the normalized URL, which is what is
checked. Each source's current state and its ``source_state`` rows move onto
the endpoint of its ``download_url``, then the per-source columns and table go.

When several sources share a URL, the one checked most recently brings its
history and the others' is dropped: it describes the same URL. An rt row's
state was the fold of all its URLs, but only its primary URL is on record, so
that endpoint may take a failure that was really a sibling URL's; the next run
corrects it with one extra transition. ``source_endpoint`` is left empty for
the next run to fill, since only the catalog knows each row's other roles.

Revision ID: b4e81d2f6a07
Revises: 7c2e5a1d9b30
Create Date: 2026-09-24 12:00:00.000000

"""

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

from geometry_car.urls import normalize_url

revision: str = "b4e81d2f6a07"
down_revision: str | Sequence[str] | None = "7c2e5a1d9b30"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

source = sa.table(
    "source",
    sa.column("source_id", sa.String),
    sa.column("download_url", sa.Text),
    sa.column("last_seen_in_catalog", sa.DateTime(timezone=True)),
    sa.column("first_seen", sa.DateTime(timezone=True)),
    sa.column("last_checked", sa.DateTime(timezone=True)),
    sa.column("state", sa.String),
    sa.column("consecutive_failures", sa.Integer),
)
source_state = sa.table(
    "source_state",
    sa.column("id", sa.Integer),
    sa.column("source_id", sa.String),
    sa.column("check_run_id", sa.Integer),
    sa.column("state", sa.String),
    sa.column("changed_at", sa.DateTime(timezone=True)),
    sa.column("status_code", sa.Integer),
    sa.column("error_class", sa.String),
)
endpoint = sa.table(
    "endpoint",
    sa.column("url", sa.Text),
    sa.column("first_seen", sa.DateTime(timezone=True)),
    sa.column("last_seen", sa.DateTime(timezone=True)),
    sa.column("last_checked", sa.DateTime(timezone=True)),
    sa.column("state", sa.String),
    sa.column("consecutive_failures", sa.Integer),
    sa.column("status_code", sa.Integer),
    sa.column("error_class", sa.String),
    sa.column("content_type", sa.Text),
    sa.column("etag", sa.Text),
    sa.column("final_url", sa.Text),
)
endpoint_state = sa.table(
    "endpoint_state",
    sa.column("url", sa.Text),
    sa.column("check_run_id", sa.Integer),
    sa.column("state", sa.String),
    sa.column("changed_at", sa.DateTime(timezone=True)),
    sa.column("status_code", sa.Integer),
    sa.column("error_class", sa.String),
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _aware(value: datetime | None) -> datetime:
    """SQLite hands back naive datetimes; Postgres aware ones."""
    if value is None:
        return EPOCH
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _create_tables() -> None:
    op.create_table(
        "endpoint",
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_checked", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error_class", sa.String(length=32), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("content_length", sa.BigInteger(), nullable=True),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=True),
        sa.Column("etag", sa.Text(), nullable=False),
        sa.Column("final_url", sa.Text(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("url"),
    )
    op.create_index("ix_endpoint_last_seen", "endpoint", ["last_seen"], unique=False)
    op.create_index("ix_endpoint_state", "endpoint", ["state"], unique=False)

    op.create_table(
        "endpoint_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("check_run_id", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error_class", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["check_run_id"], ["check_run.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["url"], ["endpoint.url"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_endpoint_state_changed_at"),
        "endpoint_state",
        ["changed_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_endpoint_state_url"), "endpoint_state", ["url"], unique=False
    )

    op.create_table(
        "source_endpoint",
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_id"], ["source.source_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["url"], ["endpoint.url"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("source_id", "role"),
    )
    op.create_index(
        op.f("ix_source_endpoint_url"), "source_endpoint", ["url"], unique=False
    )

    op.create_table(
        "feed",
        sa.Column("feed_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("present", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("feed_id"),
    )
    op.create_index(
        "ix_feed_present_last_seen", "feed", ["present", "last_seen"], unique=False
    )

    op.create_table(
        "feed_member",
        sa.Column("feed_id", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(["feed_id"], ["feed.feed_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_id"], ["source.source_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("feed_id", "source_id", "role"),
    )
    op.create_index(
        op.f("ix_feed_member_source_id"), "feed_member", ["source_id"], unique=False
    )


def _migrate_state() -> None:
    bind = op.get_bind()

    # Per normalized URL, the source checked most recently speaks for it.
    chosen: dict[str, dict] = {}
    for row in bind.execute(sa.select(source)).mappings():
        key = normalize_url(row["download_url"] or "")
        if not key:
            continue
        best = chosen.get(key)
        if best is None or _aware(row["last_checked"]) > _aware(best["last_checked"]):
            chosen[key] = dict(row)

    if not chosen:
        return
    url_by_source = {row["source_id"]: key for key, row in chosen.items()}

    history: dict[str, list[dict]] = {key: [] for key in chosen}
    for row in bind.execute(
        sa.select(source_state).order_by(source_state.c.changed_at, source_state.c.id)
    ).mappings():
        if (key := url_by_source.get(row["source_id"])) is not None:
            history[key].append(dict(row))

    endpoints = []
    for key, row in chosen.items():
        latest = history[key][-1] if history[key] else {}
        endpoints.append(
            {
                "url": key,
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen_in_catalog"],
                "last_checked": row["last_checked"],
                "state": row["state"],
                "consecutive_failures": row["consecutive_failures"],
                "status_code": latest.get("status_code"),
                "error_class": latest.get("error_class") or "",
                "content_type": "",
                "etag": "",
                "final_url": "",
            }
        )
    op.bulk_insert(endpoint, endpoints)
    op.bulk_insert(
        endpoint_state,
        [
            {
                "url": key,
                "check_run_id": state["check_run_id"],
                "state": state["state"],
                "changed_at": state["changed_at"],
                "status_code": state["status_code"],
                "error_class": state["error_class"],
            }
            for key, states in history.items()
            for state in states
        ],
    )


def upgrade() -> None:
    _create_tables()
    _migrate_state()

    op.drop_index(op.f("ix_source_state_source_id"), table_name="source_state")
    op.drop_index(op.f("ix_source_state_changed_at"), table_name="source_state")
    op.drop_table("source_state")
    op.drop_index("ix_source_state_current", table_name="source")
    with op.batch_alter_table("source") as batch:
        batch.drop_column("consecutive_failures")
        batch.drop_column("state")
        batch.drop_column("last_checked")


def downgrade() -> None:
    """Puts each source's state back from its download URL's endpoint.

    Lossy the other way too: sources sharing a URL all get its one history.
    """
    with op.batch_alter_table("source") as batch:
        batch.add_column(
            sa.Column("last_checked", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "state", sa.String(length=16), nullable=False, server_default="unknown"
            )
        )
        batch.add_column(
            sa.Column(
                "consecutive_failures",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
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

    bind = op.get_bind()
    endpoints = {
        row["url"]: row for row in bind.execute(sa.select(endpoint)).mappings()
    }
    states: dict[str, list[dict]] = {}
    for row in bind.execute(
        sa.select(endpoint_state).order_by(endpoint_state.c.changed_at)
    ).mappings():
        states.setdefault(row["url"], []).append(dict(row))

    restored = []
    for row in bind.execute(
        sa.select(source.c.source_id, source.c.download_url)
    ).mappings():
        key = normalize_url(row["download_url"] or "")
        if (current := endpoints.get(key)) is None:
            continue
        bind.execute(
            source.update()
            .where(source.c.source_id == row["source_id"])
            .values(
                last_checked=current["last_checked"],
                state=current["state"],
                consecutive_failures=current["consecutive_failures"],
            )
        )
        restored += [
            {
                "source_id": row["source_id"],
                "check_run_id": state["check_run_id"],
                "state": state["state"],
                "changed_at": state["changed_at"],
                "status_code": state["status_code"],
                "error_class": state["error_class"],
            }
            for state in states.get(key, [])
        ]
    if restored:
        op.bulk_insert(source_state, restored)

    op.drop_index(op.f("ix_feed_member_source_id"), table_name="feed_member")
    op.drop_table("feed_member")
    op.drop_index("ix_feed_present_last_seen", table_name="feed")
    op.drop_table("feed")
    op.drop_index(op.f("ix_source_endpoint_url"), table_name="source_endpoint")
    op.drop_table("source_endpoint")
    op.drop_index(op.f("ix_endpoint_state_url"), table_name="endpoint_state")
    op.drop_index(op.f("ix_endpoint_state_changed_at"), table_name="endpoint_state")
    op.drop_table("endpoint_state")
    op.drop_index("ix_endpoint_state", table_name="endpoint")
    op.drop_index("ix_endpoint_last_seen", table_name="endpoint")
    op.drop_table("endpoint")
