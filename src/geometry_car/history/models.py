"""Check history, shaped so storage does not grow with feeds x days.

Forty thousand endpoints checked daily is fifteen million rows a year if every
check is a row, and nobody ever reads fourteen million of them. So a check
writes a row only when the answer *changes*: ``source_state`` holds one row per
transition, and the current answer lives on ``source`` itself. A feed that has
been up for a year is one row.

These tables are this repo's, with this repo's Alembic. They share the database
with Dagster's own run and event storage, which owns its schema and creates it
itself; the names here do not collide with any of Dagster's.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class SourceRecord(Base):
    """Current catalog state, one row per source id."""

    __tablename__ = "source"

    source_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    catalog: Mapped[str] = mapped_column(String(32), nullable=False)
    kind: Mapped[str] = mapped_column(String(8), nullable=False)
    name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    # The row's primary endpoint, for reading the table without a join to the
    # published artifact. Not unique: two catalogs may list the same URL.
    download_url: Mapped[str] = mapped_column(Text, nullable=False, default="")

    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    last_seen_in_catalog: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    # False once a catalog stops listing it. Kept for the retention window so a
    # feed that vanishes for a week and returns does not lose its history.
    present: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    last_checked: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # up / down / unknown. unknown means nothing about it was checkable, which
    # is not the same as down.
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    consecutive_failures: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    __table_args__ = (
        Index("ix_source_present_last_seen", "present", "last_seen_in_catalog"),
        Index("ix_source_state_current", "state"),
    )


class CheckRun(Base):
    """One row per pipeline run, so a run's numbers survive the event log."""

    __tablename__ = "check_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    sources_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    endpoints_checked: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    endpoints_ok: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class SourceState(Base):
    """One row per *state change*, never one per check."""

    __tablename__ = "source_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("source.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    check_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("check_run.id", ondelete="SET NULL"), nullable=True
    )
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, index=True
    )
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_class: Mapped[str] = mapped_column(String(32), nullable=False, default="")
