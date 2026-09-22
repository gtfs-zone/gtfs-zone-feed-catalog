"""Delete history nobody will read, daily, in the same run that wrote it.

Retention is load-bearing rather than tidy. ``source_state`` grows with every
up/down flap across forty thousand endpoints, and a catalog that churns ids
would otherwise leave a permanent row per id ever seen. Both windows are
settings, and both default generously: 400 days of transitions is more than a
year-over-year comparison needs, and 90 days absent is long enough that a feed
which disappears for a season comes back to its own history.

Same idea as ``gtfs/traccar/retention-cronjob.yaml`` in the deploy repo, run
from the pipeline instead of a CronJob because the pipeline already runs daily
and already holds the connection.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from dagster import asset
from sqlalchemy import delete

from geometry_car.assets.check_history import SourceStatus
from geometry_car.database import get_session_factory
from geometry_car.history.models import SourceRecord, SourceState
from geometry_car.settings import settings

log = logging.getLogger(__name__)


def prune(now: datetime | None = None) -> tuple[int, int]:
    """Returns (state rows deleted, source rows deleted)."""
    now = now or datetime.now(UTC)
    state_cutoff = now - timedelta(days=settings.state_retention_days)
    source_cutoff = now - timedelta(days=settings.source_absent_retention_days)

    with get_session_factory()() as session, session.begin():
        states = session.execute(
            delete(SourceState).where(SourceState.changed_at < state_cutoff)
        ).rowcount
        # Only rows no catalog still lists: a present source keeps its row
        # however old it is.
        sources = session.execute(
            delete(SourceRecord).where(
                SourceRecord.present.is_(False),
                SourceRecord.last_seen_in_catalog < source_cutoff,
            )
        ).rowcount
    return states, sources


@asset(description="Delete history past its retention window")
def history_retention(check_history: dict[str, SourceStatus]) -> None:
    if not settings.database_url:
        log.warning("DATABASE_URL is unset; nothing to prune")
        return
    states, sources = prune()
    log.info("pruned %d state rows and %d absent sources", states, sources)
