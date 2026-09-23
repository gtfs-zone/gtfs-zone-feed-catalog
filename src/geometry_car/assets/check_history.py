"""Fold this run's checks into per-source status, and record what changed.

Two jobs in one asset, because they are the same fold. First, a source's many
endpoints become one answer: an rt row naming vehicle, trip-update and alert
URLs is up only when all three answer, since a consumer that picks it expects
all three to load. A source with nothing checkable (path-only URLs, or an API
key we do not hold) is ``unknown``, which is deliberately not ``down``.

Second, that answer is written to Postgres - but only the *change*. See
``history.models`` for why.

Without a DATABASE_URL the fold still happens and the run still publishes; it
just keeps no history. That is what a developer without a database gets, rather
than a failed run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from dagster import asset
from sqlalchemy import func, select

from geometry_car.assets.endpoint_checks import CheckResult, result_key
from geometry_car.catalog import Source
from geometry_car.database import get_session_factory
from geometry_car.history.models import CheckRun, SourceRecord, SourceState
from geometry_car.settings import settings

log = logging.getLogger(__name__)

UP = "up"
DOWN = "down"
UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SourceStatus:
    source_id: str
    state: str
    checked_at: datetime | None = None
    status_code: int | None = None
    error_class: str = ""
    # Set when the endpoint answered somewhere other than where it was aimed.
    final_url: str = ""
    latency_ms: int | None = None
    consecutive_failures: int = 0
    # When this state was first seen. Null until the database has been written
    # to at least once, since it is the history that knows.
    since: datetime | None = None


def fold(
    rows: list[Source], results: dict[str, CheckResult]
) -> dict[str, SourceStatus]:
    statuses: dict[str, SourceStatus] = {}
    for row in rows:
        checks = [
            result
            for role in row.roles
            if (url := row.urls.get(role))
            and (result := results.get(result_key(url))) is not None
            and not result.skipped
        ]
        if not checks:
            statuses[row.source_id] = SourceStatus(row.source_id, UNKNOWN)
            continue

        # The row's status is its worst endpoint: the first failure if there
        # is one, otherwise the slowest answer.
        worst = next(
            (r for r in checks if not r.ok),
            max(checks, key=lambda r: r.latency_ms or 0),
        )
        statuses[row.source_id] = SourceStatus(
            source_id=row.source_id,
            state=UP if all(r.ok for r in checks) else DOWN,
            checked_at=max(r.checked_at for r in checks),
            status_code=worst.status_code,
            error_class=worst.error_class,
            final_url=next((r.final_url for r in checks if r.final_url), ""),
            latency_ms=max((r.latency_ms or 0) for r in checks) or None,
        )
    return statuses


def _primary_url(row: Source) -> str:
    return next((row.urls[role] for role in row.roles if row.urls.get(role)), "")


def record(
    rows: list[Source], statuses: dict[str, SourceStatus], started_at: datetime
) -> dict[str, SourceStatus]:
    """Write the run, the catalog state and any state change.

    Returns the statuses with ``since`` and ``consecutive_failures`` filled in,
    which only the history knows.
    """
    now = datetime.now(UTC)
    session_factory = get_session_factory()

    with session_factory() as session, session.begin():
        run = CheckRun(
            started_at=started_at,
            finished_at=now,
            sources_total=len(rows),
            endpoints_checked=sum(1 for s in statuses.values() if s.state != UNKNOWN),
            endpoints_ok=sum(1 for s in statuses.values() if s.state == UP),
        )
        session.add(run)
        session.flush()

        existing = {
            record.source_id: record
            for record in session.scalars(select(SourceRecord)).all()
        }
        # When each source last changed state, for the "up since" a UI shows.
        since_by_id = dict(
            session.execute(
                select(
                    SourceState.source_id, func.max(SourceState.changed_at)
                ).group_by(SourceState.source_id)
            ).all()
        )

        seen: set[str] = set()
        enriched: dict[str, SourceStatus] = {}

        for row in rows:
            status = statuses[row.source_id]
            seen.add(row.source_id)
            current = existing.get(row.source_id)
            if current is None:
                # Column defaults apply at INSERT, not construction, so the
                # counter read below is set here.
                current = SourceRecord(
                    source_id=row.source_id,
                    first_seen=now,
                    state=UNKNOWN,
                    consecutive_failures=0,
                )
                session.add(current)

            current.catalog = row.catalog
            current.kind = row.kind
            current.name = row.name
            current.download_url = _primary_url(row)
            current.last_seen_in_catalog = now
            current.present = True

            if status.state != UNKNOWN:
                current.last_checked = status.checked_at or now
                if status.state != current.state:
                    session.add(
                        SourceState(
                            source_id=row.source_id,
                            check_run_id=run.id,
                            state=status.state,
                            changed_at=now,
                            status_code=status.status_code,
                            error_class=status.error_class,
                        )
                    )
                    current.state = status.state
                    since_by_id[row.source_id] = now
                current.consecutive_failures = (
                    current.consecutive_failures + 1 if status.state == DOWN else 0
                )

            enriched[row.source_id] = replace(
                status,
                consecutive_failures=current.consecutive_failures,
                since=since_by_id.get(row.source_id),
            )

        # A source every catalog has dropped stops being present, but keeps its
        # row until retention takes it.
        for source_id, current in existing.items():
            if source_id not in seen:
                current.present = False

    return enriched


@asset(description="Per-source status, folded from the checks and recorded in Postgres")
def check_history(
    sources: list[Source], endpoint_checks: dict[str, CheckResult]
) -> dict[str, SourceStatus]:
    started_at = datetime.now(UTC)
    statuses = fold(sources, endpoint_checks)

    if settings.database_url:
        statuses = record(sources, statuses, started_at)
    else:
        log.warning("DATABASE_URL is unset; this run keeps no history")

    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status.state] = counts.get(status.state, 0) + 1
    log.info("source states: %s", counts)
    return statuses
