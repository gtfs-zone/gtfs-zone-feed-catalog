"""Fold this run's checks into per-source status, and record what changed.

Two jobs in one asset, because they are the same fold. First, a source's many
endpoints become one answer: an rt row naming vehicle, trip-update and alert
URLs is up only when all three answer, since a consumer that picks it expects
all three to load. A source with nothing checkable (path-only URLs, or an API
key we do not hold) is ``unknown``, which is deliberately not ``down``.

Second, each URL's check is written to Postgres, and a state change becomes a
row - but only the *change*. See ``history.models`` for why. History is per
URL, so a row's "since" is derived from its URLs' histories rather than kept.

Without a DATABASE_URL the fold still happens and the run still publishes; it
just keeps no history. That is what a developer without a database gets, rather
than a failed run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from dagster import asset
from sqlalchemy import delete, func, insert, select

from geometry_car.assets.endpoint_checks import CheckResult, result_key
from geometry_car.catalog import Source
from geometry_car.database import get_session_factory
from geometry_car.history.models import (
    CheckRun,
    EndpointRecord,
    EndpointState,
    SourceEndpoint,
    SourceRecord,
)
from geometry_car.settings import settings
from geometry_car.urls import normalize_url

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
    # Static rows only: the schedule's size and when its origin says it last
    # changed, for ranking schedules by recency without downloading any.
    content_length: int | None = None
    last_modified: datetime | None = None
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
        schedule = checks[0] if row.kind == "static" else None
        statuses[row.source_id] = SourceStatus(
            source_id=row.source_id,
            state=UP if all(r.ok for r in checks) else DOWN,
            checked_at=max(r.checked_at for r in checks),
            status_code=worst.status_code,
            error_class=worst.error_class,
            final_url=next((r.final_url for r in checks if r.final_url), ""),
            latency_ms=max((r.latency_ms or 0) for r in checks) or None,
            content_length=schedule.content_length if schedule else None,
            last_modified=schedule.last_modified if schedule else None,
        )
    return statuses


def _primary_url(row: Source) -> str:
    return next((row.urls[role] for role in row.roles if row.urls.get(role)), "")


def row_endpoints(row: Source) -> dict[str, str]:
    """role -> normalized URL, for the roles that have one."""
    return {
        role: key
        for role in row.roles
        if (key := normalize_url(row.urls.get(role, "")))
    }


def _aware(value: datetime) -> datetime:
    """Postgres returns aware datetimes; SQLite, in tests, naive UTC ones."""
    return value if value.tzinfo else value.replace(tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _Current:
    state: str
    since: datetime | None
    consecutive_failures: int


def _row_history(status: SourceStatus, endpoints: list[_Current]) -> SourceStatus:
    """A row's ``since`` and failure count, from its URLs' histories.

    An up row has been up since its last URL came up. A down row has been down
    since its earliest still-failing URL went down, and has failed as many runs
    running as its longest-failing URL.
    """
    if status.state == UP:
        sinces = [e.since for e in endpoints if e.state == UP and e.since]
        return replace(status, since=max(sinces, default=None), consecutive_failures=0)
    if status.state == DOWN:
        down = [e for e in endpoints if e.state == DOWN]
        sinces = [e.since for e in down if e.since]
        return replace(
            status,
            since=min(sinces, default=None),
            consecutive_failures=max((e.consecutive_failures for e in down), default=0),
        )
    return status


def _apply_check(endpoint: EndpointRecord, result: CheckResult, now: datetime) -> bool:
    """Copy one check's facts onto its endpoint. True when the state changed."""
    state = UP if result.ok else DOWN
    endpoint.last_checked = result.checked_at or now
    endpoint.status_code = result.status_code
    endpoint.error_class = result.error_class
    endpoint.content_type = result.content_type
    endpoint.content_length = result.content_length
    endpoint.last_modified = result.last_modified
    endpoint.etag = result.etag
    endpoint.final_url = result.final_url
    endpoint.latency_ms = result.latency_ms
    endpoint.consecutive_failures = (
        endpoint.consecutive_failures + 1 if state == DOWN else 0
    )
    changed = state != endpoint.state
    endpoint.state = state
    return changed


def record(
    rows: list[Source],
    statuses: dict[str, SourceStatus],
    results: dict[str, CheckResult],
    started_at: datetime,
) -> dict[str, SourceStatus]:
    """Write the run, the catalog state, each URL's check and any state change.

    Returns the statuses with ``since`` and ``consecutive_failures`` filled in,
    which only the history knows.
    """
    now = datetime.now(UTC)
    session_factory = get_session_factory()

    links = {row.source_id: row_endpoints(row) for row in rows}
    urls = {key for roles in links.values() for key in roles.values()}
    checked = {
        key: result
        for key in urls
        if (result := results.get(key)) is not None and not result.skipped
    }

    with session_factory() as session, session.begin():
        run = CheckRun(
            started_at=started_at,
            finished_at=now,
            sources_total=len(rows),
            endpoints_checked=len(checked),
            endpoints_ok=sum(1 for r in checked.values() if r.ok),
        )
        session.add(run)
        session.flush()

        existing = {
            record.source_id: record
            for record in session.scalars(select(SourceRecord)).all()
        }
        seen: set[str] = set()
        for row in rows:
            seen.add(row.source_id)
            current = existing.get(row.source_id)
            if current is None:
                current = SourceRecord(source_id=row.source_id, first_seen=now)
                session.add(current)
            current.catalog = row.catalog
            current.kind = row.kind
            current.name = row.name
            current.download_url = _primary_url(row)
            current.last_seen_in_catalog = now
            current.present = True

        # A source every catalog has dropped stops being present, but keeps its
        # row until retention takes it.
        for source_id, current in existing.items():
            if source_id not in seen:
                current.present = False

        endpoints = {
            endpoint.url: endpoint
            for endpoint in session.scalars(select(EndpointRecord)).all()
        }
        # When each URL last changed state, for the "up since" a UI shows.
        since_by_url = {
            url: _aware(changed_at)
            for url, changed_at in session.execute(
                select(EndpointState.url, func.max(EndpointState.changed_at)).group_by(
                    EndpointState.url
                )
            ).all()
        }
        for key in urls:
            endpoint = endpoints.get(key)
            if endpoint is None:
                # Column defaults apply at INSERT, not construction, so the
                # fields read below are set here.
                endpoint = EndpointRecord(
                    url=key, first_seen=now, state=UNKNOWN, consecutive_failures=0
                )
                session.add(endpoint)
                endpoints[key] = endpoint
            endpoint.last_seen = now
            result = checked.get(key)
            if result is not None and _apply_check(endpoint, result, now):
                session.add(
                    EndpointState(
                        url=key,
                        check_run_id=run.id,
                        state=endpoint.state,
                        changed_at=now,
                        status_code=result.status_code,
                        error_class=result.error_class,
                    )
                )
                since_by_url[key] = now
        session.flush()

        # Rewritten whole each run: it is a projection of today's catalog, and
        # an absent source's links are no longer anything's claim.
        session.execute(delete(SourceEndpoint))
        mapping = [
            {"source_id": source_id, "role": role, "url": key}
            for source_id, roles in links.items()
            for role, key in roles.items()
        ]
        if mapping:
            session.execute(insert(SourceEndpoint), mapping)

        enriched: dict[str, SourceStatus] = {}
        for row in rows:
            status = statuses[row.source_id]
            enriched[row.source_id] = _row_history(
                status,
                [
                    _Current(
                        state=endpoints[key].state,
                        since=since_by_url.get(key),
                        consecutive_failures=endpoints[key].consecutive_failures,
                    )
                    for key in set(links[row.source_id].values())
                    if key in checked
                ],
            )

    return enriched


@asset(description="Per-source status, folded from the checks and recorded in Postgres")
def check_history(
    sources: list[Source], endpoint_checks: dict[str, CheckResult]
) -> dict[str, SourceStatus]:
    started_at = datetime.now(UTC)
    statuses = fold(sources, endpoint_checks)

    if settings.database_url:
        statuses = record(sources, statuses, endpoint_checks, started_at)
    else:
        log.warning("DATABASE_URL is unset; this run keeps no history")

    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status.state] = counts.get(status.state, 0) + 1
    log.info("source states: %s", counts)
    return statuses
