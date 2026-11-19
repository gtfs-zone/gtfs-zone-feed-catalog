"""What each schedule's download actually held.

HEAD says a URL answers; it cannot say the body is a GTFS zip. ``feed_facts``
says, the same day, for every schedule ``feed_fetch`` stored: the outcome
(``ok``, ``not_zip``, ``missing_files``, ...) and, when ok, the zip's
feed_info, service range, agencies, counts and place.

For a URL ``feed_facts`` has nothing on, timetable-sites's content report is
the fallback: per shard, the outcome of its last download of each site's zip
and its facts, listed in ``_content/index.json`` at
``settings.content_report_base``. It is read over HTTPS like any consumer
would, and is the previous day's.

Contents are keyed by normalized URL and recorded against the URL's endpoint,
with one ``endpoint_content_state`` row per outcome change. A report that
cannot be fetched leaves the last recorded contents in place rather than
failing the run.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx
from dagster import asset
from sqlalchemy import select

from gtfs_zone_feed_catalog.assets.check_history import SourceStatus
from gtfs_zone_feed_catalog.database import get_session_factory
from gtfs_zone_feed_catalog.history.models import (
    EndpointContent,
    EndpointContentState,
    EndpointRecord,
)
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.urls import normalize_url

log = logging.getLogger(__name__)

INDEX_KEY = "_content/index.json"
# Report keys that are not facts about the zip.
ENTRY_KEYS = frozenset(
    {"feedId", "url", "checked", "version", "outcome", "since", "head", "detail"}
)


@dataclass(frozen=True, slots=True)
class FeedContent:
    """One URL's last download, from feed_facts or timetable-sites's report."""

    url: str
    outcome: str
    checked: date
    since: date
    detail: str = ""
    sha256: str = ""
    size: int | None = None
    # feedInfo, service, agencies, counts, routeTypes, filtered; ok only.
    facts: dict[str, Any] = field(default_factory=dict)


def parse_entry(entry: dict[str, Any]) -> FeedContent | None:
    """A report entry as a FeedContent, None when it names no absolute URL."""
    url = normalize_url(entry.get("url") or "")
    if not url or not entry.get("outcome") or not entry.get("checked"):
        return None
    facts = {
        key: value
        for key, value in entry.items()
        if key not in ENTRY_KEYS | {"sha256", "bytes"}
    }
    checked = date.fromisoformat(entry["checked"])
    return FeedContent(
        url=url,
        outcome=entry["outcome"],
        checked=checked,
        since=date.fromisoformat(entry.get("since") or entry["checked"]),
        detail=entry.get("detail") or "",
        sha256=entry.get("sha256") or "",
        size=entry.get("bytes"),
        facts=facts if entry["outcome"] == "ok" else {},
    )


def fetch_report(http: httpx.Client, base: str) -> dict[str, FeedContent]:
    """Every shard's entries by normalized URL. When two sites share a URL,
    the more recently checked entry wins."""
    index = http.get(f"{base}{INDEX_KEY}").raise_for_status().json()
    contents: dict[str, FeedContent] = {}
    for key in index.get("shards", []):
        shard = http.get(f"{base}{key}").raise_for_status().json()
        for entry in shard.values():
            content = parse_entry(entry) if entry else None
            if content is None:
                continue
            old = contents.get(content.url)
            if old is None or content.checked >= old.checked:
                contents[content.url] = content
    return contents


def _from_record(record: EndpointContent) -> FeedContent:
    return FeedContent(
        url=record.url,
        outcome=record.outcome,
        checked=record.checked,
        since=record.since,
        detail=record.detail,
        sha256=record.sha256,
        size=record.size,
        facts=record.facts or {},
    )


def record(
    reported: dict[str, FeedContent], now: datetime | None = None
) -> dict[str, FeedContent]:
    """Upsert the reported contents onto known endpoints, a state row per
    outcome change, and return every recorded content by URL. An entry older
    than what is recorded is ignored."""
    now = now or datetime.now(UTC)
    with get_session_factory()() as session, session.begin():
        endpoints = set(session.scalars(select(EndpointRecord.url)).all())
        existing = {
            row.url: row for row in session.scalars(select(EndpointContent)).all()
        }
        for url, content in reported.items():
            if url not in endpoints:
                continue
            row = existing.get(url)
            if row is not None and content.checked < row.checked:
                continue
            if row is None:
                row = EndpointContent(url=url)
                session.add(row)
                existing[url] = row
            changed = row.outcome != content.outcome
            row.checked = content.checked
            row.outcome = content.outcome
            row.since = content.since
            row.detail = content.detail
            row.sha256 = content.sha256
            row.size = content.size
            row.facts = content.facts or None
            if changed:
                session.add(
                    EndpointContentState(
                        url=url,
                        outcome=content.outcome,
                        detail=content.detail,
                        changed_at=now,
                    )
                )
        session.flush()
        return {url: _from_record(row) for url, row in existing.items()}


def recorded() -> dict[str, FeedContent]:
    """Every recorded content by URL."""
    with get_session_factory()() as session:
        rows = session.scalars(select(EndpointContent)).all()
        return {row.url: _from_record(row) for row in rows}


@asset(description="Each schedule's last download outcome and facts")
def feed_contents(
    check_history: dict[str, SourceStatus],
    feed_facts: dict[str, FeedContent],
) -> dict[str, FeedContent]:
    # check_history is an ordering input: every URL's endpoint row exists first.
    reported = site_report()
    if reported is None:
        if not feed_facts:
            return recorded() if settings.database_url else {}
        reported = {}
    log.info(
        "contents: %d urls from feed_facts, %d more from the site report",
        len(feed_facts),
        len(reported.keys() - feed_facts.keys()),
    )
    contents = reported | feed_facts
    if not settings.database_url:
        return contents
    return record(contents)


def site_report() -> dict[str, FeedContent] | None:
    """timetable-sites's content report, None when it cannot be read."""
    base = settings.content_report_base
    if not base:
        log.warning("CONTENT_REPORT_BASE is unset; no site report")
        return {}
    try:
        with httpx.Client(
            headers={"User-Agent": settings.check_user_agent},
            timeout=settings.check_timeout_seconds,
            follow_redirects=True,
        ) as http:
            return fetch_report(http, base)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("content report unavailable: %s", exc)
        return None
