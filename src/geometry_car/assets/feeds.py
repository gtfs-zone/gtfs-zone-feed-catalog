"""Logical feeds: one per transit system, across catalogs and roles.

Catalog rows stay the raw layer (see ``sources``). A logical feed bundles the
rows that describe the same system, so a consumer lists it once instead of once
per catalog and once per realtime entity type. Rows join a feed through a
union-find over four kinds of evidence, and nothing else:

- a shared normalized URL, of any kind, in any catalog;
- a Mobility Database realtime row's ``feed_references`` to a static row;
- realtime URLs that differ only in a final entity-type segment
  (``x/vehicles``, ``x/trips``, ``x/alerts``), see ``urls.rt_sibling_key``;
- the static and realtime rows one catalog feed was split into.

No fuzzy name matching and, for now, no override file, so a wrong link cannot be
undone except by changing the rules. The groups can false-merge: one regional
schedule referenced by several agencies' realtime feeds makes them one feed.

Feed ids go in shareable URLs, so they are persisted and sticky: each group
takes the id of the existing feed it shares the most members with, and a new id
is minted only for a group that shares none. A feed that loses its group keeps
its row, and its remaining members, for the retention window, so a group that
comes back gets its old id.
"""

import hashlib
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

from dagster import asset
from sqlalchemy import delete, insert, select, update

from geometry_car.assets.check_history import DOWN, UNKNOWN, UP
from geometry_car.assets.endpoint_checks import CheckResult, result_key
from geometry_car.catalog import RT_ROLES, Place, Source
from geometry_car.database import get_session_factory
from geometry_car.history.models import FeedMember, FeedRecord, SourceRecord
from geometry_car.settings import settings
from geometry_car.urls import normalize_url, rt_sibling_key

log = logging.getLogger(__name__)

# Whose name a feed takes, best first.
NAME_PRIORITY = {"curated": 0, "mobilitydatabase": 1, "transitland": 2}


@dataclass(frozen=True, slots=True)
class Feed:
    feed_id: str
    name: str
    # Source ids, sorted.
    members: tuple[str, ...]
    # (source id, role) for every role a member fills, sorted.
    member_roles: tuple[tuple[str, str], ...]
    # role -> the members' URLs for it, one per normalized form, best first:
    # answering URLs ahead of the rest, schedules then by Last-Modified.
    urls: dict[str, tuple[str, ...]]
    # role -> up when any of its URLs answers, so a consumer can load it.
    role_state: dict[str, str]
    # up only when every checkable URL answers.
    state: str
    place: Place
    # From the best scheduled URL: up first, then the most recently modified.
    static_bytes: int | None = None
    last_modified: datetime | None = None
    # Roles whose every URL needs an API key we do not hold, so none is checked.
    auth_roles: tuple[str, ...] = ()


class _UnionFind:
    def __init__(self, items: list[str]) -> None:
        self._parent = {item: item for item in items}

    def find(self, item: str) -> str:
        root = item
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[item] != root:
            self._parent[item], item = root, self._parent[item]
        return root

    def union(self, items: list[str]) -> None:
        roots = sorted({self.find(item) for item in items})
        for root in roots[1:]:
            self._parent[root] = roots[0]


def group(rows: list[Source]) -> list[list[Source]]:
    """Rows describing the same system, each group sorted by source id."""
    by_id = {row.source_id: row for row in rows}
    links: dict[tuple[str, ...], list[str]] = defaultdict(list)

    for row in rows:
        links[("feed", row.catalog, row.feed_id)].append(row.source_id)
        for role, url in row.urls.items():
            if key := normalize_url(url):
                links[("url", key)].append(row.source_id)
            if role in RT_ROLES and (sibling := rt_sibling_key(url)):
                links[("rt", sibling)].append(row.source_id)
        if row.catalog == "mobilitydatabase":
            for ref in row.feed_references:
                if (target := f"md:{ref}:static") in by_id:
                    links[("ref", row.source_id, ref)] += [row.source_id, target]

    union = _UnionFind(list(by_id))
    for members in links.values():
        union.union(members)

    groups: dict[str, list[Source]] = defaultdict(list)
    for source_id in sorted(by_id):
        groups[union.find(source_id)].append(by_id[source_id])
    return sorted(groups.values(), key=lambda members: members[0].source_id)


def _mint(members: list[str], taken: set[str]) -> str:
    seed = "\n".join(sorted(members))
    for attempt in range(1000):
        digest = hashlib.sha256(f"{seed}\n{attempt}".encode()).hexdigest()
        feed_id = f"f-{digest[:10]}"
        if feed_id not in taken:
            return feed_id
    raise RuntimeError("could not mint an unused feed id")


def assign_ids(groups: list[list[str]], existing: dict[str, set[str]]) -> list[str]:
    """One id per group, reusing the existing feed with the most shared members.

    Greedy by overlap, largest first, and each existing id goes to one group at
    most: when a feed splits, the larger part keeps the id. Ties go to the lower
    id so a rerun on the same input assigns the same ids.
    """
    owner: dict[str, str] = {
        source_id: feed_id
        for feed_id, members in existing.items()
        for source_id in members
    }
    candidates = []
    for index, members in enumerate(groups):
        overlap = Counter(owner[m] for m in members if m in owner)
        candidates += [(-count, feed_id, index) for feed_id, count in overlap.items()]

    ids: list[str | None] = [None] * len(groups)
    claimed: set[str] = set()
    for _, feed_id, index in sorted(candidates):
        if ids[index] is None and feed_id not in claimed:
            ids[index] = feed_id
            claimed.add(feed_id)

    taken = set(existing) | claimed
    for index, members in enumerate(groups):
        if ids[index] is None:
            ids[index] = _mint(members, taken)
            taken.add(ids[index])
    return [feed_id for feed_id in ids if feed_id is not None]


def _url_state(url: str, results: dict[str, CheckResult]) -> str:
    result = results.get(result_key(url))
    if result is None or result.skipped:
        return UNKNOWN
    return UP if result.ok else DOWN


def _any_up(states: list[str]) -> str:
    if UP in states:
        return UP
    return DOWN if DOWN in states else UNKNOWN


def _all_up(states: list[str]) -> str:
    checked = [state for state in states if state != UNKNOWN]
    if not checked:
        return UNKNOWN
    return UP if all(state == UP for state in checked) else DOWN


def _name_order(row: Source) -> tuple[int, int, str]:
    return (NAME_PRIORITY.get(row.catalog, 9), row.kind != "static", row.source_id)


def build_feed(
    feed_id: str, members: list[Source], results: dict[str, CheckResult]
) -> Feed:
    ordered = sorted(members, key=_name_order)

    urls: dict[str, dict[str, str]] = defaultdict(dict)
    member_roles: list[tuple[str, str]] = []
    for row in ordered:
        for role in row.roles:
            if url := row.urls.get(role):
                urls[role].setdefault(normalize_url(url) or url, url)
                member_roles.append((row.source_id, role))

    # Up, then unchecked, then down; within each, the most recently modified.
    order = {UP: 0, UNKNOWN: 1, DOWN: 2}

    def rank(url: str) -> tuple[int, float]:
        result = results.get(result_key(url))
        modified = (result and result.last_modified) or datetime.min.replace(tzinfo=UTC)
        return (order[_url_state(url, results)], -modified.timestamp())

    # Stable, so ties keep the name order and a rerun lists the same URL first.
    ranked = {role: sorted(by_key.values(), key=rank) for role, by_key in urls.items()}
    states = {
        role: [_url_state(url, results) for url in role_urls]
        for role, role_urls in ranked.items()
    }
    auth_roles = tuple(
        sorted(
            role
            for role, role_urls in ranked.items()
            if all(
                getattr(results.get(result_key(url)), "error_class", "")
                == "auth_required"
                for url in role_urls
            )
        )
    )

    place = next(
        (row.place for row in ordered if row.place.placed),
        next((row.place for row in ordered if row.place.country_code), Place()),
    )

    best = next(
        (
            result
            for url in ranked.get("scheduled", [])
            if (result := results.get(result_key(url))) and not result.skipped
        ),
        None,
    )

    return Feed(
        feed_id=feed_id,
        name=ordered[0].name,
        members=tuple(sorted(row.source_id for row in members)),
        member_roles=tuple(sorted(member_roles)),
        urls={role: tuple(role_urls) for role, role_urls in ranked.items()},
        role_state={role: _any_up(role_states) for role, role_states in states.items()},
        state=_all_up([s for role_states in states.values() for s in role_states]),
        place=place,
        static_bytes=best.content_length if best else None,
        last_modified=best.last_modified if best else None,
        auth_roles=auth_roles,
    )


def load_existing() -> dict[str, set[str]]:
    with get_session_factory()() as session:
        existing: dict[str, set[str]] = {
            feed_id: set() for feed_id in session.scalars(select(FeedRecord.feed_id))
        }
        for feed_id, source_id in session.execute(
            select(FeedMember.feed_id, FeedMember.source_id)
        ):
            existing[feed_id].add(source_id)
    return existing


def persist(feeds: list[Feed], now: datetime | None = None) -> None:
    """Upsert today's feeds and their members; mark the rest absent.

    A source belongs to one feed at a time, so a present source's old
    memberships are cleared before today's are written. An absent feed keeps
    only members no present feed has, which is what lets it be matched again
    if they return.
    """
    now = now or datetime.now(UTC)
    with get_session_factory()() as session, session.begin():
        records = {
            record.feed_id: record for record in session.scalars(select(FeedRecord))
        }
        current = {feed.feed_id for feed in feeds}
        for feed in feeds:
            record = records.get(feed.feed_id)
            if record is None:
                record = FeedRecord(feed_id=feed.feed_id, first_seen=now)
                session.add(record)
            record.name = feed.name
            record.last_seen = now
            record.present = True
        session.flush()

        session.execute(
            delete(FeedMember).where(
                FeedMember.source_id.in_(
                    select(SourceRecord.source_id).where(SourceRecord.present)
                )
            )
        )
        mapping = [
            {"feed_id": feed.feed_id, "source_id": source_id, "role": role}
            for feed in feeds
            for source_id, role in feed.member_roles
        ]
        if mapping:
            session.execute(insert(FeedMember), mapping)

        gone = [feed_id for feed_id in records if feed_id not in current]
        if gone:
            session.execute(
                update(FeedRecord)
                .where(FeedRecord.feed_id.in_(gone), FeedRecord.present)
                .values(present=False)
            )


def build_feeds(
    rows: list[Source],
    results: dict[str, CheckResult],
    existing: dict[str, set[str]],
) -> list[Feed]:
    groups = group(rows)
    ids = assign_ids([[row.source_id for row in g] for g in groups], existing)
    return sorted(
        (
            build_feed(feed_id, g, results)
            for feed_id, g in zip(ids, groups, strict=True)
        ),
        key=lambda feed: feed.feed_id,
    )


@asset(
    description="Logical feeds: catalog rows grouped per transit system, sticky ids",
    # Ordering only: members reference source rows check_history writes.
    deps=["check_history"],
)
def feeds(sources: list[Source], endpoint_checks: dict[str, CheckResult]) -> list[Feed]:
    existing = load_existing() if settings.database_url else {}
    result = build_feeds(sources, endpoint_checks, existing)
    if settings.database_url:
        persist(result)
    else:
        log.warning("DATABASE_URL is unset; feed ids are minted fresh each run")

    sizes = sorted((len(feed.members) for feed in result), reverse=True)
    realtime = sum(1 for feed in result if set(feed.urls) & set(RT_ROLES))
    log.info(
        "%d feeds from %d rows, %d with realtime; largest groups %s",
        len(result),
        len(sources),
        realtime,
        sizes[:5],
    )
    return result
