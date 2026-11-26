"""Logical feeds: one per transit system, across catalogs and roles.

Catalog rows stay the raw layer (see ``sources``). A logical feed bundles the
rows that describe the same system, so a consumer lists it once instead of once
per catalog and once per realtime entity type. Rows join a feed through a
union-find over six kinds of evidence, and nothing else:

- a shared URL (``urls.link_key``), of any kind, in any catalog;
- byte-identical schedules: scheduled URLs whose stored zip has the same
  sha256 in ``feed_contents`` (the same day's download, from ``feed_facts``);
- identical cleaned data: readable schedules with trips and the same ``cleanedHash``
  (every GTFS file but feed_info, ignoring row and column order and packaging);
- a realtime row's ``feed_references`` to a static row of its catalog: a
  Mobility Database reference, or a Transitland operator's only schedule;
- realtime URLs that differ only in a final entity-type segment
  (``x/vehicles``, ``x/trips``, ``x/alerts``), see ``urls.rt_sibling_key``;
- the static and realtime rows one catalog feed was split into.

No fuzzy name matching and, for now, no override file, so a wrong link cannot be
undone except by changing the rules. The groups can false-merge: one regional
schedule referenced by several agencies' realtime feeds makes them one feed,
whether by MDB reference or by being each Transitland operator's only schedule.

Feed ids go in shareable URLs, so they are persisted and sticky: each group
takes the id of the existing feed it shares the most members with, and a new id
is minted only for a group that shares none. A feed that loses its group keeps
its row, and its remaining members, for the retention window, so a group that
comes back gets its old id. A feed whose members joined another feed records
that feed as ``moved_to``, so consumers can follow it.

Feeds that are not joined can still overlap: a feed whose agencies include
another's (an aggregate such as DELFI and a city feed in it), or two feeds
sharing an agency, with intersecting station bboxes. When both feeds have
agency fingerprints, a shared agency is kept only when the edge containment of
its smaller side reaches ``EDGE_THRESHOLD``. Overlaps are published, never
joined.
"""

import hashlib
import logging
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from itertools import combinations
from urllib.parse import urlsplit

from dagster import AssetExecutionContext, MetadataValue, asset
from gtfs_zone_db_models.object_store import ObjectNotFound, ObjectStore
from gtfs_zone_feed_fetch.fingerprint import (
    AgencyFingerprint,
    FeedFingerprint,
    containment,
    from_parquet,
)
from gtfs_zone_feed_fetch.store import fingerprint_key
from sqlalchemy import delete, insert, select, update

from gtfs_zone_feed_catalog.assets.check_history import DOWN, UNKNOWN, UP
from gtfs_zone_feed_catalog.assets.endpoint_checks import CheckResult, result_key
from gtfs_zone_feed_catalog.assets.feed_contents import FeedContent
from gtfs_zone_feed_catalog.assets.transitland_atlas import humanize_feed_id
from gtfs_zone_feed_catalog.catalog import RT_ROLES, Place, Source
from gtfs_zone_feed_catalog.database import get_session_factory
from gtfs_zone_feed_catalog.history.models import FeedMember, FeedRecord, SourceRecord
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.urls import link_key, normalize_url, rt_sibling_key
from gtfs_zone_feed_catalog.zips import ZipStoreSettings

log = logging.getLogger(__name__)

# A feed whose schedule answers but some realtime role does not.
PARTIAL = "partial"

# Source id prefix of the static rows each catalog's feed_references name.
REFERENCE_PREFIXES = {"mobilitydatabase": "md", "transitland": "tl"}

# Overlap kinds, from the point of view of the feed that lists the overlap.
CONTAINS = "contains"
CONTAINED_IN = "containedIn"
SHARES = "shares"

# Least edge containment of a shared agency's smaller side that confirms it
# (decided over every overlaps pair on 2026-11-25).
EDGE_THRESHOLD = 0.01

type Bbox = tuple[float, float, float, float]


@dataclass(frozen=True, slots=True)
class Overlap:
    feed_id: str
    kind: str
    # Display names of the agencies both feeds have, sorted.
    agencies: tuple[str, ...]
    # Per agency, its edge containment; None when either feed has no fingerprint.
    scores: tuple[float | None, ...] = ()


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
    # From role_state: down when the schedule is, partial when the schedule is
    # up and a realtime role is down, up when nothing checked is down.
    state: str
    place: Place
    # From the best scheduled URL: up first, then the most recently modified.
    static_bytes: int | None = None
    last_modified: datetime | None = None
    # Roles whose every URL needs an API key we do not hold, so none is checked.
    auth_roles: tuple[str, ...] = ()
    # What tells this feed apart from others of the same name: a Mobility
    # Database feed_name such as "Rail" or "Bus".
    subtitle: str = ""
    # Agency display names over every scheduled URL's facts, one per
    # normalized name, sorted by it.
    agencies: tuple[str, ...] = ()
    # Union of those URLs' station bboxes (min lon, min lat, max lon, max lat).
    bbox: Bbox | None = None
    # sha256 of every scheduled URL's readable zip, sorted.
    zips: tuple[str, ...] = ()
    overlaps: tuple[Overlap, ...] = ()


def zip_sha256(url: str, contents: dict[str, FeedContent] | None) -> str:
    """The sha256 of `url`'s last downloaded zip, readable or not, else "".
    Identical non-zip bodies (one host's error page) never link."""
    content = (contents or {}).get(normalize_url(url) or url)
    return content.sha256 if content and content.outcome != "not_zip" else ""


def cleaned_hash(url: str, contents: dict[str, FeedContent] | None) -> str:
    """The cleaned-data hash of `url`'s last readable zip, else "". A schedule
    without trips has none: placeholder feeds share their few rows."""
    content = (contents or {}).get(normalize_url(url) or url)
    if not content or content.outcome != "ok":
        return ""
    if not (content.facts.get("counts") or {}).get("trips"):
        return ""
    return content.facts.get("cleanedHash") or ""


def normalize_agency(name: str) -> str:
    """`name` without accents, case or punctuation, spaces collapsed."""
    decomposed = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in decomposed if not unicodedata.combining(c)).casefold()
    return " ".join("".join(c if c.isalnum() else " " for c in text).split())


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


def links(
    rows: list[Source],
    contents: dict[str, FeedContent] | None = None,
    clean: bool = True,
) -> dict[tuple[str, ...], list[str]]:
    """Source ids per piece of join evidence, keyed by (kind, value...). Kinds:
    feed, url, sha, clean, rt, ref. `clean` False leaves out cleaned hashes."""
    by_id = {row.source_id: row for row in rows}
    found: dict[tuple[str, ...], list[str]] = defaultdict(list)

    for row in rows:
        found[("feed", row.catalog, row.feed_id)].append(row.source_id)
        for role, url in row.urls.items():
            if key := link_key(url):
                found[("url", key)].append(row.source_id)
            if role == "scheduled" and (sha := zip_sha256(url, contents)):
                found[("sha", sha)].append(row.source_id)
            if role == "scheduled" and clean and (h := cleaned_hash(url, contents)):
                found[("clean", h)].append(row.source_id)
            if role in RT_ROLES and (sibling := rt_sibling_key(url)):
                found[("rt", sibling)].append(row.source_id)
        prefix = REFERENCE_PREFIXES.get(row.catalog)
        for ref in row.feed_references if prefix else ():
            if (target := f"{prefix}:{ref}:static") in by_id:
                found[("ref", row.source_id, ref)] += [row.source_id, target]
    return found


def group(
    rows: list[Source],
    contents: dict[str, FeedContent] | None = None,
    clean: bool = True,
) -> list[list[Source]]:
    """Rows describing the same system, each group sorted by source id.
    `clean` False leaves out cleaned-hash links, to count what they join."""
    by_id = {row.source_id: row for row in rows}
    union = _UnionFind(list(by_id))
    for members in links(rows, contents, clean).values():
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


def feed_state(role_state: dict[str, str]) -> str:
    """One state for the feed, led by its schedule.

    A realtime role that is down makes an answering schedule partial, not down.
    Without a checked schedule the realtime roles decide on their own.
    """
    scheduled = role_state.get("scheduled", UNKNOWN)
    realtime = [s for role, s in role_state.items() if role != "scheduled"]
    if scheduled == DOWN:
        return DOWN
    if scheduled == UP:
        return PARTIAL if DOWN in realtime else UP
    checked = [s for s in realtime if s != UNKNOWN]
    if not checked:
        return UNKNOWN
    if all(s == UP for s in checked):
        return UP
    return PARTIAL if UP in checked else DOWN


def _provider(row: Source) -> str:
    """A Mobility Database row's provider, without its "(feed_name)"."""
    return row.operator_name if row.catalog == "mobilitydatabase" else ""


def _feed_name(row: Source) -> str:
    """The "(feed_name)" part of a Mobility Database row name, if any."""
    prefix = f"{row.operator_name} ("
    if row.name.startswith(prefix) and row.name.endswith(")"):
        return row.name[len(prefix) : -1]
    return ""


def content_agency(content: FeedContent | None) -> str:
    """The name the schedule gives itself: its one agency, else its publisher."""
    if content is None:
        return ""
    agencies = [a["name"] for a in content.facts.get("agencies") or [] if a.get("name")]
    if len(agencies) == 1:
        return agencies[0]
    return (content.facts.get("feedInfo") or {}).get("publisher") or ""


def feed_name(rows: list[Source], content: FeedContent | None = None) -> str:
    """The first of: a Transitland operator, a Mobility Database provider, the
    schedule's own agency, an NTD agency, then any row's name. Static rows first
    throughout."""
    ordered = sorted(rows, key=lambda row: (row.kind != "static", row.source_id))
    candidates = [
        *(r.name for r in ordered if r.catalog == "transitland" and r.operator_name),
        *(_provider(r) for r in ordered),
        content_agency(content),
        *(r.operator_name for r in ordered if r.catalog == "ntd"),
        *(r.name for r in ordered if r.catalog == "mobilitydatabase"),
        *(r.name for r in ordered),
    ]
    return next((name for name in candidates if name.strip()), "")


def feed_subtitle(rows: list[Source], name: str) -> str:
    """A Mobility Database feed_name, from a provider the feed is named for
    first, then from any provider."""
    ordered = sorted(rows, key=lambda row: (row.kind != "static", row.source_id))
    ordered.sort(key=lambda row: _provider(row) != name)
    return next((sub for r in ordered if (sub := _feed_name(r))), "")


def _url_stem(url: str) -> str:
    """The last path segment of `url` without its extension."""
    try:
        segment = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    except ValueError:
        return ""
    return segment.rsplit(".", 1)[0] if "." in segment else segment


def fallback_subtitle(
    feed: Feed, rows: list[Source], content: FeedContent | None
) -> str:
    """A subtitle for a feed whose name another feed shares: the schedule's own
    agency, a Transitland feed id, then the schedule URL's file name. Any that
    only repeats the name is skipped."""
    ordered = sorted(rows, key=lambda row: (row.kind != "static", row.source_id))
    candidates = [
        content_agency(content),
        *(humanize_feed_id(r.feed_id) for r in ordered if r.catalog == "transitland"),
        *(_url_stem(url) for url in feed.urls.get("scheduled", ())),
    ]
    taken = feed.name.strip().casefold()
    return next(
        (c.strip() for c in candidates if c.strip() and c.strip().casefold() != taken),
        "",
    )


def _content(feed: Feed, contents: dict[str, FeedContent] | None) -> FeedContent | None:
    """The content of the feed's best scheduled URL that has one."""
    return next(
        (
            found
            for url in feed.urls.get("scheduled", ())
            if (found := (contents or {}).get(normalize_url(url) or url))
        ),
        None,
    )


def tell_apart(
    feeds: list[Feed],
    groups: list[list[Source]],
    contents: dict[str, FeedContent] | None = None,
) -> list[Feed]:
    """`feeds` with a fallback subtitle on each one without a subtitle whose
    name another feed also has with none."""
    shared = Counter((feed.name, feed.subtitle) for feed in feeds)
    return [
        replace(feed, subtitle=fallback_subtitle(feed, rows, _content(feed, contents)))
        if not feed.subtitle and shared[(feed.name, "")] > 1
        else feed
        for feed, rows in zip(feeds, groups, strict=True)
    ]


def _contents(
    feed_urls: list[str], contents: dict[str, FeedContent] | None
) -> list[FeedContent]:
    """The contents of `feed_urls` that have one, in order."""
    return [
        found
        for url in feed_urls
        if (found := (contents or {}).get(normalize_url(url) or url))
    ]


def feed_agencies(found: list[FeedContent]) -> tuple[str, ...]:
    """Agency names over `found`, one per normalized name (the first spelling
    seen), sorted by it."""
    names: dict[str, str] = {}
    for content in found:
        for agency in content.facts.get("agencies") or []:
            name = (agency.get("name") or "").strip()
            if name and (key := normalize_agency(name)):
                names.setdefault(key, name)
    return tuple(names[key] for key in sorted(names))


def feed_bbox(found: list[FeedContent]) -> Bbox | None:
    """The union of `found`'s station bboxes."""
    boxes = [
        box
        for content in found
        if len(box := (content.facts.get("location") or {}).get("bbox") or []) == 4
    ]
    if not boxes:
        return None
    return (
        min(b[0] for b in boxes),
        min(b[1] for b in boxes),
        max(b[2] for b in boxes),
        max(b[3] for b in boxes),
    )


def intersect(a: Bbox, b: Bbox) -> bool:
    return a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3]


def no_fingerprint(_: str) -> None:
    return None


def overlap_pairs(feeds: list[Feed]) -> dict[tuple[str, str], set[str]]:
    """{(feed id, feed id): shared normalized agency names} for feeds sharing
    an agency name, with intersecting bboxes. A feed without agencies or a
    bbox pairs with nothing."""
    keys = {
        feed.feed_id: {normalize_agency(name) for name in feed.agencies}
        for feed in feeds
    }
    boxes = {feed.feed_id: feed.bbox for feed in feeds}
    by_agency: dict[str, list[str]] = defaultdict(list)
    for feed in feeds:
        if feed.bbox is not None:
            for key in keys[feed.feed_id]:
                by_agency[key].append(feed.feed_id)
    pairs = {pair for ids in by_agency.values() for pair in combinations(ids, 2)}
    found = {}
    for a, b in sorted(pairs):
        box_a, box_b = boxes[a], boxes[b]
        if box_a is not None and box_b is not None and intersect(box_a, box_b):
            found[(a, b)] = keys[a] & keys[b]
    return found


def agency_edges(
    feed: Feed, fingerprint: Callable[[str], FeedFingerprint | None]
) -> dict[str, frozenset[int]] | None:
    """{normalized agency name: the union of its edges} over `feed`'s zips,
    None unless every zip has a fingerprint."""
    prints = [fingerprint(sha) for sha in feed.zips]
    if not prints or any(p is None for p in prints):
        return None
    edges: dict[str, set[int]] = defaultdict(set)
    for printed in prints:
        for agency in printed.agencies if printed else ():
            if key := normalize_agency(agency.name):
                edges[key] |= agency.edges
    return {key: frozenset(found) for key, found in edges.items()}


def edge_score(
    key: str,
    a: dict[str, frozenset[int]] | None,
    b: dict[str, frozenset[int]] | None,
) -> float | None:
    """Edge containment of the agency's side with fewer edges in the other;
    None when either side has no fingerprint or the agency is not in it."""
    if a is None or b is None or key not in a or key not in b:
        return None
    small, big = sorted((a[key], b[key]), key=len)
    return containment(
        AgencyFingerprint("", key, frozenset(), small),
        AgencyFingerprint("", key, frozenset(), big),
    )


def overlaps(
    feeds: list[Feed],
    fingerprint: Callable[[str], FeedFingerprint | None] = no_fingerprint,
) -> dict[str, tuple[Overlap, ...]]:
    """{feed id: its overlaps}: feeds sharing a normalized agency name, with
    intersecting bboxes (`overlap_pairs`). A shared agency scored below
    `EDGE_THRESHOLD` is dropped; an unscored one is kept. One feed whose
    agencies all remain shared, and are a strict subset of the other's, is
    containedIn it, and the other contains it; any other pair shares. A pair
    with no agency left does not overlap."""
    by_id = {feed.feed_id: feed for feed in feeds}
    names = {
        feed.feed_id: {normalize_agency(name): name for name in feed.agencies}
        for feed in feeds
    }
    pairs = overlap_pairs(feeds)
    paired = sorted({feed_id for pair in pairs for feed_id in pair})
    edges = {feed_id: agency_edges(by_id[feed_id], fingerprint) for feed_id in paired}

    found: dict[str, list[Overlap]] = defaultdict(list)
    for (a, b), shared in pairs.items():
        scores = {key: edge_score(key, edges[a], edges[b]) for key in shared}
        kept = {
            key
            for key, score in scores.items()
            if score is None or score >= EDGE_THRESHOLD
        }
        if not kept:
            continue
        mine, theirs = names[a].keys(), names[b].keys()
        if kept == mine and mine < theirs:
            kind_a, kind_b = CONTAINED_IN, CONTAINS
        elif kept == theirs and theirs < mine:
            kind_a, kind_b = CONTAINS, CONTAINED_IN
        else:
            kind_a = kind_b = SHARES
        found[a].append(_overlap(b, kind_a, kept, names[a], scores))
        found[b].append(_overlap(a, kind_b, kept, names[b], scores))
    return {
        feed_id: tuple(sorted(items, key=lambda o: o.feed_id))
        for feed_id, items in found.items()
    }


def _overlap(
    feed_id: str,
    kind: str,
    kept: Iterable[str],
    names: dict[str, str],
    scores: dict[str, float | None],
) -> Overlap:
    """An overlap with `kept` as display names and their scores, by name."""
    ordered = sorted((names[key], scores[key]) for key in kept)
    return Overlap(
        feed_id,
        kind,
        tuple(name for name, _ in ordered),
        tuple(score for _, score in ordered),
    )


def build_feed(
    feed_id: str,
    members: list[Source],
    results: dict[str, CheckResult],
    contents: dict[str, FeedContent] | None = None,
) -> Feed:
    ordered = sorted(members, key=lambda row: (row.kind != "static", row.source_id))

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

    found = _contents(ranked.get("scheduled", []), contents)
    content = found[0] if found else None
    agencies = feed_agencies(found)
    name = feed_name(members, content)
    role_state = {role: _any_up(role_states) for role, role_states in states.items()}

    return Feed(
        feed_id=feed_id,
        name=name,
        members=tuple(sorted(row.source_id for row in members)),
        member_roles=tuple(sorted(member_roles)),
        urls={role: tuple(role_urls) for role, role_urls in ranked.items()},
        role_state=role_state,
        state=feed_state(role_state),
        place=place,
        static_bytes=best.content_length if best else None,
        last_modified=best.last_modified if best else None,
        auth_roles=auth_roles,
        subtitle=feed_subtitle(members, name),
        agencies=agencies,
        bbox=feed_bbox(found),
        zips=tuple(sorted({c.sha256 for c in found if c.outcome == "ok" and c.sha256})),
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


def moved_to(members: dict[str, set[str]], feeds: list[Feed]) -> dict[str, str]:
    """{feed id: the feed holding most of its members} for each id in
    `members` that is not one of `feeds`. Ties go to the lower id."""
    owner = {source_id: feed.feed_id for feed in feeds for source_id in feed.members}
    current = {feed.feed_id for feed in feeds}
    moved = {}
    for feed_id, sources in members.items():
        if feed_id in current:
            continue
        counts = Counter(owner[s] for s in sources if s in owner)
        if counts:
            moved[feed_id] = min(counts, key=lambda f: (-counts[f], f))
    return moved


def persist(feeds: list[Feed], now: datetime | None = None) -> None:
    """Upsert today's feeds and their members; mark the rest absent.

    A source belongs to one feed at a time, so a present source's old
    memberships are cleared before today's are written. An absent feed keeps
    only members no present feed has, which is what lets it be matched again
    if they return. A feed going absent records where its members went.
    """
    now = now or datetime.now(UTC)
    with get_session_factory()() as session, session.begin():
        records = {
            record.feed_id: record for record in session.scalars(select(FeedRecord))
        }
        current = {feed.feed_id for feed in feeds}
        leaving: dict[str, set[str]] = defaultdict(set)
        for feed_id, source_id in session.execute(
            select(FeedMember.feed_id, FeedMember.source_id).where(
                FeedMember.feed_id.in_(
                    [f for f, r in records.items() if r.present and f not in current]
                )
            )
        ):
            leaving[feed_id].add(source_id)
        for feed_id, target in moved_to(leaving, feeds).items():
            records[feed_id].moved_to = target
        for feed in feeds:
            record = records.get(feed.feed_id)
            if record is None:
                record = FeedRecord(feed_id=feed.feed_id, first_seen=now)
                session.add(record)
            record.name = feed.name
            record.last_seen = now
            record.present = True
            record.moved_to = None
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


def load_moved() -> dict[str, str]:
    """{absent feed id: present feed id}, following moves to the last one."""
    with get_session_factory()() as session:
        rows = session.execute(
            select(FeedRecord.feed_id, FeedRecord.moved_to, FeedRecord.present)
        ).all()
    present = {feed_id for feed_id, _, is_present in rows if is_present}
    step = {feed_id: target for feed_id, target, _ in rows if target}
    moved = {}
    for feed_id in step:
        target, seen = step[feed_id], {feed_id}
        while target not in present and target in step and target not in seen:
            seen.add(target)
            target = step[target]
        if target in present:
            moved[feed_id] = target
    return moved


def build_feeds(
    rows: list[Source],
    results: dict[str, CheckResult],
    existing: dict[str, set[str]],
    contents: dict[str, FeedContent] | None = None,
    fingerprint: Callable[[str], FeedFingerprint | None] = no_fingerprint,
) -> list[Feed]:
    groups = group(rows, contents)
    ids = assign_ids([[row.source_id for row in g] for g in groups], existing)
    built = [
        build_feed(feed_id, g, results, contents)
        for feed_id, g in zip(ids, groups, strict=True)
    ]
    told = tell_apart(built, groups, contents)
    found = overlaps(told, fingerprint)
    told = [replace(feed, overlaps=found.get(feed.feed_id, ())) for feed in told]
    return sorted(told, key=lambda feed: feed.feed_id)


def fingerprint_reader(store: ObjectStore) -> Callable[[str], FeedFingerprint | None]:
    """The stored fingerprint of a zip by sha256; None when there is none or
    another FINGERPRINT_VERSION wrote it."""

    def read(sha: str) -> FeedFingerprint | None:
        try:
            return from_parquet(store.get(fingerprint_key(sha)))
        except ObjectNotFound:
            return None
        except ValueError as exc:
            log.warning("fingerprint of %s unreadable: %s", sha, exc)
            return None

    return read


def load_names() -> dict[str, str]:
    """{feed id: its stored name} for present feeds."""
    with get_session_factory()() as session:
        return dict(
            session.execute(
                select(FeedRecord.feed_id, FeedRecord.name).where(FeedRecord.present)
            ).all()
        )


def _renames_table(renamed: list[tuple[str, str, str]]) -> str:
    lines = ["| feed | before | after |", "| --- | --- | --- |"]
    lines += [
        f"| {feed_id} | {before.replace('|', '/')} | {after.replace('|', '/')} |"
        for feed_id, before, after in renamed
    ]
    return "\n".join(lines)


@asset(
    description="Logical feeds: catalog rows grouped per transit system, sticky ids",
    # Ordering only: members reference source rows check_history writes.
    deps=["check_history"],
)
def feeds(
    context: AssetExecutionContext,
    sources: list[Source],
    endpoint_checks: dict[str, CheckResult],
    feed_contents: dict[str, FeedContent],
) -> list[Feed]:
    existing = load_existing() if settings.database_url else {}
    before = load_names() if settings.database_url else {}
    zip_settings = ZipStoreSettings()
    if zip_settings.s3_endpoint:
        fingerprint = fingerprint_reader(ObjectStore(zip_settings))
    else:
        log.warning("ZIPS_S3_ENDPOINT is unset; overlaps by agency name only")
        fingerprint = no_fingerprint
    result = build_feeds(sources, endpoint_checks, existing, feed_contents, fingerprint)
    if settings.database_url:
        persist(result)
    else:
        log.warning("DATABASE_URL is unset; feed ids are minted fresh each run")

    clean_joins = len(group(sources, feed_contents, clean=False)) - len(result)
    kinds = Counter(o.kind for feed in result for o in feed.overlaps)
    scored = sum(
        1
        for feed in result
        for o in feed.overlaps
        if any(x is not None for x in o.scores)
    )
    renamed = sorted(
        (feed.feed_id, before[feed.feed_id], feed.name)
        for feed in result
        if feed.feed_id in before and before[feed.feed_id] != feed.name
    )
    log.info(
        "%d groups joined by cleaned hash only; overlaps %s; %d renamed",
        clean_joins,
        dict(sorted(kinds.items())),
        len(renamed),
    )
    context.add_output_metadata(
        {
            "feeds": len(result),
            "clean_joins": clean_joins,
            "multi_agency": sum(1 for feed in result if len(feed.agencies) > 1),
            "with_overlaps": sum(1 for feed in result if feed.overlaps),
            **{f"overlaps_{kind}": count for kind, count in sorted(kinds.items())},
            "overlaps_scored": scored,
            "renamed": len(renamed),
            "renames": MetadataValue.md(_renames_table(renamed)),
        }
    )

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
