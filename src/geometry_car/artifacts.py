"""The published JSON documents, as pure functions.

Everything here returns bytes or plain data and touches no network, so the
shape of what consumers fetch is testable without a bucket.

``sources.json`` keeps the field names of the old ``public/atlas-feeds.json``
(`rowId`, `feedId`, `name`, `operator_name`, `source`, `scheduledUrl`,
`vehiclesUrl`, `tripUpdatesUrl`, `alertsUrl`) as a compatible subset, so a
consumer moves over by changing where it fetches rather than how it reads. The
new fields sit alongside in snake_case. ``rowId`` values are now namespaced by
catalog (`tl:`, `md:`, `gz:`), which is the one deliberate break.

``feeds.json`` is the layer consumers list: one entry per logical feed, which
references its rows by ``rowId``. ``search.json`` is the same feeds cut down to
what a feed picker lists and searches over. ``status.json`` stays per row; a
feed's state is carried on the feed itself.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from geometry_car.catalog import ATLAS_URL_KEYS, Place, Source
from geometry_car.urls import normalize_url

if TYPE_CHECKING:
    from geometry_car.assets.check_history import SourceStatus
    from geometry_car.assets.feed_contents import FeedContent
    from geometry_car.assets.feeds import Feed

ARTIFACT_CONTENT_TYPE = "application/json"
SNAPSHOT_CONTENT_TYPE = "application/gzip"
SNAPSHOT_PREFIX = "snapshots/"
SNAPSHOT_INDEX_KEY = "snapshots/index.json"
TRANSITLAND_FEED_BASE = "https://www.transit.land/feeds/"
MOBILITYDATABASE_FEED_BASE = "https://mobilitydatabase.org/feeds/"

# Carried at the top of sources.json, feeds.json and manifest.json. The Atlas
# is CC-BY 4.0 and asks for a link; the Mobility Database catalog is CC0.
ATTRIBUTION: dict[str, Any] = {
    "catalogs": [
        {
            "name": "Transitland Atlas",
            "url": "https://github.com/transitland/transitland-atlas",
            "license": "CC-BY-4.0",
            "licenseUrl": "https://creativecommons.org/licenses/by/4.0/",
        },
        {
            "name": "Mobility Database",
            "url": "https://mobilitydatabase.org",
            "license": "CC0-1.0",
            "licenseUrl": "https://creativecommons.org/publicdomain/zero/1.0/",
        },
    ],
    "feeds": (
        "Each feed's data belongs to its publisher and is licensed on the"
        " publisher's terms: see a row's license_url and a feed's licenses."
    ),
}


def dumps(payload: object) -> bytes:
    """Compact, key-sorted JSON. Sorted so two equal documents hash equal."""
    return json.dumps(
        payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    ).encode()


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def place_fields(place: Place) -> dict[str, Any]:
    """The place keys a row or feed carries; absent rather than empty."""
    fields: dict[str, Any] = {}
    if place.country_code:
        fields["country_code"] = place.country_code
    if place.country:
        fields["country"] = place.country
    if place.subdivision:
        fields["subdivision"] = place.subdivision
    if place.municipality:
        fields["municipality"] = place.municipality
    if place.placed:
        fields["lat"] = round(place.latitude, 5)
        fields["lon"] = round(place.longitude, 5)
    if place.bbox:
        fields["bbox"] = [round(v, 5) for v in place.bbox]
    return fields


def catalog_url(row: Source) -> str | None:
    """Mirrors globe-of-contents' labels.ts catalogUrl."""
    if row.catalog == "transitland":
        return f"{TRANSITLAND_FEED_BASE}{row.feed_id}"
    if row.catalog == "mobilitydatabase":
        kind = "gtfs_rt" if row.kind == "rt" else "gtfs"
        return f"{MOBILITYDATABASE_FEED_BASE}{kind}/{row.feed_id}"
    return None


def source_row(source: Source, status: SourceStatus | None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "rowId": source.source_id,
        "kind": source.kind,
        "feedId": source.feed_id,
        "name": source.name,
        "operator_name": source.operator_name,
        "source": source.origin,
        "catalog": source.catalog,
    }
    for role, url in source.urls.items():
        row[ATLAS_URL_KEYS[role]] = url

    row |= place_fields(source.place)

    if source.same_endpoint_as:
        row["same_endpoint_as"] = list(source.same_endpoint_as)
    if source.status:
        row["feed_status"] = source.status
    if source.authentication_type:
        row["auth"] = source.authentication_type
    if source.license_url:
        row["license_url"] = source.license_url
    if status is not None:
        row["state"] = status.state
    return row


def sources_document(
    sources: list[Source], statuses: dict[str, SourceStatus], generated_at: datetime
) -> bytes:
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "attribution": ATTRIBUTION,
            "count": len(sources),
            "sources": [source_row(s, statuses.get(s.source_id)) for s in sources],
        }
    )


def feed_since(feed: Feed, statuses: dict[str, SourceStatus]) -> datetime | None:
    """When the feed's state was reached, from its rows' histories.

    Up since its last row came up. Down or partial since the earliest
    still-down row among those filling the roles that make it so: the schedule
    for a down feed that has one, else every down role. Null until the history
    has been written at least once.
    """
    if feed.state == "up":
        sinces = [
            s.since
            for m in feed.members
            if (s := statuses.get(m)) and s.state == "up" and s.since
        ]
        return max(sinces) if sinces else None
    if feed.state not in ("down", "partial"):
        return None
    down = {role for role, state in feed.role_state.items() if state == "down"}
    if feed.state == "down" and "scheduled" in down:
        down = {"scheduled"}
    rows = {member for member, role in feed.member_roles if role in down}
    sinces = [
        s.since
        for m in rows
        if (s := statuses.get(m)) and s.state == "down" and s.since
    ]
    return min(sinces) if sinces else None


def feed_content(
    feed: Feed, contents: dict[str, FeedContent] | None
) -> FeedContent | None:
    """The content report for the feed's best reported schedule URL."""
    for url in feed.urls.get("scheduled", ()):
        if content := (contents or {}).get(normalize_url(url) or url):
            return content
    return None


def content_fields(content: FeedContent) -> dict[str, Any]:
    """A feed's `content` in feeds.json: the download outcome and, when ok, a
    compact subset of the zip's facts. Agencies stay on the feed page."""
    facts = content.facts
    service = facts.get("service") or {}
    info = facts.get("feedInfo") or {}
    counts = facts.get("counts") or {}
    fields = {
        "state": content.outcome,
        "since": content.since.isoformat(),
        "checked": content.checked.isoformat(),
        "detail": content.detail,
        "serviceStart": service.get("start"),
        "serviceEnd": service.get("end"),
        "publisher": info.get("publisher"),
        "version": info.get("version"),
        "routes": counts.get("routes"),
        "stops": counts.get("stops"),
        "trips": counts.get("trips"),
        "routeTypes": facts.get("routeTypes"),
    }
    return {key: value for key, value in fields.items() if value not in (None, "")}


def feed_entry(
    feed: Feed,
    statuses: dict[str, SourceStatus],
    contents: dict[str, FeedContent] | None = None,
    rows: dict[str, Source] | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "feedId": feed.feed_id,
        "name": feed.name,
        "members": list(feed.members),
        "state": feed.state,
        "roleState": dict(feed.role_state),
        # role -> URLs, best first. The first is the one to load; its size and
        # Last-Modified are the ones published for a schedule.
        "urls": {role: list(urls) for role, urls in feed.urls.items()},
    }
    if feed.subtitle:
        entry["subtitle"] = feed.subtitle
    if feed.auth_roles:
        entry["auth"] = list(feed.auth_roles)
    if feed.static_bytes is not None:
        entry["staticBytes"] = feed.static_bytes
    if feed.last_modified is not None:
        entry["lastModified"] = feed.last_modified.isoformat()
    if (since := feed_since(feed, statuses)) is not None:
        entry["since"] = since.isoformat()
    if (content := feed_content(feed, contents)) is not None:
        entry["content"] = content_fields(content)
    members = [rows[m] for m in feed.members if m in (rows or {})]
    if licenses := sorted({row.license_url for row in members if row.license_url}):
        entry["licenses"] = licenses
    if links := sorted({url for row in members if (url := catalog_url(row))}):
        entry["catalogLinks"] = links
    return entry | place_fields(feed.place)


def feeds_document(
    feeds: list[Feed],
    statuses: dict[str, SourceStatus],
    generated_at: datetime,
    contents: dict[str, FeedContent] | None = None,
    sources: list[Source] | None = None,
) -> bytes:
    rows = {source.source_id: source for source in sources or []}
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "attribution": ATTRIBUTION,
            "count": len(feeds),
            "feeds": [feed_entry(feed, statuses, contents, rows) for feed in feeds],
        }
    )


def status_entries(statuses: dict[str, SourceStatus]) -> dict[str, dict[str, Any]]:
    """The small document a UI polls: reachability and nothing else."""
    entries: dict[str, dict[str, Any]] = {}
    for source_id, status in statuses.items():
        entry: dict[str, Any] = {"state": status.state}
        if status.status_code is not None:
            entry["code"] = status.status_code
        if status.error_class:
            entry["error"] = status.error_class
        if status.final_url:
            entry["final_url"] = status.final_url
        if status.latency_ms is not None:
            entry["latency_ms"] = status.latency_ms
        if status.consecutive_failures:
            entry["failures"] = status.consecutive_failures
        if status.since is not None:
            entry["since"] = status.since.isoformat()
        entries[source_id] = entry
    return entries


def status_document(statuses: dict[str, SourceStatus], generated_at: datetime) -> bytes:
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "sources": status_entries(statuses),
        }
    )


def alt_names(
    feed: Feed, rows: list[Source], content: FeedContent | None = None
) -> list[str]:
    """Every other name the feed goes by: its rows', operators' and agencies'."""
    names = {row.name for row in rows} | {row.operator_name for row in rows}
    if content is not None:
        names |= {a.get("name") or "" for a in content.facts.get("agencies") or []}
        names.add((content.facts.get("feedInfo") or {}).get("publisher") or "")
    names -= {"", feed.name}
    return sorted(name for name in names if name.strip())


def search_entry(
    feed: Feed,
    statuses: dict[str, SourceStatus],
    contents: dict[str, FeedContent] | None = None,
    rows: dict[str, Source] | None = None,
) -> dict[str, Any]:
    """One feed in search.json. Keys are short: the file is fetched whole by
    every app's feed picker. See feed_entry for what each field means."""
    members = [rows[m] for m in feed.members if m in (rows or {})]
    place = feed.place
    entry: dict[str, Any] = {
        "i": feed.feed_id,
        "n": feed.name,
        "st": feed.state,
        "rs": dict(feed.role_state),
        "u": {role: list(urls) for role, urls in feed.urls.items()},
    }
    if feed.subtitle:
        entry["s"] = feed.subtitle
    if names := alt_names(feed, members, feed_content(feed, contents)):
        entry["a"] = names
    # Municipality, subdivision, country: what a place search matches and a
    # place line shows.
    if parts := [
        part
        for part in (
            place.municipality,
            place.subdivision,
            place.country or place.country_code,
        )
        if part
    ]:
        entry["p"] = parts
    if place.country_code:
        entry["cc"] = place.country_code
    if place.placed:
        entry["ll"] = [round(place.latitude, 5), round(place.longitude, 5)]
    if feed.auth_roles:
        entry["au"] = list(feed.auth_roles)
    if feed.static_bytes is not None:
        entry["b"] = feed.static_bytes
    if feed.last_modified is not None:
        entry["m"] = feed.last_modified.date().isoformat()
    if (since := feed_since(feed, statuses)) is not None:
        entry["since"] = since.date().isoformat()
    return entry


def search_document(
    feeds: list[Feed],
    statuses: dict[str, SourceStatus],
    generated_at: datetime,
    contents: dict[str, FeedContent] | None = None,
    sources: list[Source] | None = None,
) -> bytes:
    rows = {source.source_id: source for source in sources or []}
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "attribution": ATTRIBUTION,
            "count": len(feeds),
            "feeds": [search_entry(f, statuses, contents, rows) for f in feeds],
        }
    )


def summary_document(
    sources: list[Source],
    statuses: dict[str, SourceStatus],
    feeds: list[Feed],
    generated_at: datetime,
    contents: dict[str, FeedContent] | None = None,
) -> bytes:
    by_catalog = Counter(s.catalog for s in sources)
    by_kind = Counter(s.kind for s in sources)
    by_state = Counter(
        getattr(statuses.get(s.source_id), "state", "unknown") for s in sources
    )
    by_country = Counter(s.place.country_code for s in sources if s.place.country_code)
    placed = sum(1 for s in sources if s.place.placed)
    feeds_placed = sum(1 for feed in feeds if feed.place.placed)
    by_content = Counter(
        content.outcome
        for feed in feeds
        if (content := feed_content(feed, contents)) is not None
    )

    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "total": len(sources),
            "by_catalog": dict(by_catalog),
            "by_kind": dict(by_kind),
            "by_state": dict(by_state),
            "by_country": dict(by_country),
            "placed": placed,
            # Published, not hidden: most of the corpus has no coordinates at
            # all, and a map that silently drops them would be a lie.
            "unplaced": len(sources) - placed,
            "feeds": {
                "total": len(feeds),
                "by_state": dict(Counter(feed.state for feed in feeds)),
                "realtime": sum(1 for feed in feeds if set(feed.urls) - {"scheduled"}),
                "placed": feeds_placed,
                "unplaced": len(feeds) - feeds_placed,
                # Only feeds cape-flier builds a site from have a content state.
                "by_content": dict(by_content),
            },
        }
    )


def snapshot_payload(statuses: dict[str, SourceStatus]) -> bytes:
    """What a snapshot's identity is hashed over.

    Deliberately excludes the timestamp: a day on which nothing changed must
    hash the same as the day before, or the dedupe never fires.
    """
    return dumps(status_entries(statuses))


def snapshot_body(payload: bytes, generated_at: datetime) -> bytes:
    return gzip.compress(
        dumps(
            {"generated_at": generated_at.isoformat(), "sources": json.loads(payload)}
        ),
        mtime=0,
    )


def snapshot_key(day: date) -> str:
    return f"{SNAPSHOT_PREFIX}{day.isoformat()}.json.gz"


def snapshots_to_delete(
    index: list[dict[str, Any]], today: date, *, daily_days: int, weekly_days: int
) -> list[str]:
    """Which snapshot keys retention drops.

    Dailies in full for ``daily_days``, then one per ISO week out to
    ``weekly_days``, then one per calendar month. The kept one in a period is
    always the oldest, so a kept date never moves once chosen.
    """
    daily_cutoff = today - timedelta(days=daily_days)
    weekly_cutoff = today - timedelta(days=weekly_days)

    kept_periods: set[tuple[str, Any]] = set()
    drop: list[str] = []
    for entry in sorted(index, key=lambda e: e["date"]):
        day = date.fromisoformat(entry["date"])
        if day >= daily_cutoff:
            continue
        period = (
            ("week", day.isocalendar()[:2])
            if day >= weekly_cutoff
            else ("month", (day.year, day.month))
        )
        if period in kept_periods:
            drop.append(entry["key"])
        else:
            kept_periods.add(period)
    return drop


def manifest_document(
    artifacts: dict[str, bytes], generated_at: datetime, run_id: str
) -> bytes:
    """What a consumer polls to decide whether to refetch anything."""
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "run_id": run_id,
            "attribution": ATTRIBUTION,
            "artifacts": {
                name: {"sha256": sha256(body), "bytes": len(body)}
                for name, body in sorted(artifacts.items())
            },
        }
    )


def utcnow() -> datetime:
    return datetime.now(UTC)
