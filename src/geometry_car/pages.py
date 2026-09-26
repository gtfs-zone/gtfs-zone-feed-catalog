"""Per-feed HTML fragments and the sitemap, as pure functions.

list.gtfs.zone serves ``/feed/<feedId>/<slug>`` by including two fragments into
its app shell with nginx SSI: ``head.html`` (title, description, canonical, og
tags, schema.org Dataset JSON-LD) and ``body.html`` (what a crawler reads before
the app replaces it). Nothing here varies by run date, so a page's hash changes
only when the feed does, and ``lastmod`` in the sitemap is honest.

Every catalog value is untrusted text and is escaped on the way in.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timedelta
from html import escape
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

from geometry_car.artifacts import feed_since
from geometry_car.catalog import RT_ROLES, STATIC_ROLES

if TYPE_CHECKING:
    from geometry_car.assets.check_history import SourceStatus
    from geometry_car.assets.feeds import Feed
    from geometry_car.catalog import Source

SITE_BASE = "https://list.gtfs.zone"
EDITOR_BASE = "https://edit.gtfs.zone"
VIEWER_BASE = "https://viz.rt.gtfs.zone"
TRANSITLAND_FEED_BASE = "https://www.transit.land/feeds/"
MOBILITYDATABASE_FEED_BASE = "https://mobilitydatabase.org/feeds/"

PAGES_PREFIX = "pages/feed/"
PAGES_INDEX_KEY = "pages/index.json"
SITEMAP_KEY = "sitemap.xml"
PAGE_CONTENT_TYPE = "text/html; charset=utf-8"
SITEMAP_CONTENT_TYPE = "application/xml"

# A feed down this long is kept reachable but out of the index.
NOINDEX_DOWN_AFTER = timedelta(days=90)

ROLE_LABELS = {
    "scheduled": "GTFS Schedule",
    "vehicles": "Vehicle positions",
    "trip_updates": "Trip updates",
    "alerts": "Service alerts",
}
ROLE_FORMATS = {
    "scheduled": "application/zip",
    "vehicles": "application/x-protobuf",
    "trip_updates": "application/x-protobuf",
    "alerts": "application/x-protobuf",
}
STATE_LABELS = {"up": "Up", "down": "Down", "unknown": "Inaccessible"}
CATALOG_LABELS = {
    "transitland": "Transitland",
    "mobilitydatabase": "Mobility Database",
    "curated": "gtfs.zone examples",
}


def slugify(name: str) -> str:
    """ASCII, lowercase, hyphenated; decorative only, the id is what resolves."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    return slug[:60].rstrip("-") or "feed"


def feed_url(feed: Feed) -> str:
    return f"{SITE_BASE}/feed/{feed.feed_id}/{slugify(feed.name)}"


def _absolute(url: str) -> bool:
    return url.startswith(("http://", "https://"))


def _has_realtime(feed: Feed) -> bool:
    return any(role != "scheduled" for role in feed.urls)


def _place_line(feed: Feed) -> str:
    place = feed.place
    parts = [place.municipality, place.subdivision, place.country]
    return ", ".join(part for part in parts if part)


def _kinds(feed: Feed) -> str:
    if "scheduled" in feed.urls and _has_realtime(feed):
        return "GTFS and GTFS Realtime"
    return "GTFS Realtime" if _has_realtime(feed) else "GTFS"


def title(feed: Feed) -> str:
    return f"{feed.name} {_kinds(feed)} feed - list.gtfs.zone"


def _roles(feed: Feed) -> list[tuple[str, tuple[str, ...]]]:
    """The feed's URLs, schedule first, then realtime in a fixed order."""
    order = {role: i for i, role in enumerate(STATIC_ROLES + RT_ROLES)}
    return sorted(feed.urls.items(), key=lambda item: order.get(item[0], len(order)))


def _catalogs(rows: list[Source]) -> str:
    names = [
        label
        for catalog, label in CATALOG_LABELS.items()
        if catalog != "curated" and any(row.catalog == catalog for row in rows)
    ]
    return " and ".join(names) or "the gtfs.zone examples"


def description(feed: Feed, rows: list[Source], since: datetime | None) -> str:
    where = _place_line(feed)
    subject = f"{feed.name} ({where})" if where else feed.name
    state = STATE_LABELS.get(feed.state, feed.state)
    checked = f"{state} since {since.date().isoformat()}" if since else state
    return (
        f"{_kinds(feed)} feed URLs for {subject}, from {_catalogs(rows)}. "
        f"Checked daily for whether it answers: {checked}."
    )


def editor_url(feed: Feed) -> str | None:
    """Mirrors globe-of-contents' app-links.ts."""
    scheduled = next(iter(feed.urls.get("scheduled", ())), None)
    if not scheduled or not _absolute(scheduled):
        return None
    return f"{EDITOR_BASE}/#load={quote(scheduled, safe='')}"


def viewer_url(feed: Feed) -> str | None:
    """Mirrors globe-of-contents' app-links.ts."""
    scheduled = next(iter(feed.urls.get("scheduled", ())), None)
    if not scheduled or not _has_realtime(feed):
        return None
    params = {"scheduled": scheduled}
    for role, key in (
        ("vehicles", "rt_vp"),
        ("trip_updates", "rt_tu"),
        ("alerts", "rt_al"),
    ):
        if urls := feed.urls.get(role):
            params[key] = urls[0]
    return f"{VIEWER_BASE}/#{urlencode(params)}"


def catalog_url(row: Source) -> str | None:
    """Mirrors globe-of-contents' labels.ts catalogUrl."""
    if row.catalog == "transitland":
        return f"{TRANSITLAND_FEED_BASE}{row.feed_id}"
    if row.catalog == "mobilitydatabase":
        kind = "gtfs_rt" if row.kind == "rt" else "gtfs"
        return f"{MOBILITYDATABASE_FEED_BASE}{kind}/{row.feed_id}"
    return None


def indexable(feed: Feed, since: datetime | None, now: datetime) -> bool:
    """Named, and not long dead: thin or dead pages stay out of the index."""
    if not feed.name.strip():
        return False
    return not (feed.state == "down" and since and now - since > NOINDEX_DOWN_AFTER)


def dataset(feed: Feed, rows: list[Source]) -> dict[str, Any]:
    """schema.org Dataset, for Google Dataset Search."""
    url = feed_url(feed)
    doc: dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "@id": url,
        "url": url,
        "name": f"{feed.name} {_kinds(feed)}",
        "description": description(feed, rows, None),
        "identifier": feed.feed_id,
        "isAccessibleForFree": True,
        "keywords": ["GTFS", "public transit", "transit schedule"]
        + (["GTFS Realtime"] if _has_realtime(feed) else []),
        "includedInDataCatalog": {
            "@type": "DataCatalog",
            "name": "list.gtfs.zone",
            "url": f"{SITE_BASE}/",
        },
        "distribution": [
            {
                "@type": "DataDownload",
                "name": ROLE_LABELS.get(role, role),
                "encodingFormat": ROLE_FORMATS.get(role, "application/octet-stream"),
                "contentUrl": urls[0],
            }
            for role, urls in _roles(feed)
            if urls and _absolute(urls[0])
        ],
    }
    aliases = sorted({row.name for row in rows if row.name and row.name != feed.name})
    if aliases:
        doc["alternateName"] = aliases
    operators = sorted({row.operator_name for row in rows if row.operator_name})
    if operators:
        doc["creator"] = [{"@type": "Organization", "name": op} for op in operators]
    licenses = sorted({row.license_url for row in rows if row.license_url})
    if licenses:
        doc["license"] = licenses[0]
    if feed.last_modified:
        doc["dateModified"] = feed.last_modified.isoformat()
    where = _place_line(feed)
    coverage: dict[str, Any] = {"@type": "Place"}
    if where:
        coverage["name"] = where
    if feed.place.bbox:
        min_lat, min_lon, max_lat, max_lon = feed.place.bbox
        coverage["geo"] = {
            "@type": "GeoShape",
            "box": f"{min_lat:.5f} {min_lon:.5f} {max_lat:.5f} {max_lon:.5f}",
        }
    if len(coverage) > 1:
        doc["spatialCoverage"] = coverage
    return doc


def _json_ld(doc: dict[str, Any]) -> str:
    # `<` escaped so a name holding `</script>` cannot close the element.
    body = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    return body.replace("<", "\\u003c")


def head_fragment(
    feed: Feed, rows: list[Source], since: datetime | None, now: datetime
) -> str:
    url = escape(feed_url(feed))
    page_title = escape(title(feed))
    desc = escape(description(feed, rows, since))
    lines = [
        f"<title>{page_title}</title>",
        f'<meta name="description" content="{desc}" />',
        f'<link rel="canonical" href="{url}" />',
        '<meta property="og:type" content="website" />',
        '<meta property="og:site_name" content="gtfs.zone" />',
        f'<meta property="og:url" content="{url}" />',
        f'<meta property="og:title" content="{page_title}" />',
        f'<meta property="og:description" content="{desc}" />',
        '<meta property="og:image" content="https://list.gtfs.zone/logo.svg" />',
        '<meta name="twitter:card" content="summary" />',
    ]
    if not indexable(feed, since, now):
        lines.append('<meta name="robots" content="noindex" />')
    lines.append(
        f'<script type="application/ld+json">{_json_ld(dataset(feed, rows))}</script>'
    )
    return "\n".join(lines) + "\n"


def _link(href: str, text: str) -> str:
    return f'<a href="{escape(href)}" rel="noopener">{escape(text)}</a>'


def body_fragment(feed: Feed, rows: list[Source], since: datetime | None) -> str:
    state = STATE_LABELS.get(feed.state, feed.state)
    status = f"{state} since {since.date().isoformat()}" if since else state
    out = [f"<h1>{escape(feed.name)}</h1>"]
    if where := _place_line(feed):
        out.append(f"<p>{escape(where)}</p>")
    out.append(f"<p>{escape(_kinds(feed))} feed. Reachability: {escape(status)}.</p>")

    out.append("<h2>Feed URLs</h2>")
    out.append("<dl>")
    for role, urls in _roles(feed):
        label = ROLE_LABELS.get(role, role)
        role_state = STATE_LABELS.get(feed.role_state.get(role, ""), "")
        suffix = " (needs an API key)" if role in feed.auth_roles else ""
        out.append(f"<dt>{escape(label)}{escape(suffix)}</dt>")
        for url in urls:
            text = f"{url} ({role_state})" if role_state else url
            out.append(
                f"<dd>{_link(url, text) if _absolute(url) else escape(text)}</dd>"
            )
    out.append("</dl>")

    facts = []
    if feed.static_bytes is not None:
        facts.append(f"Schedule size: {feed.static_bytes:,} bytes")
    if feed.last_modified:
        facts.append(f"Schedule last modified: {feed.last_modified.date().isoformat()}")
    if facts:
        out.append("<ul>" + "".join(f"<li>{escape(f)}</li>" for f in facts) + "</ul>")

    apps = []
    if editor := editor_url(feed):
        apps.append(_link(editor, "Open the schedule in edit.gtfs.zone"))
    if viewer := viewer_url(feed):
        apps.append(_link(viewer, "Watch it live in viz.rt.gtfs.zone"))
    if apps:
        out.append("<ul>" + "".join(f"<li>{a}</li>" for a in apps) + "</ul>")

    out.append("<h2>Catalog entries</h2>")
    out.append("<ul>")
    # Curated static and rt rows share a name and id; list them once.
    seen: set[str] = set()
    for row in rows:
        catalog = CATALOG_LABELS.get(row.catalog, row.catalog)
        text = f"{catalog}: {row.name} ({row.feed_id})"
        href = catalog_url(row)
        item = f"<li>{_link(href, text) if href else escape(text)}</li>"
        if item not in seen:
            seen.add(item)
            out.append(item)
    out.append("</ul>")
    return "\n".join(out) + "\n"


def render(
    feed: Feed,
    rows: list[Source],
    statuses: dict[str, SourceStatus],
    now: datetime,
) -> tuple[str, str]:
    """The (head, body) fragments for one feed."""
    since = feed_since(feed, statuses)
    return head_fragment(feed, rows, since, now), body_fragment(feed, rows, since)


def sitemap(entries: list[tuple[str, str]]) -> bytes:
    """(loc, lastmod date) pairs to a sitemap. One file holds up to 50,000."""
    urls = "".join(
        f"<url><loc>{escape(loc)}</loc><lastmod>{lastmod}</lastmod></url>"
        for loc, lastmod in entries
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{urls}</urlset>"
    ).encode()
