"""Normalizing a download URL so two catalogs can be recognised as agreeing.

Transitland and the Mobility Database list many of the same feeds, with URLs
that differ only in case, a default port or query-parameter order. Comparing
raw strings finds almost none of those; comparing normalized ones finds most.
The normalized form is a comparison key only - it is never fetched, so it may
lose information a request would need.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

# The index key of gtfs/index.json, shared with every reader of the bucket.
from gtfs_zone_feed_fetch.urls import is_absolute as is_absolute
from gtfs_zone_feed_fetch.urls import normalize_url as normalize_url


def with_scheme(url: str) -> str:
    """`url` with https:// in front when it names a host but no scheme, as some
    Mobility Database producer URLs do (`example.org/gtfs.zip`)."""
    host, slash, _ = url.partition("/")
    if "://" in url or not slash or "." not in host or " " in host:
        return url
    return f"https://{url}"


def link_key(url: str) -> str:
    """A key for whether two catalogs list the same feed: `normalize_url` with
    http and https unified, since catalogs move a URL to https at different
    times, and a leading www. dropped, since a host and its www. name serve the
    same feed. Only for linking rows; checks still tell them apart."""
    key = normalize_url(url)
    return key.removeprefix("https://").removeprefix("http://").removeprefix("www.")


def host_of(url: str) -> str:
    """The host a request would hit, for per-host rate limiting.

    "" when the URL cannot be parsed; the request itself then fails as a check.
    """
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


# Final path segments that name one GTFS-RT entity type, compared lowercased
# with separators and extension removed.
RT_ENTITY_SEGMENTS = frozenset(
    {
        "vehicles",
        "vehiclepositions",
        "trips",
        "tripupdates",
        "alerts",
        "servicealerts",
    }
)


def rt_sibling_key(url: str) -> str:
    """The normalized URL minus a final entity-type segment, or "".

    ``https://x/rt/vehicles.pb`` and ``https://x/rt/TripUpdates`` share a key,
    so an agency that serves each entity type at its own path is recognised as
    one realtime feed. The query is kept: two feeds behind one path told apart
    by ``?agency=`` stay apart.
    """
    key = normalize_url(url)
    if not key:
        return ""
    parts = urlsplit(key)
    head, _, last = parts.path.rpartition("/")
    stem = last.split(".", 1)[0].lower().replace("_", "").replace("-", "")
    if stem not in RT_ENTITY_SEGMENTS:
        return ""
    return urlunsplit((parts.scheme, parts.netloc, head, parts.query, ""))
