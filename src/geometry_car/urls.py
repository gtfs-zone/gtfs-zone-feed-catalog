"""Normalizing a download URL so two catalogs can be recognised as agreeing.

Transitland and the Mobility Database list many of the same feeds, with URLs
that differ only in case, a default port or query-parameter order. Comparing
raw strings finds almost none of those; comparing normalized ones finds most.
The normalized form is a comparison key only - it is never fetched, so it may
lose information a request would need.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit, urlunsplit

DEFAULT_PORTS = {"http": "80", "https": "443"}


def is_absolute(url: str) -> bool:
    """Whether a URL names a host to talk to. The scheme's case is not its own."""
    return url.strip().lower().startswith(("http://", "https://"))


def normalize_url(url: str) -> str:
    """A comparison key for a download URL, or "" when there is nothing to compare.

    Lowercases the scheme and host, drops a default port, drops a trailing
    slash on an empty path, sorts query parameters and drops the fragment.
    http and https are *not* unified: a plain-http endpoint is a materially
    different thing to check, and several curated entries exist only in that
    form.
    """
    url = url.strip()
    if not is_absolute(url):
        return ""

    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        port = parts.port
    except ValueError:
        # A malformed host or port; there is nothing to compare.
        return ""
    scheme = parts.scheme.lower()
    netloc = (
        host
        if port is None or str(port) == DEFAULT_PORTS.get(scheme)
        else f"{host}:{port}"
    )

    path = parts.path.rstrip("/") if parts.path != "/" else ""
    query = "&".join(
        f"{k}={v}" for k, v in sorted(parse_qsl(parts.query, keep_blank_values=True))
    )
    return urlunsplit((scheme, netloc, path, query, ""))


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
