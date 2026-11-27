"""Transitous: the feed list of a community-run routing service, as source rows.

``github.com/public-transport/transitous`` keeps one JSON file per region under
``feeds/`` (``de.json``, ``au-nsw.json``, ``eu.json``), each a list of
``sources``. The whole repository is read with one GET of its tarball, and the
region files are read from it in memory; nothing is extracted to disk.

A source is one of:

- ``http`` / ``url`` / ``ftp``: a URL of its own. ``url-override`` is
  Transitous' own mirror and is ignored. A URL that is not http(s) (an FTP
  server, an ``AGE-ENCRYPTED:`` secret, a ``#1/google_transit.zip`` a script
  resolves) is not one we can request, and its source is skipped.
- ``mobility-database`` / ``transitland-atlas``: an ``mdb-id`` or a
  ``transitland-atlas-id``, and no URL. The row carries none either and joins
  the referenced feed's rows by ``feed_references``, whatever their kind: a
  reference to an Atlas realtime feed often declares no ``spec``.

``spec`` (default ``gtfs``) picks the kind: ``gtfs`` is static, ``gtfs-rt`` is
realtime with the untyped ``realtime`` role. GBFS, SIRI and NeTEx are skipped
and counted. A ``skip: true`` source stays a row with its ``skip-reason`` but
is not checked or fetched; ``headers`` / ``api-key`` mark it as behind a key.

A row's feed id is ``<region>/<name>``, sticky while the source keeps its name.
Transitous pairs a realtime source with the schedule of the same name, so the
sources of one name in a region are one catalog feed, as a DMFR feed's static
and realtime rows are. Their row ids end in the kind, and a kind the name has
more than once gets ``-2``, ``-3`` in file order. A realtime URL listed under
more than one name (one alerts endpoint for every operator of a state) pairs
with none of them: its rows get a feed id of their own, ``<region>/<name>/<kind>``.

A catalog that cannot be fetched yields no rows rather than failing the run;
the rows' feeds keep their ids for the absent-retention window.
"""

import io
import json
import logging
import tarfile
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import PurePosixPath
from typing import Any

import httpx
import pycountry
from dagster import asset

from gtfs_zone_feed_catalog.catalog import Place, Source
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.urls import is_absolute, link_key, with_scheme

log = logging.getLogger(__name__)

ORIGIN = "Transitous"
REGION_FILE_BASE = "https://github.com/public-transport/transitous/blob/main/feeds/"

# spec -> row kind. Anything else is not loadable as GTFS.
KINDS = {"gtfs": "static", "gtfs-rt": "rt"}
# Source types that reference another catalog's row, and that row's prefix.
REFERENCES = {
    "mobility-database": ("mdb-id", "md"),
    "transitland-atlas": ("transitland-atlas-id", "tl"),
}
# MDB's authentication_type for a key sent in a header, which is how every
# keyed Transitous source sends its key.
AUTH_HEADER = 2


def region_files(body: bytes) -> dict[str, list[dict[str, Any]]]:
    """{region: its sources} from the repository tarball's ``feeds/*.json``."""
    regions: dict[str, list[dict[str, Any]]] = {}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
        for member in tar:
            path = PurePosixPath(member.name)
            # <top>/feeds/<region>.json, not the vendored Atlas or anything nested.
            if not member.isfile() or len(path.parts) != 3:
                continue
            if path.parts[1] != "feeds" or path.suffix != ".json":
                continue
            handle = tar.extractfile(member)
            if handle is None:
                continue
            try:
                doc = json.loads(handle.read())
            except (UnicodeDecodeError, json.JSONDecodeError):
                log.warning("transitous: skipping unreadable %s", path.name)
                continue
            sources = doc.get("sources") if isinstance(doc, dict) else None
            regions[path.stem] = [s for s in sources or [] if isinstance(s, dict)]
    return regions


def place_of(region: str) -> Place:
    """The country, and the subdivision for a ``cc-sub`` region; none for a
    code ISO 3166 does not know (``eu``)."""
    code = region.upper()
    country = pycountry.countries.get(alpha_2=code.split("-", 1)[0])
    if country is None:
        return Place()
    subdivision = pycountry.subdivisions.get(code=code) if "-" in code else None
    return Place(
        country_code=country.alpha_2,
        country=getattr(country, "common_name", None) or country.name,
        subdivision=subdivision.name if subdivision else "",
    )


def license_url(source: dict[str, Any]) -> str:
    """The license's URL, else the SPDX page of its identifier."""
    license_ = source.get("license")
    if not isinstance(license_, dict):
        return ""
    if url := (license_.get("url") or "").strip():
        return url
    if spdx := (license_.get("spdx-identifier") or "").strip():
        return f"https://spdx.org/licenses/{spdx}.html"
    return ""


def _row(region: str, source: dict[str, Any], kind: str, source_id: str) -> Source:
    common: dict[str, Any] = {
        "source_id": source_id,
        "catalog": "transitous",
        "kind": kind,
        "feed_id": f"{region}/{source['name']}",
        "name": source["name"].replace("-", " ").strip(),
        "origin": f"{ORIGIN} {region}",
        "place": place_of(region),
        "authentication_type": AUTH_HEADER
        if source.get("headers") or source.get("api-key")
        else 0,
        "license_url": license_url(source),
        "skip_reason": (
            (source.get("skip-reason") or "").strip() or "skipped"
            if source.get("skip")
            else ""
        ),
    }
    if reference := REFERENCES.get(source.get("type") or ""):
        key, prefix = reference
        return Source(feed_references=(f"{prefix}:{source[key]}",), **common)
    role = "scheduled" if kind == "static" else "realtime"
    return Source(urls={role: with_scheme(source["url"].strip())}, **common)


def build_sources(regions: dict[str, list[dict[str, Any]]]) -> list[Source]:
    rows: list[Source] = []
    skipped: Counter[str] = Counter()
    for region in sorted(regions):
        seen: Counter[tuple[str, str]] = Counter()
        for source in regions[region]:
            name = source.get("name")
            spec = source.get("spec") or "gtfs"
            kind = KINDS.get(spec)
            reference = REFERENCES.get(source.get("type") or "")
            if kind is None:
                skipped[spec] += 1
                continue
            if not isinstance(name, str) or not name.strip():
                skipped["no name"] += 1
                continue
            if reference and not source.get(reference[0]):
                skipped["no reference id"] += 1
                continue
            url = source.get("url")
            if not reference and not (
                isinstance(url, str) and is_absolute(with_scheme(url.strip()))
            ):
                skipped["no http(s) url"] += 1
                continue
            seen[name, kind] += 1
            suffix = f"-{seen[name, kind]}" if seen[name, kind] > 1 else ""
            source_id = f"transitous:{region}/{name}:{kind}{suffix}"
            rows.append(_row(region, source, kind, source_id))
    if skipped:
        log.info("transitous: skipped %s", dict(sorted(skipped.items())))
    return sorted(unpair_shared(rows), key=lambda row: row.source_id)


def unpair_shared(rows: list[Source]) -> list[Source]:
    """`rows` with each realtime row whose URL other names also list moved to
    a feed id of its own, so the URL does not join their schedules."""
    names: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.kind == "rt" and (key := link_key(row.urls.get("realtime", ""))):
            names[key].add(row.feed_id)
    return [
        replace(row, feed_id=f"{row.feed_id}/{row.source_id.rsplit(':', 1)[1]}")
        if row.kind == "rt"
        and len(names.get(link_key(row.urls.get("realtime", "")), ())) > 1
        else row
        for row in rows
    ]


@asset(
    group_name="catalogs",
    description="Transitous region feed lists, from its repository tarball",
)
def transitous() -> list[Source]:
    url = settings.transitous_url
    if not url:
        log.warning("TRANSITOUS_URL is unset; skipping the catalog")
        return []
    try:
        response = httpx.get(
            url,
            headers={"User-Agent": settings.check_user_agent},
            timeout=settings.check_timeout_seconds,
            follow_redirects=True,
        )
        response.raise_for_status()
        regions = region_files(response.content)
    except (httpx.HTTPError, tarfile.TarError, OSError) as exc:
        log.warning("transitous catalog unavailable: %s", exc)
        return []
    rows = build_sources(regions)
    log.info("transitous: %d region files, %d source rows", len(regions), len(rows))
    return rows
