"""Transitland Atlas: the DMFR corpus, one row per source kind.

Ported from ``interlocking/scripts/generate-atlas-data.ts``, including the part
that is easy to get wrong. The operator index is built in two passes over the
*whole* corpus, because a feed almost never declares ``operators[]`` - the link
runs the other way, from ``operator.associated_feeds[].feed_onestop_id`` back
to the feed, and usually from a different file than the feed lives in. A
per-file index resolves almost nothing.

A local checkout is preferred when one is configured: it is ~730 files, and
fetching them one at a time from GitHub is both slow and rude. The cluster has
no checkout and uses the tree API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from dagster import asset

from geometry_car.catalog import Source, make_rows
from geometry_car.settings import settings

if TYPE_CHECKING:
    from collections.abc import Iterable

log = logging.getLogger(__name__)

# GBFS is bikeshare discovery, not something a GTFS consumer can load.
USABLE_SPECS = {"gtfs", "gtfs-rt"}

CONCURRENCY = 8

# A onestop id's second dash-segment is a geohash when there is one.
GEOHASH_SEGMENT = re.compile(r"^[0-9bcdefghjkmnpqrstuvwxyz]{1,6}$")


def origin_from_filename(path: str) -> str:
    """``feeds/511.org.dmfr.json`` -> ``511.org``. The domain is search context."""
    name = path.rsplit("/", 1)[-1]
    for suffix in (".dmfr.json", ".json"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def humanize_feed_id(feed_id: str) -> str:
    """A readable name for a feed no operator resolves.

    ``f-9q8-samtrans`` -> ``samtrans``; ``f-columbia~county~public~transportation``
    -> ``columbia county public transportation``. The geohash segment is dropped
    only when a third segment remains, so a genuinely short name is not eaten.
    """
    segments = feed_id.split("-")
    if segments and segments[0] == "f":
        segments.pop(0)
    if len(segments) > 1 and GEOHASH_SEGMENT.match(segments[0]):
        segments.pop(0)
    return "-".join(segments).replace("~", " ").replace("_", " ").strip()


def build_operator_index(docs: Iterable[tuple[str, dict]]) -> dict[str, dict]:
    """Operator per feed onestop id, resolved across the whole corpus."""
    docs = list(docs)
    by_onestop_id: dict[str, dict] = {}
    by_feed_id: dict[str, dict] = {}

    for _origin, dmfr in docs:
        for op in dmfr.get("operators") or []:
            if onestop_id := op.get("onestop_id"):
                by_onestop_id[onestop_id] = op
            for assoc in op.get("associated_feeds") or []:
                feed_onestop_id = assoc.get("feed_onestop_id")
                # First operator to claim a feed wins; later files do not clobber.
                if feed_onestop_id and feed_onestop_id not in by_feed_id:
                    by_feed_id[feed_onestop_id] = op

    # Second pass for the minority of feeds that do declare operators[].
    for _origin, dmfr in docs:
        for feed in dmfr.get("feeds") or []:
            feed_id = feed.get("id")
            if not feed_id or feed_id in by_feed_id:
                continue
            operators = feed.get("operators") or []
            op_id = operators[0].get("onestop_id") if operators else None
            if op_id and (op := by_onestop_id.get(op_id)):
                by_feed_id[feed_id] = op

    return by_feed_id


def build_sources(docs: Iterable[tuple[str, dict]]) -> list[Source]:
    docs = list(docs)
    operators_by_feed_id = build_operator_index(docs)
    rows: list[Source] = []
    seen: set[str] = set()

    for origin, dmfr in docs:
        for feed in dmfr.get("feeds") or []:
            feed_id = feed.get("id")
            if not feed_id or feed.get("spec", "gtfs") not in USABLE_SPECS:
                continue

            urls = feed.get("urls") or {}
            op = operators_by_feed_id.get(feed_id) or {}
            license_url = (feed.get("license") or {}).get("url", "")

            for row in make_rows(
                catalog="transitland",
                id_prefix="tl",
                feed_id=feed_id,
                name=op.get("short_name")
                or op.get("name")
                or humanize_feed_id(feed_id),
                operator_name=op.get("name") or op.get("short_name") or "",
                origin=origin,
                scheduled=urls.get("static_current", ""),
                vehicles=urls.get("realtime_vehicle_positions", ""),
                trip_updates=urls.get("realtime_trip_updates", ""),
                alerts=urls.get("realtime_alerts", ""),
                license_url=license_url,
            ):
                # The corpus repeats a feed id across files often enough to matter.
                if row.source_id in seen:
                    continue
                seen.add(row.source_id)
                rows.append(row)

    rows.sort(key=lambda r: r.source_id)
    return rows


def read_local(atlas_path: str) -> list[tuple[str, dict]]:
    feeds_dir = Path(atlas_path) / "feeds"
    docs: list[tuple[str, dict]] = []
    for path in sorted(feeds_dir.rglob("*.json")):
        try:
            docs.append((origin_from_filename(path.name), json.loads(path.read_text())))
        except (OSError, json.JSONDecodeError):
            # One malformed file must not cost the whole corpus.
            log.warning("skipping unreadable DMFR file %s", path.name)
    return docs


async def _fetch_remote() -> list[tuple[str, dict]]:
    repo = settings.transitland_atlas_repo
    ref = settings.transitland_atlas_ref
    headers = {"Accept": "application/json", "User-Agent": settings.check_user_agent}

    async with httpx.AsyncClient(headers=headers, timeout=60.0) as client:
        tree = (
            (
                await client.get(
                    f"https://api.github.com/repos/{repo}/git/trees/{ref}?recursive=1"
                )
            )
            .raise_for_status()
            .json()
        )
        paths = [
            item["path"]
            for item in tree.get("tree", [])
            if item.get("type") == "blob"
            and item["path"].startswith("feeds/")
            and item["path"].endswith(".json")
        ]

        limit = asyncio.Semaphore(CONCURRENCY)

        async def one(path: str) -> tuple[str, dict]:
            async with limit:
                url = f"https://raw.githubusercontent.com/{repo}/{ref}/{path}"
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    return origin_from_filename(path), response.json()
                except (httpx.HTTPError, json.JSONDecodeError):
                    log.warning("skipping unreadable DMFR file %s", path)
                    return origin_from_filename(path), {}

        return list(await asyncio.gather(*(one(p) for p in paths)))


@asset(
    group_name="catalogs", description="Transitland Atlas DMFR corpus, as source rows"
)
def transitland_atlas() -> list[Source]:
    path = settings.transitland_atlas_path
    if path and (Path(path) / "feeds").is_dir():
        log.info("reading the DMFR corpus from %s", path)
        docs = read_local(path)
    else:
        log.info("fetching the DMFR corpus from GitHub")
        docs = asyncio.run(_fetch_remote())

    sources = build_sources(docs)
    log.info("transitland: %d documents, %d source rows", len(docs), len(sources))
    return sources
