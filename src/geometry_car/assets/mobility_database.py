"""Mobility Database: the other catalog, and the only source of coordinates.

Two calls' worth of shape worth stating, because both are easy to assume wrong:

- Authentication is a *refresh* token exchanged for a short-lived access token
  at ``POST /v1/tokens/access``. The refresh token is long-lived and secret;
  neither it nor the access token is ever logged.
- ``/v1/gtfs_feeds`` and ``/v1/gtfs_rt_feeds`` return a bare JSON array with
  ``limit``/``offset`` paging and no total in the body, so paging stops on a
  short page. ``limit`` is capped per endpoint by the schema: 2500 for
  ``gtfs_feeds``, 1000 for ``gtfs_rt_feeds``. Over the cap is a 422.

A realtime feed here is one endpoint that declares which entity types it
carries (``vp``/``tu``/``sa``), not three separate URLs the way DMFR has it, so
every declared role points at the same producer URL.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx
from dagster import asset

from geometry_car.catalog import Place, Source, make_rows
from geometry_car.settings import settings

if TYPE_CHECKING:
    from collections.abc import Iterator

log = logging.getLogger(__name__)

# Largest ``limit`` each endpoint accepts.
PAGE_SIZES = {"gtfs_feeds": 2500, "gtfs_rt_feeds": 1000}

# GTFS-RT entity type -> the URL role it satisfies.
ENTITY_ROLES = {"vp": "vehicles", "tu": "trip_updates", "sa": "alerts"}


class MobilityDatabaseError(RuntimeError):
    """The catalog refused a call."""


def fetch_access_token(client: httpx.Client, refresh_token: str) -> str:
    response = client.post(
        f"{settings.mobility_db_base_url}/tokens/access",
        json={"refresh_token": refresh_token},
    )
    if response.status_code != httpx.codes.OK:
        # The body echoes nothing secret, but say nothing about the token either.
        raise MobilityDatabaseError(
            f"token exchange failed with HTTP {response.status_code}"
        )
    token = response.json().get("access_token")
    if not token:
        raise MobilityDatabaseError("token exchange returned no access_token")
    return token


def iter_feeds(client: httpx.Client, path: str) -> Iterator[dict]:
    page_size = PAGE_SIZES[path]
    offset = 0
    while True:
        response = client.get(
            f"{settings.mobility_db_base_url}/{path}",
            params={"limit": page_size, "offset": offset},
        )
        response.raise_for_status()
        page = response.json()
        yield from page
        if len(page) < page_size:
            return
        offset += len(page)


def place_of(feed: dict) -> Place:
    """Location and centroid for one feed.

    The bounding box lives on the feed for a GTFS feed and on nothing at all for
    a realtime one, so a realtime row is placed by its location names only
    unless a cross-link later supplies coordinates.
    """
    locations = feed.get("locations") or [{}]
    location = locations[0] or {}
    box = feed.get("bounding_box") or (feed.get("latest_dataset") or {}).get(
        "bounding_box"
    )

    latitude = longitude = None
    bbox = None
    if box and all(
        box.get(k) is not None
        for k in (
            "minimum_latitude",
            "maximum_latitude",
            "minimum_longitude",
            "maximum_longitude",
        )
    ):
        bbox = (
            float(box["minimum_latitude"]),
            float(box["minimum_longitude"]),
            float(box["maximum_latitude"]),
            float(box["maximum_longitude"]),
        )
        latitude = (bbox[0] + bbox[2]) / 2
        longitude = (bbox[1] + bbox[3]) / 2

    return Place(
        country_code=location.get("country_code") or "",
        country=location.get("country") or "",
        subdivision=location.get("subdivision_name") or "",
        municipality=location.get("municipality") or "",
        latitude=latitude,
        longitude=longitude,
        bbox=bbox,
    )


def _name(feed: dict) -> str:
    provider = feed.get("provider") or ""
    feed_name = feed.get("feed_name") or ""
    if provider and feed_name:
        return f"{provider} ({feed_name})"
    return provider or feed_name or feed.get("id", "")


def _common(feed: dict) -> dict[str, Any]:
    source_info = feed.get("source_info") or {}
    return {
        "name": _name(feed),
        "operator_name": feed.get("provider") or "",
        "origin": "mobilitydatabase.org",
        "status": feed.get("status") or "",
        "authentication_type": int(source_info.get("authentication_type") or 0),
        "license_url": source_info.get("license_url") or "",
    }


def build_static_sources(feeds: Iterator[dict] | list[dict]) -> list[Source]:
    rows: list[Source] = []
    for feed in feeds:
        feed_id = feed.get("id")
        url = (feed.get("source_info") or {}).get("producer_url") or ""
        if not feed_id or not url:
            continue
        place = place_of(feed)
        rows.extend(
            row.with_place(place)
            for row in make_rows(
                catalog="mobilitydatabase",
                id_prefix="md",
                feed_id=feed_id,
                scheduled=url,
                **_common(feed),
            )
        )
    return rows


def build_realtime_sources(feeds: Iterator[dict] | list[dict]) -> list[Source]:
    rows: list[Source] = []
    for feed in feeds:
        feed_id = feed.get("id")
        url = (feed.get("source_info") or {}).get("producer_url") or ""
        if not feed_id or not url:
            continue
        # One endpoint, one URL, however many entity types it declares. A feed
        # that declares none is still realtime; treat it as vehicle positions so
        # it is checked rather than silently dropped.
        roles = {
            ENTITY_ROLES[entity]
            for entity in feed.get("entity_types") or []
            if entity in ENTITY_ROLES
        } or {"vehicles"}
        place = place_of(feed)
        rows.extend(
            row.with_place(place)
            for row in make_rows(
                catalog="mobilitydatabase",
                id_prefix="md",
                feed_id=feed_id,
                vehicles=url if "vehicles" in roles else "",
                trip_updates=url if "trip_updates" in roles else "",
                alerts=url if "alerts" in roles else "",
                **_common(feed),
            )
        )
    return rows


@asset(
    group_name="catalogs",
    description="Mobility Database GTFS and GTFS-RT feeds, as source rows",
)
def mobility_database() -> list[Source]:
    if not settings.mobility_db_refresh_token:
        # Not a failure: a developer without the token still gets a usable run
        # off Transitland and the curated set.
        log.warning("MOBILITY_DB_REFRESH_TOKEN is unset; skipping the catalog")
        return []

    with httpx.Client(
        timeout=120.0, headers={"User-Agent": settings.check_user_agent}
    ) as client:
        token = fetch_access_token(client, settings.mobility_db_refresh_token)
        client.headers["Authorization"] = f"Bearer {token}"
        rows = build_static_sources(iter_feeds(client, "gtfs_feeds"))
        rows += build_realtime_sources(iter_feeds(client, "gtfs_rt_feeds"))

    rows.sort(key=lambda r: r.source_id)
    placed = sum(1 for r in rows if r.place.placed)
    log.info("mobility database: %d source rows, %d placed", len(rows), placed)
    return rows
