"""The feeds rt.gtfs.zone serves, from cafe-car's public catalog.

``GET /feeds`` lists every feed cafe-car publishes with its schedule URL and its
three realtime URLs. Each becomes a static and an rt row under the ``gtfszone``
catalog. The rows carry no name or place of their own: a feed's name is a slug,
so a feed that shares its schedule URL with a Transitland or Mobility Database
row takes its name and place from there.

A catalog that cannot be fetched yields no rows rather than failing the run;
the rows' feeds keep their ids for the absent-retention window.
"""

import logging
from typing import Any

import httpx
from dagster import asset

from geometry_car.catalog import Source, make_rows
from geometry_car.settings import settings

log = logging.getLogger(__name__)


def build_sources(entries: list[dict[str, Any]]) -> list[Source]:
    rows: list[Source] = []
    for entry in entries:
        feed_name = entry.get("feed_name") or ""
        if not feed_name:
            continue
        rows.extend(
            make_rows(
                catalog="gtfszone",
                id_prefix="gz",
                feed_id=feed_name,
                name=feed_name.replace("-", " ").replace("_", " "),
                origin="rt.gtfs.zone",
                scheduled=entry.get("static_url") or "",
                vehicles=entry.get("vehicle_positions_url") or "",
                trip_updates=entry.get("trip_updates_url") or "",
                alerts=entry.get("service_alerts_url") or "",
            )
        )
    return sorted(rows, key=lambda row: row.source_id)


@asset(group_name="catalogs", description="Feeds served by rt.gtfs.zone (cafe-car)")
def cafe_car() -> list[Source]:
    url = settings.cafe_car_catalog_url
    if not url:
        log.warning("CAFE_CAR_CATALOG_URL is unset; skipping the catalog")
        return []
    try:
        response = httpx.get(
            url,
            headers={"User-Agent": settings.check_user_agent},
            timeout=settings.check_timeout_seconds,
            follow_redirects=True,
        )
        response.raise_for_status()
        entries = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("cafe-car catalog unavailable: %s", exc)
        return []
    rows = build_sources(entries if isinstance(entries, list) else [])
    log.info("cafe-car: %d feeds, %d source rows", len(entries), len(rows))
    return rows
