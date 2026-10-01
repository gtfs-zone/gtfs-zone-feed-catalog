"""The FTA National Transit Database's GTFS weblinks, as static source rows.

Every NTD reporter with fixed-route service submits a public GTFS weblink, and
FTA publishes them as the "General Transit Feed Specification Weblinks"
dataset on data.transportation.gov. It is read over Socrata's SODA3 query API,
paged with ``pageNumber``/``pageSize`` and stopped on a short page. The API
answers anonymously today; ``NTD_APP_TOKEN`` is sent as ``X-App-Token`` when
set, should Socrata start requiring one.

The dataset has one record per agency x mode x type of service, and mode and
type of service do not change the URL, so records collapse to one row per
(NTD id, normalized weblink). The NTD id names an agency, not a feed, and some
agencies list several weblinks, so a row's feed id is the NTD id plus a short
hash of the normalized URL: stable while the URL is, and a changed URL is a
new endpoint anyway.

A catalog that cannot be fetched yields no rows rather than failing the run;
the rows' feeds keep their ids for the absent-retention window.
"""

import hashlib
import logging
from typing import Any

import httpx
from dagster import asset

from gtfs_zone_feed_catalog.catalog import Place, Source, make_rows
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.urls import normalize_url

log = logging.getLogger(__name__)

PAGE_SIZE = 1000
QUERY = (
    "SELECT ntd_id, agency_name, city, state, weblink, weblink_justification"
    " ORDER BY :id"
)
ORIGIN = "FTA National Transit Database"

# USPS codes the dataset uses, to the full names the Mobility Database uses as
# its subdivision_name.
STATES = {
    "AK": "Alaska",
    "AL": "Alabama",
    "AR": "Arkansas",
    "AS": "American Samoa",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "GU": "Guam",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MP": "Northern Mariana Islands",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VI": "U.S. Virgin Islands",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}


def agency_name(name: str) -> str:
    """The agency's name, its "dba" name in place of the legal one."""
    name = name.strip()
    _, dba, alias = name.partition(", dba:")
    return alias.strip() if dba and alias.strip() else name


def weblink(record: dict[str, Any]) -> str:
    """The record's weblink, which SODA3 types as ``{"url": ...}``."""
    link = record.get("weblink")
    if isinstance(link, dict):
        link = link.get("url")
    return (link or "").strip() if isinstance(link, str) else ""


def place_of(record: dict[str, Any]) -> Place:
    state = (record.get("state") or "").strip().upper()
    return Place(
        country_code="US",
        country="United States",
        subdivision=STATES.get(state, state),
        municipality=(record.get("city") or "").strip(),
    )


def build_sources(records: list[dict[str, Any]]) -> list[Source]:
    rows: dict[str, Source] = {}
    skipped = 0
    for record in records:
        ntd_id = (record.get("ntd_id") or "").strip()
        url = weblink(record)
        key = normalize_url(url)
        if not ntd_id or not key:
            skipped += 1
            continue
        feed_id = f"{ntd_id}-{hashlib.sha256(key.encode()).hexdigest()[:6]}"
        if feed_id in rows:
            continue
        name = agency_name(record.get("agency_name") or "") or ntd_id
        for row in make_rows(
            catalog="ntd",
            id_prefix="ntd",
            feed_id=feed_id,
            name=name,
            operator_name=name,
            origin=ORIGIN,
            scheduled=url,
            place=place_of(record),
        ):
            rows[feed_id] = row
    if skipped:
        log.info("ntd: skipped %d records without a usable weblink", skipped)
    return sorted(rows.values(), key=lambda row: row.source_id)


def fetch_records(client: httpx.Client, url: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    page_number = 1
    while True:
        response = client.post(
            url,
            json={
                "query": QUERY,
                "page": {"pageNumber": page_number, "pageSize": PAGE_SIZE},
            },
        )
        if response.status_code in (httpx.codes.UNAUTHORIZED, httpx.codes.FORBIDDEN):
            log.warning(
                "ntd: HTTP %d; set NTD_APP_TOKEN if Socrata now requires one",
                response.status_code,
            )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            raise ValueError(f"expected a JSON array, got {type(page).__name__}")
        records += page
        if len(page) < PAGE_SIZE:
            return records
        page_number += 1


@asset(
    group_name="catalogs",
    description="FTA National Transit Database GTFS weblinks, as source rows",
)
def ntd() -> list[Source]:
    url = settings.ntd_weblinks_url
    if not url:
        log.warning("NTD_WEBLINKS_URL is unset; skipping the catalog")
        return []
    headers = {"User-Agent": settings.check_user_agent}
    if settings.ntd_app_token:
        headers["X-App-Token"] = settings.ntd_app_token
    try:
        with httpx.Client(
            headers=headers,
            timeout=settings.check_timeout_seconds,
            follow_redirects=True,
        ) as client:
            records = fetch_records(client, url)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("ntd catalog unavailable: %s", exc)
        return []
    rows = build_sources(records)
    log.info("ntd: %d records, %d source rows", len(records), len(rows))
    return rows
