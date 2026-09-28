"""cape-flier's content report: fetched, recorded per URL, published per feed."""

import json
import re
from datetime import UTC, date, datetime

import httpx
import respx
from sqlalchemy import select

from geometry_car import artifacts, pages
from geometry_car.assets import feed_contents as module
from geometry_car.assets.feed_contents import (
    FeedContent,
    fetch_report,
    parse_entry,
    record,
)
from geometry_car.assets.feeds import build_feeds
from geometry_car.catalog import Place, Source
from geometry_car.history.models import EndpointContentState, EndpointRecord
from geometry_car.settings import settings

BASE = "https://sites.example/"
NOW = datetime(2026, 9, 30, 9, 0, tzinfo=UTC)
URL = "https://example.org/bay.zip"

OK = {
    "feedId": "f-0000000001",
    "url": "https://Example.org:443/bay.zip",
    "checked": "2026-09-29",
    "version": "0.8.0",
    "outcome": "ok",
    "since": "2026-09-20",
    "head": {"bytes": 10, "lastModified": None},
    "bytes": 10,
    "sha256": "ab" * 32,
    "service": {"start": "2026-01-01", "end": "2026-12-31"},
    "feedInfo": {"publisher": "Bay Pub", "version": "v7"},
    "agencies": [{"name": "Bay Transit", "url": "https://bay", "timezone": "X"}],
    "counts": {"routes": 3, "stops": 40, "trips": 0},
    "routeTypes": [3],
    "filtered": False,
}
BAD = {
    "url": URL,
    "checked": "2026-09-29",
    "outcome": "not_zip",
    "since": "2026-09-29",
    "detail": "html",
}
ROW = Source(
    source_id="md:mdb-1:static",
    catalog="mobilitydatabase",
    kind="static",
    feed_id="mdb-1",
    name="Bay Transit",
    urls={"scheduled": URL},
    place=Place(country_code="US"),
)
(FEED,) = build_feeds([ROW], {}, {})


def test_an_entry_parses_onto_its_normalized_url():
    content = parse_entry(OK)
    assert content.url == URL
    assert content.since == date(2026, 9, 20)
    assert content.size == 10 and content.sha256 == "ab" * 32
    assert content.facts["counts"]["routes"] == 3
    assert not {"url", "head", "bytes", "sha256", "version"} & set(content.facts)
    assert parse_entry(BAD).facts == {} and parse_entry(BAD).detail == "html"
    assert parse_entry({"url": "/relative.zip", "outcome": "ok"}) is None


@respx.mock
def test_the_report_is_read_from_every_listed_shard():
    respx.get(f"{BASE}_content/index.json").respond(
        json={"shards": ["_content/00.json", "_content/01.json"]}
    )
    older = BAD | {"checked": "2026-09-01"}
    respx.get(f"{BASE}_content/00.json").respond(json={"a": OK, "gone": None})
    respx.get(f"{BASE}_content/01.json").respond(json={"b": older})
    with httpx.Client() as http:
        contents = fetch_report(http, BASE)
    # Two sites on one URL: the later check wins.
    assert list(contents) == [URL]
    assert contents[URL].outcome == "ok"


def _endpoint(session_factory, url):
    with session_factory() as session, session.begin():
        session.add(EndpointRecord(url=url, state="up", consecutive_failures=0))


def test_recording_writes_a_state_row_per_outcome_change(session_factory):
    _endpoint(session_factory, URL)
    unknown = parse_entry(OK | {"url": "https://nowhere.example/x.zip"})

    first = record({URL: parse_entry(BAD), unknown.url: unknown}, NOW)
    assert set(first) == {URL}
    assert first[URL].outcome == "not_zip"

    record({URL: parse_entry(BAD | {"checked": "2026-09-30"})}, NOW)
    fixed = record({URL: parse_entry(OK | {"checked": "2026-10-01"})}, NOW)
    assert fixed[URL].outcome == "ok"
    assert fixed[URL].facts["feedInfo"]["publisher"] == "Bay Pub"

    # An older report never overwrites a newer record.
    stale = record({URL: parse_entry(BAD)}, NOW)
    assert stale[URL].outcome == "ok"

    with session_factory() as session:
        states = session.scalars(
            select(EndpointContentState.outcome).order_by(EndpointContentState.id)
        ).all()
    assert states == ["not_zip", "ok"]


@respx.mock
def test_an_unreachable_report_keeps_what_was_recorded(session_factory, monkeypatch):
    monkeypatch.setattr(settings, "content_report_base", BASE)
    _endpoint(session_factory, URL)
    record({URL: parse_entry(BAD)}, NOW)
    respx.get(f"{BASE}_content/index.json").respond(503)
    contents = module.feed_contents(check_history={})
    assert contents[URL].outcome == "not_zip"


def test_a_feed_entry_carries_its_schedule_content():
    contents = {URL: parse_entry(OK)}
    entry = artifacts.feed_entry(FEED, {}, contents)
    assert entry["content"] == {
        "state": "ok",
        "since": "2026-09-20",
        "checked": "2026-09-29",
        "serviceStart": "2026-01-01",
        "serviceEnd": "2026-12-31",
        "publisher": "Bay Pub",
        "version": "v7",
        "routes": 3,
        "stops": 40,
        "trips": 0,
        "routeTypes": [3],
    }
    bad = artifacts.feed_entry(FEED, {}, {URL: parse_entry(BAD)})["content"]
    assert bad == {
        "state": "not_zip",
        "since": "2026-09-29",
        "checked": "2026-09-29",
        "detail": "html",
    }
    assert "content" not in artifacts.feed_entry(FEED, {}, {})
    summary = json.loads(
        artifacts.summary_document([ROW], {}, [FEED], NOW, {URL: parse_entry(BAD)})
    )
    assert summary["feeds"]["by_content"] == {"not_zip": 1}


def test_the_feed_page_shows_what_the_download_held():
    head, body = pages.render(FEED, [ROW], {}, NOW, {URL: parse_entry(OK)})
    assert "Last download: a valid GTFS zip since 2026-09-20" in body
    assert "Service: 2026-01-01 to 2026-12-31" in body
    assert "Publisher: Bay Pub" in body and "Agencies: Bay Transit" in body
    assert "3 routes, 40 stops, 0 trips" in body
    ld = json.loads(re.search(r'ld\+json">(.*)</script>', head).group(1))
    assert ld["temporalCoverage"] == "2026-01-01/2026-12-31"

    _, bad = pages.render(FEED, [ROW], {}, NOW, {URL: parse_entry(BAD)})
    assert "Last download: not a zip file (an HTML page) since 2026-09-29" in bad
    assert "Service:" not in bad


def test_a_content_dataclass_defaults_to_no_facts():
    content = FeedContent(URL, "timeout", date(2026, 9, 29), date(2026, 9, 29))
    assert content.facts == {}
    assert pages.content_facts(content)[0].startswith(
        "Last download: a download that timed out since"
    )
