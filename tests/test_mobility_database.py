"""The Mobility Database client: token exchange, paging and coordinates."""

import httpx
import pytest
import respx

from geometry_car.assets.mobility_database import (
    MobilityDatabaseError,
    build_realtime_sources,
    build_static_sources,
    fetch_access_token,
    iter_feeds,
    place_of,
)
from geometry_car.settings import settings

BASE = settings.mobility_db_base_url


def gtfs_feed(feed_id: str, **overrides) -> dict:
    feed = {
        "id": feed_id,
        "data_type": "gtfs",
        "provider": "Example Transit",
        "status": "active",
        "source_info": {"producer_url": f"https://example.org/{feed_id}.zip"},
        "locations": [
            {
                "country_code": "US",
                "country": "United States",
                "subdivision_name": "California",
                "municipality": "Los Angeles",
            }
        ],
        "bounding_box": {
            "minimum_latitude": 33.0,
            "maximum_latitude": 35.0,
            "minimum_longitude": -119.0,
            "maximum_longitude": -117.0,
        },
    }
    return feed | overrides


@respx.mock
def test_token_exchange_returns_the_access_token():
    respx.post(f"{BASE}/tokens/access").respond(json={"access_token": "abc"})
    with httpx.Client() as client:
        assert fetch_access_token(client, "refresh") == "abc"


@respx.mock
def test_token_exchange_failure_says_nothing_about_the_token():
    respx.post(f"{BASE}/tokens/access").respond(500, json={"error": "nope"})
    with httpx.Client() as client, pytest.raises(MobilityDatabaseError) as excinfo:
        fetch_access_token(client, "s3cret-refresh-token")
    assert "s3cret" not in str(excinfo.value)


@respx.mock
def test_paging_stops_on_a_short_page():
    # The endpoint returns a bare array with no total, so a short page is the
    # only end-of-results signal there is.
    full = [gtfs_feed(f"mdb-{i}") for i in range(2500)]
    respx.get(f"{BASE}/gtfs_feeds", params={"offset": 0}).respond(json=full)
    respx.get(f"{BASE}/gtfs_feeds", params={"offset": 2500}).respond(
        json=[gtfs_feed("mdb-9999")]
    )
    with httpx.Client() as client:
        feeds = list(iter_feeds(client, "gtfs_feeds"))
    assert len(feeds) == 2501
    assert feeds[-1]["id"] == "mdb-9999"


@respx.mock
def test_realtime_paging_uses_the_smaller_cap():
    # gtfs_rt_feeds rejects limit > 1000 with a 422.
    full = [gtfs_feed(f"rt-{i}", data_type="gtfs_rt") for i in range(1000)]
    first = respx.get(
        f"{BASE}/gtfs_rt_feeds", params={"limit": 1000, "offset": 0}
    ).respond(json=full)
    second = respx.get(
        f"{BASE}/gtfs_rt_feeds", params={"limit": 1000, "offset": 1000}
    ).respond(json=[])
    with httpx.Client() as client:
        feeds = list(iter_feeds(client, "gtfs_rt_feeds"))
    assert len(feeds) == 1000
    assert first.called
    assert second.called


@respx.mock
def test_a_record_that_breaks_the_server_is_skipped():
    # The API 500s on any page containing a bad record, even alone.
    feeds = [gtfs_feed(f"rt-{i}", data_type="gtfs_rt") for i in range(10)]
    bad = {3}

    def page(request):
        limit = int(request.url.params["limit"])
        offset = int(request.url.params["offset"])
        window = range(offset, min(offset + limit, len(feeds)))
        if bad & set(window):
            return httpx.Response(500)
        return httpx.Response(200, json=[feeds[i] for i in window])

    respx.get(f"{BASE}/gtfs_rt_feeds").mock(side_effect=page)
    with httpx.Client() as client:
        got = [f["id"] for f in iter_feeds(client, "gtfs_rt_feeds")]
    assert got == [f"rt-{i}" for i in range(10) if i not in bad]


@respx.mock
def test_an_api_that_fails_everywhere_raises():
    respx.get(f"{BASE}/gtfs_rt_feeds").respond(500)
    with httpx.Client() as client, pytest.raises(MobilityDatabaseError):
        list(iter_feeds(client, "gtfs_rt_feeds"))


def test_static_rows_carry_the_centroid_and_the_place():
    rows = build_static_sources([gtfs_feed("mdb-1")])
    assert [r.source_id for r in rows] == ["md:mdb-1:static"]
    row = rows[0]
    assert row.place.latitude == 34.0
    assert row.place.longitude == -118.0
    assert row.place.bbox == (33.0, -119.0, 35.0, -117.0)
    assert row.place.municipality == "Los Angeles"


def test_a_feed_without_a_producer_url_is_dropped():
    assert build_static_sources([gtfs_feed("mdb-2", source_info={})]) == []


def test_bounding_box_falls_back_to_the_latest_dataset():
    feed = gtfs_feed("mdb-3")
    box = feed.pop("bounding_box")
    feed["latest_dataset"] = {"bounding_box": box}
    assert place_of(feed).placed


def test_realtime_entity_types_become_url_roles():
    feed = gtfs_feed(
        "mdb-rt",
        data_type="gtfs_rt",
        entity_types=["vp", "tu"],
        source_info={"producer_url": "https://example.org/rt.pb"},
    )
    rows = build_realtime_sources([feed])
    assert [r.source_id for r in rows] == ["md:mdb-rt:rt"]
    # One endpoint, however many entity types it declares.
    assert rows[0].urls == {
        "vehicles": "https://example.org/rt.pb",
        "trip_updates": "https://example.org/rt.pb",
    }


def test_a_realtime_feed_declaring_no_entity_types_is_still_checked():
    feed = gtfs_feed(
        "mdb-bare",
        data_type="gtfs_rt",
        source_info={"producer_url": "https://example.org/bare.pb"},
    )
    assert build_realtime_sources([feed])[0].urls == {
        "vehicles": "https://example.org/bare.pb"
    }


def test_authentication_type_is_carried_so_the_checker_can_skip_it():
    feed = gtfs_feed(
        "mdb-key",
        source_info={
            "producer_url": "https://example.org/key.zip",
            "authentication_type": 2,
        },
    )
    assert build_static_sources([feed])[0].authentication_type == 2
