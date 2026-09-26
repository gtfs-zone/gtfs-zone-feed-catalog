"""Feed page fragments and the sitemap: what a crawler reads, and when it changes."""

import json
import re
from datetime import UTC, datetime, timedelta

from geometry_car import pages
from geometry_car.assets.check_history import SourceStatus
from geometry_car.assets.feeds import build_feeds
from geometry_car.assets.published_artifacts import publish_pages
from geometry_car.catalog import Place, Source

NOW = datetime(2026, 9, 20, 6, 0, tzinfo=UTC)

PLACE = Place(
    country_code="US",
    country="United States",
    subdivision="California",
    municipality="Oakland",
    latitude=37.8,
    longitude=-122.3,
    bbox=(37.5, -122.5, 38.0, -122.0),
)
ROWS = [
    Source(
        source_id="md:mdb-1:static",
        catalog="mobilitydatabase",
        kind="static",
        feed_id="mdb-1",
        name="Bay </script> & Transit",
        operator_name="Bay Transit",
        urls={"scheduled": "https://example.org/bay.zip"},
        place=PLACE,
        license_url="https://example.org/license",
    ),
    Source(
        source_id="md:mdb-2:rt",
        catalog="mobilitydatabase",
        kind="rt",
        feed_id="mdb-2",
        name="Bay Transit RT",
        urls={"vehicles": "https://example.org/vp.pb"},
        feed_references=("mdb-1",),
    ),
]
FEEDS = build_feeds(ROWS, {}, {})
STATUSES = {
    "md:mdb-1:static": SourceStatus(
        "md:mdb-1:static", "up", since=NOW - timedelta(days=3)
    ),
    "md:mdb-2:rt": SourceStatus("md:mdb-2:rt", "up", since=NOW - timedelta(days=1)),
}


def _render():
    (feed,) = FEEDS
    return pages.render(feed, ROWS, STATUSES, NOW)


def _json_ld(head: str) -> dict:
    match = re.search(r'<script type="application/ld\+json">(.*)</script>', head)
    assert match
    return json.loads(match.group(1))


def test_the_head_names_the_feed_and_points_at_its_path():
    (feed,) = FEEDS
    head, _ = _render()

    assert f"https://list.gtfs.zone/feed/{feed.feed_id}/bay-script-transit" in head
    assert "GTFS and GTFS Realtime feed - list.gtfs.zone</title>" in head
    assert "noindex" not in head


def test_catalog_text_is_escaped_everywhere():
    head, body = _render()

    # One </script> only: the JSON-LD element's own closing tag.
    assert head.count("</script>") == 1
    assert "Bay &lt;/script&gt; &amp; Transit" in body
    assert _json_ld(head)["name"].startswith("Bay </script> & Transit")


def test_the_dataset_carries_downloads_place_and_license():
    doc = _json_ld(_render()[0])

    assert doc["@type"] == "Dataset"
    assert {d["contentUrl"] for d in doc["distribution"]} == {
        "https://example.org/bay.zip",
        "https://example.org/vp.pb",
    }
    assert doc["license"] == "https://example.org/license"
    assert (
        doc["spatialCoverage"]["geo"]["box"]
        == "37.50000 -122.50000 38.00000 -122.00000"
    )
    assert doc["creator"] == [{"@type": "Organization", "name": "Bay Transit"}]
    assert doc["alternateName"] == ["Bay Transit RT"]
    assert "from Mobility Database." in doc["description"]


def test_the_body_links_the_sibling_apps_and_catalogs():
    _, body = _render()

    assert "https://edit.gtfs.zone/#load=https%3A%2F%2Fexample.org%2Fbay.zip" in body
    assert "https://viz.rt.gtfs.zone/#scheduled=" in body
    assert "https://mobilitydatabase.org/feeds/gtfs_rt/mdb-2" in body


def test_a_long_dead_or_nameless_feed_is_noindex():
    (feed,) = FEEDS
    long_ago = NOW - timedelta(days=200)

    assert pages.indexable(feed, None, NOW)
    assert not pages.indexable(
        feed.__class__(**{**_fields(feed), "state": "down"}), long_ago, NOW
    )
    assert not pages.indexable(
        feed.__class__(**{**_fields(feed), "name": " "}), None, NOW
    )


def _fields(feed) -> dict:
    return {name: getattr(feed, name) for name in feed.__dataclass_fields__}


def test_publishing_rewrites_only_changed_pages(store):
    first = publish_pages(store, ROWS, FEEDS, STATUSES, NOW)
    (feed,) = FEEDS
    prefix = f"pages/feed/{feed.feed_id}/"

    assert first == {"pages_written": 1, "pages_removed": 0, "pages_indexed": 1}
    assert b"<h1>" in store.get(f"{prefix}body.html")
    sitemap = store.get("sitemap.xml").decode()
    assert (
        f"/feed/{feed.feed_id}/bay-script-transit</loc><lastmod>2026-09-20" in sitemap
    )

    later = NOW + timedelta(days=5)
    again = publish_pages(store, ROWS, FEEDS, STATUSES, later)
    assert again["pages_written"] == 0
    # Unchanged content keeps the date it last changed on.
    assert "<lastmod>2026-09-20</lastmod>" in store.get("sitemap.xml").decode()

    gone = publish_pages(store, ROWS, [], STATUSES, later)
    assert gone["pages_removed"] == 1
    assert store.list_prefix(prefix) == []
