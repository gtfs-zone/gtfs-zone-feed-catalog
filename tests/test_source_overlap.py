"""Catalog overlap counts."""

from gtfs_zone_feed_catalog.assets.feed_contents import FeedContent
from gtfs_zone_feed_catalog.assets.feeds import build_feeds
from gtfs_zone_feed_catalog.assets.source_overlap import markdown, overlap
from tests.test_feeds import NOW, row


def content(url: str, sha256: str) -> FeedContent:
    return FeedContent(
        url=url, outcome="ok", checked=NOW.date(), since=NOW.date(), sha256=sha256
    )


def test_overlap_counts_feeds_urls_and_bodies_per_catalog_pair():
    rows = [
        # Same URL in Transitland and the Mobility Database.
        row("tl:f-a:static", kind="static", scheduled="https://a.org/g.zip"),
        row("md:mdb-1:static", kind="static", scheduled="http://a.org/g.zip"),
        # Same bytes at two URLs, Transitland and NTD.
        row("tl:f-b:static", kind="static", scheduled="https://b.org/g.zip"),
        row("ntd:1:static", kind="static", scheduled="https://mirror.org/b.zip"),
        # One catalog only, and a realtime-only feed left out.
        row("md:mdb-2:static", kind="static", scheduled="https://c.org/g.zip"),
        row("md:mdb-3:rt", vehicles="https://d.org/vp"),
    ]
    contents = {
        "https://b.org/g.zip": content("https://b.org/g.zip", "bb"),
        "https://mirror.org/b.zip": content("https://mirror.org/b.zip", "bb"),
    }
    feeds = build_feeds(rows, {}, {}, contents)
    result = overlap(rows, feeds, contents)

    assert result.combos == {
        ("mobilitydatabase", "transitland"): 1,
        ("ntd", "transitland"): 1,
        ("mobilitydatabase",): 1,
    }
    assert result.listed == {"transitland": 2, "mobilitydatabase": 2, "ntd": 1}
    assert result.alone == {"mobilitydatabase": 1}
    assert result.urls == {("mobilitydatabase", "transitland"): 1}
    assert result.bodies == {("ntd", "transitland"): 1}
    assert "| ntd + transitland | 1 | 0 | 1 |" in markdown(result)
