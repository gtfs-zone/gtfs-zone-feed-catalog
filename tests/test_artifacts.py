"""The published documents: compatibility, dedupe and snapshot retention."""

import gzip
import json
from datetime import UTC, date, datetime

from gtfs_zone_feed_catalog import artifacts
from gtfs_zone_feed_catalog.assets.check_history import SourceStatus
from gtfs_zone_feed_catalog.assets.endpoint_checks import CheckResult
from gtfs_zone_feed_catalog.assets.feed_contents import FeedContent
from gtfs_zone_feed_catalog.assets.feeds import build_feeds
from gtfs_zone_feed_catalog.catalog import Place, Source

NOW = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)

# The field names the old public/atlas-feeds.json had. A consumer moving to
# sources.json must not need to change how it reads a row.
ATLAS_FIELDS = {
    "rowId",
    "kind",
    "feedId",
    "name",
    "operator_name",
    "source",
    "scheduledUrl",
}

ROW = Source(
    source_id="tl:f-a:static",
    catalog="transitland",
    kind="static",
    feed_id="f-a",
    name="A",
    operator_name="Agency A",
    origin="example.org",
    urls={"scheduled": "https://example.org/a.zip"},
    place=Place(country_code="US", latitude=34.0, longitude=-118.0),
)
UP = SourceStatus("tl:f-a:static", "up", checked_at=NOW, status_code=200)


def test_a_source_row_still_looks_like_an_atlas_row():
    row = artifacts.source_row(ROW, UP)
    assert set(row) >= ATLAS_FIELDS
    assert row["scheduledUrl"] == "https://example.org/a.zip"
    assert row["source"] == "example.org"
    # New fields sit alongside rather than replacing anything.
    assert row["catalog"] == "transitland"
    assert row["state"] == "up"
    assert (row["lat"], row["lon"]) == (34.0, -118.0)


def test_an_unplaced_row_carries_no_coordinate_keys_at_all():
    row = artifacts.source_row(
        Source(
            source_id="tl:f-b:static",
            catalog="transitland",
            kind="static",
            feed_id="f-b",
            name="B",
            urls={"scheduled": "https://example.org/b.zip"},
        ),
        None,
    )
    assert "lat" not in row
    assert "state" not in row


def test_the_summary_publishes_the_unplaced_count():
    summary = json.loads(
        artifacts.summary_document([ROW], {"tl:f-a:static": UP}, [], NOW)
    )
    assert summary["total"] == 1
    assert summary["placed"] == 1
    assert summary["unplaced"] == 0
    assert summary["by_state"] == {"up": 1}
    assert summary["by_country"] == {"US": 1}


def test_the_snapshot_payload_ignores_the_timestamp():
    statuses = {"tl:f-a:static": UP}
    first = artifacts.snapshot_payload(statuses)
    second = artifacts.snapshot_payload(statuses)
    assert artifacts.sha256(first) == artifacts.sha256(second)
    # And the timestamp is still in the file itself.
    body = json.loads(gzip.decompress(artifacts.snapshot_body(first, NOW)))
    assert body["generated_at"] == NOW.isoformat()
    assert body["sources"]["tl:f-a:static"]["state"] == "up"


def test_the_payload_changes_when_an_answer_does():
    down = SourceStatus("tl:f-a:static", "down", status_code=500)
    assert artifacts.sha256(
        artifacts.snapshot_payload({"tl:f-a:static": UP})
    ) != artifacts.sha256(artifacts.snapshot_payload({"tl:f-a:static": down}))


def index_entry(day: str) -> dict:
    return {"date": day, "key": artifacts.snapshot_key(date.fromisoformat(day))}


def test_snapshot_retention_thins_to_weekly_then_monthly():
    today = date(2026, 9, 22)
    index = [
        index_entry("2026-09-20"),  # inside the daily window
        index_entry("2026-09-21"),
        index_entry("2026-07-06"),  # a Monday, first of its ISO week: kept
        index_entry("2026-07-08"),  # same ISO week: dropped
        index_entry("2026-07-13"),  # next ISO week: kept
        index_entry("2024-03-02"),  # past the weekly window, first of its month
        index_entry("2024-03-19"),  # same month: dropped
        index_entry("2024-04-01"),  # next month: kept
    ]
    dropped = artifacts.snapshots_to_delete(
        index, today, daily_days=30, weekly_days=365
    )
    assert dropped == [
        artifacts.snapshot_key(date(2024, 3, 19)),
        artifacts.snapshot_key(date(2026, 7, 8)),
    ]


def test_the_manifest_hashes_every_artifact():
    bodies = {"sources.json": b"{}", "status.json": b"[]"}
    manifest = json.loads(artifacts.manifest_document(bodies, NOW, "run-1"))
    assert manifest["run_id"] == "run-1"
    assert manifest["artifacts"]["sources.json"]["bytes"] == 2
    assert manifest["artifacts"]["sources.json"]["sha256"] == artifacts.sha256(b"{}")
    assert manifest["attribution"] == artifacts.ATTRIBUTION


def test_feeds_lists_moved_ids():
    document = artifacts.feeds_document([], {}, NOW, moved={"f-b": "f-a"})
    assert json.loads(document)["moved"] == {"f-b": "f-a"}


def test_sources_and_feeds_credit_every_catalog_and_its_license():
    sources = json.loads(artifacts.sources_document([ROW], {}, NOW))
    feeds = json.loads(artifacts.feeds_document([], {}, NOW))
    for document in (sources, feeds):
        catalogs = {
            c["name"]: c["license"] for c in document["attribution"]["catalogs"]
        }
        assert catalogs == {
            "Transitland Atlas": "CC-BY-4.0",
            "Mobility Database": "CC0-1.0",
            "National Transit Database (FTA)": "Public Domain U.S. Government",
        }


STATIC = Source(
    source_id="md:mdb-1:static",
    catalog="mobilitydatabase",
    kind="static",
    feed_id="mdb-1",
    name="Metro",
    urls={"scheduled": "https://a.org/gtfs.zip"},
    place=Place(country_code="US", latitude=40.0, longitude=-74.0),
)
REALTIME = Source(
    source_id="md:mdb-2:rt",
    catalog="mobilitydatabase",
    kind="rt",
    feed_id="mdb-2",
    name="Metro RT",
    urls={"vehicles": "https://a.org/rt/vehicles"},
    feed_references=("mdb-1",),
    license_url="https://a.org/license",
)
MODIFIED = datetime(2026, 9, 1, tzinfo=UTC)


def test_a_feed_entry_carries_roles_state_size_and_place():
    results = {
        "https://a.org/gtfs.zip": CheckResult(
            url="https://a.org/gtfs.zip",
            ok=True,
            checked_at=NOW,
            content_length=4096,
            last_modified=MODIFIED,
        ),
        "https://a.org/rt/vehicles": CheckResult(
            url="https://a.org/rt/vehicles", ok=True, checked_at=NOW
        ),
    }
    (feed,) = build_feeds([STATIC, REALTIME], results, {})
    since = datetime(2026, 8, 1, tzinfo=UTC)
    statuses = {
        "md:mdb-1:static": SourceStatus("md:mdb-1:static", "up", since=since),
        "md:mdb-2:rt": SourceStatus("md:mdb-2:rt", "up", since=NOW),
    }
    document = json.loads(
        artifacts.feeds_document([feed], statuses, NOW, sources=[STATIC, REALTIME])
    )
    assert document["count"] == 1
    (entry,) = document["feeds"]
    assert entry["feedId"] == feed.feed_id
    assert entry["name"] == "Metro"
    assert entry["members"] == ["md:mdb-1:static", "md:mdb-2:rt"]
    assert entry["urls"] == {
        "scheduled": ["https://a.org/gtfs.zip"],
        "vehicles": ["https://a.org/rt/vehicles"],
    }
    assert entry["roleState"] == {"scheduled": "up", "vehicles": "up"}
    assert entry["state"] == "up"
    assert entry["staticBytes"] == 4096
    assert entry["lastModified"] == MODIFIED.isoformat()
    # Up since the last of its rows came up.
    assert entry["since"] == NOW.isoformat()
    assert (entry["country_code"], entry["lat"]) == ("US", 40.0)
    assert "auth" not in entry
    assert entry["licenses"] == ["https://a.org/license"]
    assert entry["catalogLinks"] == [
        "https://mobilitydatabase.org/feeds/gtfs/mdb-1",
        "https://mobilitydatabase.org/feeds/gtfs_rt/mdb-2",
    ]


def test_a_feed_lists_the_answering_schedule_first():
    # Two rows of one catalog feed are one logical feed, whatever their URLs.
    stale, moved = (
        Source(
            source_id=f"tl:f-a:{suffix}",
            catalog="transitland",
            kind="static",
            feed_id="f-a",
            name="A",
            urls={"scheduled": url},
        )
        for suffix, url in (
            ("static", "https://a.org/gtfs.zip"),
            ("mirror", "https://b.org/gtfs.zip"),
        )
    )
    results = {
        "https://a.org/gtfs.zip": CheckResult(
            url="https://a.org/gtfs.zip", ok=False, checked_at=NOW, status_code=404
        ),
        "https://b.org/gtfs.zip": CheckResult(
            url="https://b.org/gtfs.zip", ok=True, checked_at=NOW, content_length=9
        ),
    }
    (feed,) = build_feeds([stale, moved], results, {})
    entry = artifacts.feed_entry(feed, {})
    assert entry["urls"]["scheduled"] == [
        "https://b.org/gtfs.zip",
        "https://a.org/gtfs.zip",
    ]
    assert entry["staticBytes"] == 9


def test_a_role_only_behind_an_api_key_is_published_as_auth():
    keyed = CheckResult(
        url="https://a.org/rt/vehicles",
        ok=False,
        checked_at=NOW,
        error_class="auth_required",
        skipped=True,
    )
    (feed,) = build_feeds([STATIC, REALTIME], {"https://a.org/rt/vehicles": keyed}, {})
    entry = artifacts.feed_entry(feed, {})
    assert entry["auth"] == ["vehicles"]
    assert entry["roleState"]["vehicles"] == "unknown"


def test_a_down_feed_is_down_since_its_earliest_failing_row():
    (feed,) = build_feeds(
        [STATIC],
        {
            "https://a.org/gtfs.zip": CheckResult(
                url="https://a.org/gtfs.zip", ok=False, checked_at=NOW
            )
        },
        {},
    )
    statuses = {
        "md:mdb-1:static": SourceStatus("md:mdb-1:static", "down", since=MODIFIED)
    }
    assert artifacts.feed_entry(feed, statuses)["since"] == MODIFIED.isoformat()


def test_a_down_feed_no_check_has_seen_up_is_published_as_never_up():
    (feed,) = build_feeds(
        [STATIC],
        {
            "https://a.org/gtfs.zip": CheckResult(
                url="https://a.org/gtfs.zip", ok=False, checked_at=NOW
            )
        },
        {},
    )
    never = SourceStatus("md:mdb-1:static", "down", since=MODIFIED, never_up=True)
    statuses = {"md:mdb-1:static": never}
    assert artifacts.feed_entry(feed, statuses)["neverUp"] is True
    assert artifacts.search_entry(feed, statuses)["nu"] is True
    assert artifacts.status_entries(statuses)["md:mdb-1:static"]["never_up"] is True

    was_up = {"md:mdb-1:static": SourceStatus("md:mdb-1:static", "down")}
    assert "neverUp" not in artifacts.feed_entry(feed, was_up)
    assert "nu" not in artifacts.search_entry(feed, was_up)
    assert "never_up" not in artifacts.status_entries(was_up)["md:mdb-1:static"]


def test_the_summary_counts_feeds_as_well_as_rows():
    feeds = build_feeds([STATIC, REALTIME, ROW], {}, {})
    summary = json.loads(
        artifacts.summary_document([STATIC, REALTIME, ROW], {}, feeds, NOW)
    )
    assert summary["total"] == 3
    assert summary["feeds"] == {
        "total": 2,
        "by_state": {"unknown": 2},
        "realtime": 1,
        "placed": 2,
        "unplaced": 0,
        "by_content": {},
    }


def test_a_partial_feed_is_partial_since_its_down_realtime_row():
    results = {
        "https://a.org/gtfs.zip": CheckResult(
            url="https://a.org/gtfs.zip", ok=True, checked_at=NOW
        ),
        "https://a.org/rt/vehicles": CheckResult(
            url="https://a.org/rt/vehicles", ok=False, checked_at=NOW
        ),
    }
    (feed,) = build_feeds([STATIC, REALTIME], results, {})
    statuses = {
        "md:mdb-1:static": SourceStatus("md:mdb-1:static", "up", since=NOW),
        "md:mdb-2:rt": SourceStatus("md:mdb-2:rt", "down", since=MODIFIED),
    }
    entry = artifacts.feed_entry(feed, statuses)
    assert (entry["state"], entry["since"]) == ("partial", MODIFIED.isoformat())


def test_a_search_entry_is_the_feed_cut_down():
    results = {
        "https://a.org/gtfs.zip": CheckResult(
            url="https://a.org/gtfs.zip",
            ok=True,
            checked_at=NOW,
            content_length=4096,
            last_modified=MODIFIED,
        ),
    }
    operated = Source(
        source_id="tl:f-metro:static",
        catalog="transitland",
        kind="static",
        feed_id="f-metro",
        name="Metro",
        operator_name="Metro Transit Authority",
        urls={"scheduled": "https://a.org/gtfs.zip"},
    )
    placed = STATIC.with_place(
        Place(
            country_code="US",
            country="United States",
            municipality="Newark",
            latitude=40.0,
            longitude=-74.0,
        )
    )
    rows = [placed, REALTIME, operated]
    (feed,) = build_feeds(rows, results, {})
    document = json.loads(artifacts.search_document([feed], {}, NOW, sources=rows))
    (entry,) = document["feeds"]
    assert document["attribution"]["places"].startswith("Municipalities: Overture")
    assert entry == {
        "i": feed.feed_id,
        "n": "Metro",
        "a": ["Metro RT", "Metro Transit Authority"],
        "st": "up",
        "rs": {"scheduled": "up", "vehicles": "unknown"},
        "u": {
            "scheduled": ["https://a.org/gtfs.zip"],
            "vehicles": ["https://a.org/rt/vehicles"],
        },
        "p": ["Newark", "United States"],
        "cc": "US",
        "ll": [40.0, -74.0],
        "b": 4096,
        "m": "2026-09-01",
    }


def test_a_search_entry_carries_the_content_reports_place_tags():
    (feed,) = build_feeds([STATIC], {}, {})
    content = FeedContent(
        url="https://a.org/gtfs.zip",
        outcome="ok",
        checked=date(2026, 11, 15),
        since=date(2026, 11, 1),
        facts={
            "placeTags": {
                "countries": ["United States", "Canada"],
                "regions": ["Massachusetts", "Ontario"],
                "municipalities": ["Boston", "Toronto"],
            }
        },
    )
    contents = {"https://a.org/gtfs.zip": content}
    entry = artifacts.search_entry(feed, {}, contents)
    assert entry["pt"] == [
        "United States",
        "Canada",
        "Massachusetts",
        "Ontario",
        "Boston",
        "Toronto",
    ]
    place = Place(
        country_code="US",
        country="United States",
        subdivision="Massachusetts",
        latitude=42.0,
        longitude=-71.0,
    )
    (placed,) = build_feeds([STATIC.with_place(place)], {}, {})
    entry = artifacts.search_entry(placed, {}, contents)
    assert entry["pt"] == ["Canada", "Ontario", "Boston", "Toronto"]
    assert "pt" not in artifacts.search_entry(feed, {})


def test_agencies_and_overlaps_reach_feeds_and_search():
    other = Source(
        source_id="md:mdb-9:static",
        catalog="mobilitydatabase",
        kind="static",
        feed_id="mdb-9",
        name="City",
        urls={"scheduled": "https://city.org/g.zip"},
    )

    def facts(url: str, *names: str) -> FeedContent:
        return FeedContent(
            url=url,
            outcome="ok",
            checked=date(2026, 11, 15),
            since=date(2026, 11, 1),
            facts={
                "agencies": [{"name": name} for name in names],
                "location": {"bbox": [0, 0, 1, 1]},
            },
        )

    contents = {
        "https://a.org/gtfs.zip": facts("https://a.org/gtfs.zip", "Metro", "City Bus"),
        "https://city.org/g.zip": facts("https://city.org/g.zip", "City Bus"),
    }
    agg, city = sorted(
        build_feeds([STATIC, other], {}, {}, contents),
        key=lambda feed: len(feed.agencies),
        reverse=True,
    )
    entry = artifacts.feed_entry(agg, {}, contents)
    assert entry["agencies"] == ["City Bus", "Metro"]
    assert entry["agencyKeys"] == ["city bus", "metro"]
    assert entry["agencyCount"] == 2
    assert entry["overlaps"] == [
        {"feedId": city.feed_id, "kind": "contains", "agencies": ["City Bus"]}
    ]
    search = artifacts.search_entry(city, {}, contents)
    assert (search["ac"], search["ov"]) == (1, [[agg.feed_id, "containedIn"]])

    (alone,) = build_feeds([STATIC], {}, {})
    assert not {"agencies", "agencyKeys", "agencyCount", "overlaps"} & set(
        artifacts.feed_entry(alone, {})
    )
    assert not {"ac", "ov"} & set(artifacts.search_entry(alone, {}))


def test_the_summary_counts_ntd_rows_and_links_no_catalog_page():
    ntd = Source(
        source_id="ntd:00001-abcdef:static",
        catalog="ntd",
        kind="static",
        feed_id="00001-abcdef",
        name="King County",
        urls={"scheduled": "https://example.org/a.zip"},
    )
    summary = json.loads(artifacts.summary_document([ROW, ntd], {}, [], NOW))
    assert summary["by_catalog"] == {"transitland": 1, "ntd": 1}
    assert artifacts.catalog_url(ntd) is None


def stats_row(source_id: str, kind: str = "static", **fields) -> Source:
    catalog, feed_id, _ = source_id.split(":")
    return Source(
        source_id=source_id,
        catalog={"tl": "transitland", "md": "mobilitydatabase", "ntd": "ntd"}[catalog],
        kind=kind,
        feed_id=feed_id,
        name=source_id,
        **fields,
    )


def test_source_stats_counts_catalogs_overlap_and_join_evidence():
    from gtfs_zone_feed_fetch.store import Stored

    def stored(url: str, outcome: str, sha: str = "", size: int = 0) -> Stored:
        return Stored(
            url=url,
            checked=NOW.isoformat(),
            outcome=outcome,
            key=f"gtfs/{sha}.zip" if sha else "",
            sha256=sha,
            bytes=size or None,
            kind="zip" if sha else "",
        )

    def ok(url: str, sha: str) -> FeedContent:
        return FeedContent(
            url=url, outcome="ok", checked=NOW.date(), since=NOW.date(), sha256=sha
        )

    us = Place(country_code="US")
    rows = [
        # Shared URL: Transitland and the Mobility Database.
        stats_row("tl:f-a:static", urls={"scheduled": "https://a.org/g.zip"}, place=us),
        stats_row("md:mdb-1:static", urls={"scheduled": "http://a.org/g.zip"}),
        # Same bytes at two URLs: Transitland and NTD.
        stats_row("tl:f-b:static", urls={"scheduled": "https://b.org/g.zip"}),
        stats_row("ntd:1:static", urls={"scheduled": "https://mirror.org/b.zip"}),
        # One entry split into static and realtime rows: one entry, no join.
        stats_row("md:mdb-2:static", urls={"scheduled": "https://c.org/g.zip"}),
        stats_row("md:mdb-2:rt", kind="rt", urls={"vehicles": "https://c.org/vp"}),
    ]
    contents = {
        "https://b.org/g.zip": ok("https://b.org/g.zip", "bb"),
        "https://mirror.org/b.zip": ok("https://mirror.org/b.zip", "bb"),
    }
    fetched = {
        "https://a.org/g.zip": stored("https://a.org/g.zip", "not_modified", "aa", 10),
        "http://a.org/g.zip": stored("http://a.org/g.zip", "not_modified", "aa", 10),
        "https://b.org/g.zip": stored("https://b.org/g.zip", "ok", "bb", 5),
        "https://mirror.org/b.zip": stored("https://mirror.org/b.zip", "ok", "bb", 5),
        "https://c.org/g.zip": stored("https://c.org/g.zip", "too_large"),
    }
    feeds = build_feeds(rows, {}, {}, contents)
    doc = json.loads(
        artifacts.source_stats_document(rows, feeds, NOW, contents, fetched)
    )

    tl = doc["catalogs"]["transitland"]
    assert tl == {
        "entries": 2,
        "feeds": 2,
        "scheduleFeeds": 2,
        "onlyHere": 0,
        "fetch": {"not_modified": 1, "ok": 1},
        "facts": {"ok": 1},
        "bytes": 15,
        "distinctBytes": 15,
        "countries": 1,
    }
    md = doc["catalogs"]["mobilitydatabase"]
    assert (md["entries"], md["feeds"], md["onlyHere"]) == (2, 2, 1)
    assert md["fetch"] == {"not_modified": 1, "too_large": 1}
    assert (md["bytes"], md["distinctBytes"]) == (10, 10)
    assert doc["catalogs"]["ntd"]["bytes"] == 5

    assert doc["combinations"] == [
        {"catalogs": ["mobilitydatabase"], "feeds": 1},
        {"catalogs": ["mobilitydatabase", "transitland"], "feeds": 1},
        {"catalogs": ["ntd", "transitland"], "feeds": 1},
    ]
    assert doc["pairs"] == [
        {
            "catalogs": ["mobilitydatabase", "transitland"],
            "feeds": 1,
            "scheduleUrls": 1,
            "bodies": 0,
        },
        {
            "catalogs": ["ntd", "transitland"],
            "feeds": 1,
            "scheduleUrls": 0,
            "bodies": 1,
        },
    ]
    assert doc["duplication"] == {
        "feeds": 3,
        "multiEntry": 2,
        "byEvidence": {"sha256": 1, "url": 1},
        "entriesPerFeed": {"1": 1, "2": 2},
    }
    assert "closeDuplicates" not in doc
