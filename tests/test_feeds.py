"""Logical feeds: grouping rules, names, states and sticky ids."""

from datetime import UTC, datetime

from sqlalchemy import select

from geometry_car.assets.check_history import DOWN, UNKNOWN, UP, fold, record
from geometry_car.assets.endpoint_checks import CheckResult
from geometry_car.assets.feeds import (
    assign_ids,
    build_feeds,
    group,
    load_existing,
    persist,
)
from geometry_car.catalog import Place, Source
from geometry_car.history.models import FeedMember, FeedRecord
from geometry_car.urls import rt_sibling_key

NOW = datetime(2026, 9, 1, tzinfo=UTC)


def row(source_id: str, kind: str = "rt", **urls: str) -> Source:
    catalog, feed_id, _ = source_id.split(":")
    return Source(
        source_id=source_id,
        catalog={"tl": "transitland", "md": "mobilitydatabase", "curated": "curated"}[
            catalog
        ],
        kind=kind,
        feed_id=feed_id,
        name=source_id,
        urls=urls,
    )


def ok(url: str, **facts) -> CheckResult:
    return CheckResult(url=url, ok=True, checked_at=NOW, status_code=200, **facts)


def failed(url: str) -> CheckResult:
    return CheckResult(
        url=url, ok=False, checked_at=NOW, status_code=500, error_class="http_5xx"
    )


def ids(groups: list[list[Source]]) -> list[list[str]]:
    return [[r.source_id for r in g] for g in groups]


def test_split_rt_endpoints_on_one_path_become_one_feed():
    rows = [
        row("md:mdb-1:rt", vehicles="https://rt.example.org/x/vehicles"),
        row("md:mdb-2:rt", trip_updates="https://rt.example.org/x/trips"),
        row("md:mdb-3:rt", alerts="https://rt.example.org/x/alerts"),
    ]
    feeds = build_feeds(rows, {}, {})
    assert len(feeds) == 1
    assert set(feeds[0].urls) == {"vehicles", "trip_updates", "alerts"}


def test_rt_sibling_key_ignores_case_separators_and_extension():
    assert rt_sibling_key("https://x.org/rt/VehiclePositions.pb") == (
        rt_sibling_key("https://x.org/rt/trip_updates")
    )
    assert rt_sibling_key("https://x.org/rt/Alerts.json") == "https://x.org/rt"
    assert rt_sibling_key("https://x.org/rt/feed.pb") == ""


def test_rt_endpoints_told_apart_by_query_stay_apart():
    rows = [
        row("md:mdb-1:rt", vehicles="https://rt.example.org/vehicles?agency=a"),
        row("md:mdb-2:rt", vehicles="https://rt.example.org/trips?agency=b"),
    ]
    assert len(group(rows)) == 2


def test_mdb_static_and_rt_linked_by_feed_references_become_one_feed():
    static = row("md:mdb-10:static", kind="static", scheduled="https://a.org/g.zip")
    rt = Source(
        source_id="md:mdb-11:rt",
        catalog="mobilitydatabase",
        kind="rt",
        feed_id="mdb-11",
        name="B",
        urls={"vehicles": "https://b.org/vp"},
        feed_references=("mdb-10",),
    )
    unrelated = row("md:mdb-12:rt", vehicles="https://c.org/vp")
    assert ids(group([static, rt, unrelated])) == [
        ["md:mdb-10:static", "md:mdb-11:rt"],
        ["md:mdb-12:rt"],
    ]


def test_catalogs_sharing_a_url_and_one_feed_split_in_two_are_one_feed():
    rows = [
        row("tl:f-a:static", kind="static", scheduled="https://a.org/g.zip"),
        row("tl:f-a:rt", vehicles="https://a.org/rt/feed"),
        row("md:mdb-1:static", kind="static", scheduled="HTTPS://A.org:443/g.zip"),
    ]
    assert len(group(rows)) == 1


def test_name_prefers_curated_then_mobility_database_then_transitland():
    rows = [
        row("tl:f-a:static", kind="static", scheduled="https://a.org/g.zip"),
        row("md:mdb-1:static", kind="static", scheduled="https://a.org/g.zip"),
    ]
    assert build_feeds(rows, {}, {})[0].name == "md:mdb-1:static"
    rows.append(row("curated:a:static", kind="static", scheduled="https://a.org/g.zip"))
    assert build_feeds(rows, {}, {})[0].name == "curated:a:static"


def test_coordinates_come_from_any_placed_member():
    place = Place(country_code="US", latitude=1.0, longitude=2.0)
    rows = [
        row("tl:f-a:static", kind="static", scheduled="https://a.org/g.zip"),
        row("md:mdb-1:rt", vehicles="https://a.org/rt/vehicles").with_place(place),
    ]
    rows.append(row("tl:f-a:rt", vehicles="https://a.org/rt/alerts"))
    assert build_feeds(rows, {}, {})[0].place == place


def test_role_state_is_usable_when_any_url_answers_and_overall_needs_all():
    rows = [
        row("tl:f-a:static", kind="static", scheduled="https://a.org/old.zip"),
        row("tl:f-a:rt", vehicles="https://a.org/rt/vehicles"),
        Source(
            source_id="md:mdb-1:static",
            catalog="mobilitydatabase",
            kind="static",
            feed_id="mdb-1",
            name="A",
            urls={"scheduled": "https://a.org/new.zip"},
        ),
        Source(
            source_id="md:mdb-2:rt",
            catalog="mobilitydatabase",
            kind="rt",
            feed_id="mdb-2",
            name="A",
            urls={"vehicles": "https://a.org/rt/vehicles"},
            feed_references=("mdb-1",),
        ),
    ]
    results = {
        "https://a.org/old.zip": failed("https://a.org/old.zip"),
        "https://a.org/new.zip": ok(
            "https://a.org/new.zip", content_length=2048, last_modified=NOW
        ),
        "https://a.org/rt/vehicles": ok("https://a.org/rt/vehicles"),
    }
    (feed,) = build_feeds(rows, results, {})
    assert feed.role_state == {"scheduled": UP, "vehicles": UP}
    assert feed.state == DOWN
    # The size comes from the schedule that answers, not the dead one.
    assert feed.static_bytes == 2048
    assert feed.last_modified == NOW


def test_a_feed_with_nothing_checked_is_unknown():
    (feed,) = build_feeds([row("curated:a:rt", vehicles="/amtrak/vp.pb")], {}, {})
    assert feed.state == UNKNOWN
    assert feed.role_state == {"vehicles": UNKNOWN}


def test_ids_are_minted_deterministically_and_reused_by_overlap():
    groups = [["a", "b"], ["c"]]
    first = assign_ids(groups, {})
    assert first == assign_ids(groups, {})
    assert all(feed_id.startswith("f-") for feed_id in first)

    existing = {first[0]: {"a", "b"}, first[1]: {"c"}}
    # "c" joins a, b: the bigger overlap keeps its id, "c"'s id is left free.
    assert assign_ids([["a", "b", "c"]], existing) == [first[0]]


def test_a_split_feed_keeps_its_id_on_the_larger_part():
    existing = {"f-old": {"a", "b", "c"}}
    result = assign_ids([["a"], ["b", "c"]], existing)
    assert result[1] == "f-old"
    assert result[0] != "f-old"


def _history(rows, results):
    record(rows, fold(rows, results), results, NOW)


def test_a_feed_id_survives_a_member_being_added(session_factory):
    rt = [
        row("md:mdb-1:rt", vehicles="https://rt.example.org/x/vehicles"),
        row("md:mdb-2:rt", trip_updates="https://rt.example.org/x/trips"),
    ]
    _history(rt, {})
    (before,) = build_feeds(rt, {}, load_existing())
    persist([before])

    grown = [*rt, row("md:mdb-3:rt", alerts="https://rt.example.org/x/alerts")]
    _history(grown, {})
    (after,) = build_feeds(grown, {}, load_existing())
    persist([after])

    assert after.feed_id == before.feed_id
    with session_factory() as session:
        members = session.scalars(select(FeedMember.source_id)).all()
        assert sorted(members) == ["md:mdb-1:rt", "md:mdb-2:rt", "md:mdb-3:rt"]


def test_a_feed_that_loses_its_group_is_kept_absent_and_returns(session_factory):
    a = [row("tl:f-a:static", kind="static", scheduled="https://a.org/g.zip")]
    b = [row("tl:f-b:static", kind="static", scheduled="https://b.org/g.zip")]

    _history(a + b, {})
    first = build_feeds(a + b, {}, load_existing())
    persist(first)
    a_id = next(f.feed_id for f in first if f.members == ("tl:f-a:static",))

    _history(b, {})
    persist(build_feeds(b, {}, load_existing()))
    with session_factory() as session:
        assert session.get(FeedRecord, a_id).present is False

    _history(a + b, {})
    again = build_feeds(a + b, {}, load_existing())
    persist(again)
    assert a_id in {f.feed_id for f in again}
    with session_factory() as session:
        assert session.get(FeedRecord, a_id).present is True
