"""The fold into one answer per source, and the one-row-per-change storage."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from geometry_car.assets.check_history import DOWN, UNKNOWN, UP, fold, record
from geometry_car.assets.endpoint_checks import CheckResult
from geometry_car.assets.history_retention import prune
from geometry_car.catalog import Source
from geometry_car.history.models import (
    CheckRun,
    EndpointRecord,
    EndpointState,
    FeedRecord,
    SourceEndpoint,
    SourceRecord,
)
from geometry_car.settings import settings

NOW = datetime(2026, 9, 1, tzinfo=UTC)
VP = "https://example.org/vp.pb"
ALERTS = "https://example.org/a.pb"


def result(url: str, *, ok: bool = True, status: int = 200, **facts) -> CheckResult:
    return CheckResult(
        url=url,
        ok=ok,
        checked_at=NOW,
        status_code=status,
        latency_ms=10,
        error_class="" if ok else f"http_{status // 100}xx",
        **facts,
    )


def rt_row(source_id: str = "tl:f-a:rt", **urls: str) -> Source:
    return Source(
        source_id=source_id,
        catalog="transitland",
        kind="rt",
        feed_id="f-a",
        name="A",
        urls=urls,
    )


def run(rows: list[Source], results: dict[str, CheckResult]):
    return record(rows, fold(rows, results), results, NOW)


def test_an_rt_row_is_up_only_when_every_endpoint_answers():
    row = rt_row(vehicles=VP, alerts=ALERTS)
    results = {VP: result(VP), ALERTS: result(ALERTS, ok=False, status=503)}
    status = fold([row], results)["tl:f-a:rt"]
    assert status.state == DOWN
    assert status.error_class == "http_5xx"


def test_a_row_with_nothing_checkable_is_unknown_not_down():
    row = rt_row(vehicles="/amtrak/vehicle_positions.pb")
    assert fold([row], {})["tl:f-a:rt"].state == UNKNOWN


def test_a_static_row_carries_its_schedule_size_and_last_modified():
    url = "https://example.org/gtfs.zip"
    row = Source(
        source_id="tl:f-a:static",
        catalog="transitland",
        kind="static",
        feed_id="f-a",
        name="A",
        urls={"scheduled": url},
    )
    results = {url: result(url, content_length=4096, last_modified=NOW)}
    status = fold([row], results)["tl:f-a:static"]
    assert status.content_length == 4096
    assert status.last_modified == NOW


def test_a_state_change_writes_one_row_and_a_steady_state_writes_none(session_factory):
    row = rt_row(vehicles=VP)
    up = {VP: result(VP)}

    run([row], up)
    run([row], up)

    with session_factory() as session:
        states = session.scalars(select(EndpointState)).all()
        # Two runs, one transition: unknown -> up.
        assert len(states) == 1
        assert states[0].state == UP
        assert states[0].url == VP
        assert len(session.scalars(select(CheckRun)).all()) == 2
        assert session.get(EndpointRecord, VP).consecutive_failures == 0


def test_going_down_and_back_writes_two_more_rows_and_counts_failures(session_factory):
    row = rt_row(vehicles=VP)
    up = {VP: result(VP)}
    down = {VP: result(VP, ok=False, status=500)}

    run([row], up)
    run([row], down)
    statuses = run([row], down)
    assert statuses["tl:f-a:rt"].consecutive_failures == 2
    run([row], up)

    with session_factory() as session:
        states = [s.state for s in session.scalars(select(EndpointState)).all()]
        assert states == [UP, DOWN, UP]
        assert session.get(EndpointRecord, VP).consecutive_failures == 0


def test_a_source_down_the_first_time_it_is_seen_counts_one_failure(session_factory):
    row = rt_row(vehicles=VP)
    statuses = run([row], {VP: result(VP, ok=False, status=500)})
    assert statuses["tl:f-a:rt"].consecutive_failures == 1
    assert statuses["tl:f-a:rt"].since is not None


def test_two_rows_sharing_a_url_share_one_history(session_factory):
    a = rt_row("tl:f-a:rt", vehicles=VP)
    b = rt_row("md:mdb-1:rt", vehicles=VP)
    run([a, b], {VP: result(VP)})
    run([a, b], {VP: result(VP, ok=False, status=500)})

    with session_factory() as session:
        assert [s.state for s in session.scalars(select(EndpointState)).all()] == [
            UP,
            DOWN,
        ]
        links = session.scalars(select(SourceEndpoint)).all()
        assert {(link.source_id, link.url) for link in links} == {
            ("tl:f-a:rt", VP),
            ("md:mdb-1:rt", VP),
        }


def test_check_facts_land_on_the_endpoint(session_factory):
    url = "https://example.org/gtfs.zip"
    row = Source(
        source_id="tl:f-a:static",
        catalog="transitland",
        kind="static",
        feed_id="f-a",
        name="A",
        urls={"scheduled": url},
    )
    run(
        [row],
        {
            url: result(
                url,
                content_type="application/zip",
                content_length=4096,
                last_modified=NOW,
                etag='"abc"',
            )
        },
    )
    with session_factory() as session:
        endpoint = session.get(EndpointRecord, url)
        assert endpoint.content_type == "application/zip"
        assert endpoint.content_length == 4096
        assert endpoint.etag == '"abc"'
        assert endpoint.last_modified.replace(tzinfo=UTC) == NOW


def test_an_rt_row_down_since_its_earliest_failing_url(session_factory):
    row = rt_row(vehicles=VP, alerts=ALERTS)
    down_vp = {VP: result(VP, ok=False, status=500), ALERTS: result(ALERTS)}
    first = run([row], down_vp)["tl:f-a:rt"]
    both_down = {
        VP: result(VP, ok=False, status=500),
        ALERTS: result(ALERTS, ok=False, status=500),
    }
    second = run([row], both_down)["tl:f-a:rt"]
    assert second.since == first.since
    assert second.consecutive_failures == 2


def test_a_source_the_catalogs_drop_stops_being_present(session_factory):
    row = rt_row(vehicles=VP)
    run([row], {})
    record([], {}, {}, NOW)

    with session_factory() as session:
        assert session.get(SourceRecord, "tl:f-a:rt").present is False


def _source(session, source_id: str, *, present: bool, age_days: int, now: datetime):
    session.add(
        SourceRecord(
            source_id=source_id,
            catalog="transitland",
            kind="static",
            present=present,
            first_seen=now - timedelta(days=age_days),
            last_seen_in_catalog=now - timedelta(days=age_days),
        )
    )


def _endpoint(session, url: str, *, age_days: int, now: datetime):
    session.add(
        EndpointRecord(
            url=url,
            first_seen=now - timedelta(days=age_days),
            last_seen=now - timedelta(days=age_days),
            state=UP,
            consecutive_failures=0,
        )
    )


def test_retention_deletes_old_rows_and_absent_things_only(session_factory):
    now = datetime.now(UTC)
    with session_factory() as session, session.begin():
        _source(session, "keep:present", present=True, age_days=5000, now=now)
        _source(session, "drop:absent", present=False, age_days=200, now=now)
        _source(session, "keep:recently-absent", present=False, age_days=10, now=now)
        _endpoint(session, "https://keep.example.org", age_days=0, now=now)
        _endpoint(session, "https://drop.example.org", age_days=200, now=now)
        session.add_all(
            [
                FeedRecord(
                    feed_id="f-keep", present=True, last_seen=now - timedelta(days=500)
                ),
                FeedRecord(
                    feed_id="f-drop", present=False, last_seen=now - timedelta(days=200)
                ),
            ]
        )
        session.flush()
        session.add_all(
            [
                EndpointState(
                    url="https://keep.example.org",
                    state=UP,
                    changed_at=now - timedelta(days=500),
                ),
                EndpointState(
                    url="https://keep.example.org",
                    state=DOWN,
                    changed_at=now - timedelta(days=10),
                ),
                EndpointState(
                    url="https://drop.example.org",
                    state=UP,
                    changed_at=now - timedelta(days=20),
                ),
            ]
        )

    # The dropped endpoint's recent row goes with it, by cascade, not by age.
    assert prune() == {"endpoint_state": 1, "source": 1, "endpoint": 1, "feed": 1}

    with session_factory() as session:
        assert {r.source_id for r in session.scalars(select(SourceRecord)).all()} == {
            "keep:present",
            "keep:recently-absent",
        }
        remaining = session.scalars(select(EndpointState)).all()
        assert [(s.url, s.state) for s in remaining] == [
            ("https://keep.example.org", DOWN)
        ]
        assert [f.feed_id for f in session.scalars(select(FeedRecord)).all()] == [
            "f-keep"
        ]


def test_retention_windows_come_from_settings(session_factory, monkeypatch):
    monkeypatch.setattr(settings, "state_retention_days", 1)
    now = datetime.now(UTC)
    with session_factory() as session, session.begin():
        _endpoint(session, "https://example.org", age_days=0, now=now)
        session.flush()
        session.add(
            EndpointState(
                url="https://example.org", state=UP, changed_at=now - timedelta(days=2)
            )
        )
    assert prune()["endpoint_state"] == 1
