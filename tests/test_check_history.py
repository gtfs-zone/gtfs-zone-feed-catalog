"""The fold into one answer per source, and the one-row-per-change storage."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from geometry_car.assets.check_history import DOWN, UNKNOWN, UP, fold, record
from geometry_car.assets.endpoint_checks import CheckResult
from geometry_car.assets.history_retention import prune
from geometry_car.catalog import Source
from geometry_car.history.models import CheckRun, SourceRecord, SourceState
from geometry_car.settings import settings

NOW = datetime(2026, 9, 1, tzinfo=UTC)


def result(url: str, *, ok: bool = True, status: int = 200) -> CheckResult:
    return CheckResult(
        url=url,
        ok=ok,
        checked_at=NOW,
        status_code=status,
        latency_ms=10,
        error_class="" if ok else f"http_{status // 100}xx",
    )


def rt_row(**urls: str) -> Source:
    return Source(
        source_id="tl:f-a:rt",
        catalog="transitland",
        kind="rt",
        feed_id="f-a",
        name="A",
        urls=urls,
    )


def test_an_rt_row_is_up_only_when_every_endpoint_answers():
    row = rt_row(
        vehicles="https://example.org/vp.pb", alerts="https://example.org/a.pb"
    )
    results = {
        "https://example.org/vp.pb": result("https://example.org/vp.pb"),
        "https://example.org/a.pb": result(
            "https://example.org/a.pb", ok=False, status=503
        ),
    }
    status = fold([row], results)["tl:f-a:rt"]
    assert status.state == DOWN
    assert status.error_class == "http_5xx"


def test_a_row_with_nothing_checkable_is_unknown_not_down():
    row = rt_row(vehicles="/amtrak/vehicle_positions.pb")
    assert fold([row], {})["tl:f-a:rt"].state == UNKNOWN


def test_a_state_change_writes_one_row_and_a_steady_state_writes_none(session_factory):
    row = rt_row(vehicles="https://example.org/vp.pb")
    up = {"https://example.org/vp.pb": result("https://example.org/vp.pb")}

    record([row], fold([row], up), NOW)
    record([row], fold([row], up), NOW)

    with session_factory() as session:
        states = session.scalars(select(SourceState)).all()
        # Two runs, one transition: unknown -> up.
        assert len(states) == 1
        assert states[0].state == UP
        assert len(session.scalars(select(CheckRun)).all()) == 2
        assert session.get(SourceRecord, "tl:f-a:rt").consecutive_failures == 0


def test_going_down_and_back_writes_two_more_rows_and_counts_failures(session_factory):
    row = rt_row(vehicles="https://example.org/vp.pb")
    up = {"https://example.org/vp.pb": result("https://example.org/vp.pb")}
    down = {
        "https://example.org/vp.pb": result(
            "https://example.org/vp.pb", ok=False, status=500
        )
    }

    record([row], fold([row], up), NOW)
    record([row], fold([row], down), NOW)
    statuses = record([row], fold([row], down), NOW)
    assert statuses["tl:f-a:rt"].consecutive_failures == 2
    record([row], fold([row], up), NOW)

    with session_factory() as session:
        states = [s.state for s in session.scalars(select(SourceState)).all()]
        assert states == [UP, DOWN, UP]
        assert session.get(SourceRecord, "tl:f-a:rt").consecutive_failures == 0


def test_a_source_the_catalogs_drop_stops_being_present(session_factory):
    row = rt_row(vehicles="https://example.org/vp.pb")
    record([row], fold([row], {}), NOW)
    record([], {}, NOW)

    with session_factory() as session:
        assert session.get(SourceRecord, "tl:f-a:rt").present is False


def test_retention_deletes_old_transitions_and_absent_sources_only(session_factory):
    now = datetime.now(UTC)
    with session_factory() as session, session.begin():
        session.add_all(
            [
                SourceRecord(
                    source_id="keep:present",
                    catalog="transitland",
                    kind="static",
                    present=True,
                    first_seen=now - timedelta(days=5000),
                    last_seen_in_catalog=now - timedelta(days=5000),
                ),
                SourceRecord(
                    source_id="drop:absent",
                    catalog="transitland",
                    kind="static",
                    present=False,
                    first_seen=now - timedelta(days=200),
                    last_seen_in_catalog=now - timedelta(days=200),
                ),
                SourceRecord(
                    source_id="keep:recently-absent",
                    catalog="transitland",
                    kind="static",
                    present=False,
                    first_seen=now - timedelta(days=10),
                    last_seen_in_catalog=now - timedelta(days=10),
                ),
            ]
        )
        session.flush()
        session.add_all(
            [
                SourceState(
                    source_id="keep:present",
                    state=UP,
                    changed_at=now - timedelta(days=500),
                ),
                SourceState(
                    source_id="keep:present",
                    state=DOWN,
                    changed_at=now - timedelta(days=10),
                ),
            ]
        )

    assert prune() == (1, 1)

    with session_factory() as session:
        assert {r.source_id for r in session.scalars(select(SourceRecord)).all()} == {
            "keep:present",
            "keep:recently-absent",
        }
        remaining = session.scalars(select(SourceState)).all()
        # A row older than the window goes; a present source keeps its own row
        # however old the source is.
        assert [s.state for s in remaining] == [DOWN]


def test_retention_windows_come_from_settings(session_factory, monkeypatch):
    monkeypatch.setattr(settings, "state_retention_days", 1)
    now = datetime.now(UTC)
    with session_factory() as session, session.begin():
        session.add(
            SourceRecord(
                source_id="s",
                catalog="transitland",
                kind="static",
                first_seen=now,
                last_seen_in_catalog=now,
            )
        )
        session.flush()
        session.add(
            SourceState(source_id="s", state=UP, changed_at=now - timedelta(days=2))
        )
    assert prune() == (1, 0)
