"""The Alembic chain, run for real against a SQLite file."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parent.parent
T1 = datetime(2026, 9, 20, tzinfo=UTC)
T2 = datetime(2026, 9, 22, tzinfo=UTC)
T3 = datetime(2026, 9, 23, tzinfo=UTC)


@pytest.fixture
def alembic(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'history.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "src/geometry_car/alembic"))
    return config, create_engine(url)


def _source(conn, source_id, url, *, state, last_checked, failures=0, seen=T3):
    conn.execute(
        text(
            "insert into source (source_id, catalog, kind, name, download_url,"
            " first_seen, last_seen_in_catalog, present, last_checked, state,"
            " consecutive_failures) values (:id, 'transitland', 'static', '',"
            " :url, :t1, :seen, 1, :checked, :state, :failures)"
        ),
        {
            "id": source_id,
            "url": url,
            "t1": T1,
            "seen": seen,
            "checked": last_checked,
            "state": state,
            "failures": failures,
        },
    )


def _state(conn, source_id, state, at, code=None, error=""):
    conn.execute(
        text(
            "insert into source_state (source_id, state, changed_at, status_code,"
            " error_class) values (:id, :state, :at, :code, :error)"
        ),
        {"id": source_id, "state": state, "at": at, "code": code, "error": error},
    )


def test_source_state_moves_onto_endpoints(alembic):
    config, engine = alembic
    command.upgrade(config, "40f107be9c26")
    with engine.begin() as conn:
        # Two catalogs, one URL (differently spelled): the more recently
        # checked row's history wins.
        _source(conn, "tl:a:static", "https://A.org/g.zip", state="up", last_checked=T2)
        _source(
            conn,
            "md:mdb-1:static",
            "https://a.org:443/g.zip",
            state="down",
            last_checked=T3,
            failures=2,
        )
        _source(conn, "tl:rel:rt", "/amtrak/vp.pb", state="unknown", last_checked=None)
        _state(conn, "tl:a:static", "up", T1, 200)
        _state(conn, "md:mdb-1:static", "up", T1, 200)
        _state(conn, "md:mdb-1:static", "down", T3, 500, "http_5xx")
    # 7c2e5a1d9b30 widens a column, which SQLite cannot ALTER; its effect is
    # irrelevant here.
    command.stamp(config, "7c2e5a1d9b30")

    command.upgrade(config, "head")

    with engine.connect() as conn:
        endpoints = conn.execute(
            text("select url, state, consecutive_failures, status_code from endpoint")
        ).all()
        assert endpoints == [("https://a.org/g.zip", "down", 2, 500)]
        states = conn.execute(
            text("select url, state from endpoint_state order by changed_at")
        ).all()
        assert states == [
            ("https://a.org/g.zip", "up"),
            ("https://a.org/g.zip", "down"),
        ]

    tables = inspect(engine).get_table_names()
    assert "source_state" not in tables
    assert {
        "endpoint",
        "endpoint_state",
        "source_endpoint",
        "feed",
        "feed_member",
    } <= set(tables)
    columns = {c["name"] for c in inspect(engine).get_columns("source")}
    assert not {"state", "last_checked", "consecutive_failures"} & columns


def test_downgrade_puts_state_back_on_sources(alembic):
    config, engine = alembic
    command.upgrade(config, "40f107be9c26")
    with engine.begin() as conn:
        _source(conn, "tl:a:static", "https://a.org/g.zip", state="up", last_checked=T2)
        _state(conn, "tl:a:static", "up", T1, 200)
    command.stamp(config, "7c2e5a1d9b30")
    command.upgrade(config, "head")
    command.downgrade(config, "7c2e5a1d9b30")

    with engine.connect() as conn:
        assert conn.execute(text("select state from source")).scalar_one() == "up"
        assert conn.execute(text("select count(*) from source_state")).scalar_one() == 1
    assert "endpoint" not in inspect(engine).get_table_names()
