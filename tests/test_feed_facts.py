"""Facts of the stored zips: computed once per sha256, kept beside the zip."""

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime

import pytest
from gtfs_zone_feed_fetch.facts import FACTS_VERSION
from gtfs_zone_feed_fetch.pool import WorkerError
from gtfs_zone_feed_fetch.store import Stored, facts_key, index_body

from gtfs_zone_feed_catalog import facts_worker
from gtfs_zone_feed_catalog.assets import feed_facts as module
from gtfs_zone_feed_catalog.settings import settings

CHECKED = "2026-11-19T09:10:00+00:00"
SINCE = "2026-11-01T09:10:00+00:00"
FEED = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
    "a,Bay Transit,https://bay.example,America/Los_Angeles\n",
    "routes.txt": "route_id,agency_id,route_short_name,route_type\nr,a,1,3\n",
    "trips.txt": "route_id,service_id,trip_id\nr,s,t1\nr,s,t2\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
    "x,X,37.77,-122.42\ny,Y,37.80,-122.27\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "t1,08:00:00,08:00:00,x,1\nt1,08:30:00,08:30:00,y,2\n"
    "t2,09:00:00,09:00:00,x,1\n",
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,"
    "saturday,sunday,start_date,end_date\ns,1,1,1,1,1,0,0,20261101,20270301\n",
}


def make_zip(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in files.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


@pytest.fixture(autouse=True)
def workdir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "fetch_workdir", str(tmp_path))


def put_index(store, urls: dict[str, bytes | str]) -> None:
    """Store each zip body and index it; a string is a stored non-zip kind."""
    entries = {}
    for url, body in urls.items():
        if isinstance(body, str):
            digest = sha(body.encode())
            key = f"gtfs/{digest}.body"
            store.put(key, body.encode(), content_type="text/html")
            kind = body
        else:
            digest = sha(body)
            key = f"gtfs/{digest}.zip"
            store.put(key, body)
            kind = "zip"
        entries[url] = Stored(
            url=f"https://{url}",
            checked=CHECKED,
            outcome="ok",
            status=200,
            key=key,
            sha256=digest,
            bytes=len(body),
            kind=kind,
            fetched=CHECKED,
            since=SINCE,
        )
    store.put("gtfs/index.json", index_body(entries, datetime.now(UTC), 100))


def test_a_zip_gets_facts_once(store, monkeypatch):
    body = make_zip(FEED)
    put_index(store, {"bay.example/gtfs.zip": body, "mirror.example/gtfs.zip": body})

    contents = module.facts(store)

    content = contents["bay.example/gtfs.zip"]
    assert content.outcome == "ok"
    assert content.sha256 == sha(body)
    assert content.size == len(body)
    assert content.checked.isoformat() == "2026-11-19"
    assert content.since.isoformat() == "2026-11-01"
    assert content.facts["counts"] == {"routes": 1, "stops": 2, "trips": 2}
    assert content.facts["service"] == {"start": "2026-11-01", "end": "2027-03-01"}
    assert content.facts["placeTags"]["regions"] == ["California"]
    assert "sha256" not in content.facts and "bytes" not in content.facts
    assert contents["mirror.example/gtfs.zip"].facts == content.facts

    doc = json.loads(store.get(facts_key(sha(body))))
    assert doc["version"] == FACTS_VERSION
    assert doc["outcome"] == "ok"

    def fail(*_args):
        raise AssertionError("facts computed twice")

    monkeypatch.setattr(module, "scan_stored", fail)
    assert module.facts(store)["bay.example/gtfs.zip"].facts == content.facts


def test_an_older_facts_version_is_computed_again(store):
    body = make_zip(FEED)
    put_index(store, {"bay.example/gtfs.zip": body})
    old = facts_worker.facts_doc("ok", facts={"stale": True})
    store.put(facts_key(sha(body)), json.dumps(old | {"version": 0}).encode())

    contents = module.facts(store)

    assert "stale" not in contents["bay.example/gtfs.zip"].facts
    assert json.loads(store.get(facts_key(sha(body))))["version"] == FACTS_VERSION


def test_unreadable_zips_and_other_bodies_are_outcomes(store):
    missing = make_zip({k: v for k, v in FEED.items() if k != "stop_times.txt"})
    broken = b"PK\x03\x04" + b"not really a zip" * 10
    put_index(
        store,
        {
            "missing.example/gtfs.zip": missing,
            "broken.example/gtfs.zip": broken,
            "html.example/gtfs.zip": "html",
        },
    )

    contents = module.facts(store)

    assert contents["missing.example/gtfs.zip"].outcome == "missing_files"
    assert "stop_times" in contents["missing.example/gtfs.zip"].detail
    assert contents["broken.example/gtfs.zip"].outcome == "not_zip"
    html = contents["html.example/gtfs.zip"]
    assert (html.outcome, html.detail) == ("not_zip", "html")


def test_a_worker_over_its_memory_is_stored_a_passing_fault_is_not():
    memory = module.result_doc("a", WorkerError("MemoryError: over"))
    assert memory["outcome"] == "memory"
    assert module.result_doc("b", WorkerError("OSError: disk full")) is None


def test_a_failed_copy_is_tried_again_next_run(store, monkeypatch):
    body = make_zip(FEED)
    put_index(store, {"bay.example/gtfs.zip": body})

    def broken(*_args):
        raise OSError("bucket down")

    monkeypatch.setattr(module, "scan_stored", broken)
    assert module.facts(store) == {}
    assert facts_key(sha(body)) not in store.list_prefix("gtfs/")


def test_a_url_with_no_stored_body_is_left_out(store):
    body = make_zip(FEED)
    put_index(store, {"bay.example/gtfs.zip": body})
    store.delete(f"gtfs/{sha(body)}.zip")
    assert module.facts(store) == {}
