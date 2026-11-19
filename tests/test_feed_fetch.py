"""The schedule fetch: conditional GETs, the bucket, the index and orphans."""

import hashlib
import json

import httpx
import pytest
import respx
from gtfs_zone_feed_fetch.store import INDEX_KEY, PREFIX, facts_key

from gtfs_zone_feed_catalog.assets.endpoint_checks import (
    EndpointChecksConfig,
    endpoint_checks,
    result_key,
)
from gtfs_zone_feed_catalog.assets.feed_fetch import fetch
from gtfs_zone_feed_catalog.catalog import Source
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.zips import read_index

A = "https://a.example.org/gtfs.zip"
B = "https://b.example.org/gtfs.zip"
ZIP = b"PK\x03\x04" + b"stops" * 100
ZIP2 = b"PK\x03\x04" + b"trips" * 100
LAST_MODIFIED = "Mon, 16 Nov 2026 08:00:00 GMT"


@pytest.fixture(autouse=True)
def fetch_settings(impatient, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "fetch_workdir", str(tmp_path))
    monkeypatch.setattr(settings, "fetch_max_bytes", 10_000)
    monkeypatch.setattr(settings, "fetch_body_max_bytes", 1_000)


def static(source_id: str, url: str) -> Source:
    return Source(
        source_id=source_id,
        catalog="transitland",
        kind="static",
        feed_id=source_id,
        name=source_id,
        urls={"scheduled": url},
    )


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def objects(store) -> set[str]:
    return set(store.list_prefix(PREFIX)) - {INDEX_KEY}


@respx.mock
def test_a_zip_is_stored_by_sha256_and_indexed(store):
    respx.get(A).respond(
        200,
        content=ZIP,
        headers={"etag": '"v1"', "last-modified": LAST_MODIFIED},
    )

    checks = fetch(store, [static("a", A)])

    check = checks[result_key(A)]
    assert check.ok
    assert check.method == "GET"
    assert check.sha256 == sha(ZIP)
    assert check.content_length == len(ZIP)
    assert check.etag == '"v1"'
    assert store.get(f"{PREFIX}{sha(ZIP)}.zip") == ZIP

    entry = read_index(store)[result_key(A)]
    assert entry.outcome == "ok"
    assert entry.key == f"{PREFIX}{sha(ZIP)}.zip"
    assert entry.kind == "zip"
    assert entry.last_modified == LAST_MODIFIED
    assert entry.since == entry.fetched
    raw = json.loads(store.get(INDEX_KEY))
    assert raw["urls"][result_key(A)]["lastModified"] == LAST_MODIFIED


@respx.mock
def test_an_unchanged_schedule_is_a_304_and_stores_nothing(store):
    route = respx.get(A)
    route.respond(200, content=ZIP, headers={"etag": '"v1"'})
    fetch(store, [static("a", A)])
    first = read_index(store)[result_key(A)]

    route.respond(304)
    checks = fetch(store, [static("a", A)])

    assert route.calls[-1].request.headers["If-None-Match"] == '"v1"'
    check = checks[result_key(A)]
    assert check.ok
    assert check.sha256 == sha(ZIP)
    assert check.content_length == len(ZIP)
    entry = read_index(store)[result_key(A)]
    assert entry.outcome == "not_modified"
    assert entry.key == first.key
    assert entry.fetched == first.fetched
    assert objects(store) == {first.key}


@respx.mock
def test_a_changed_schedule_replaces_the_object_and_deletes_the_orphan(store):
    route = respx.get(A)
    route.respond(200, content=ZIP)
    fetch(store, [static("a", A)])
    first = read_index(store)[result_key(A)]

    route.respond(200, content=ZIP2)
    fetch(store, [static("a", A)])

    entry = read_index(store)[result_key(A)]
    assert entry.sha256 == sha(ZIP2)
    assert entry.since != first.since
    assert objects(store) == {f"{PREFIX}{sha(ZIP2)}.zip"}


@respx.mock
def test_facts_are_kept_with_their_zip_and_deleted_with_it(store):
    route = respx.get(A)
    route.respond(200, content=ZIP)
    fetch(store, [static("a", A)])
    store.put(facts_key(sha(ZIP)), b"{}")

    fetch(store, [static("a", A)])
    assert facts_key(sha(ZIP)) in objects(store)

    route.respond(200, content=ZIP2)
    fetch(store, [static("a", A)])
    assert objects(store) == {f"{PREFIX}{sha(ZIP2)}.zip"}


@respx.mock
def test_two_urls_with_the_same_bytes_share_one_object(store):
    respx.get(A).respond(200, content=ZIP)
    respx.get(B).respond(200, content=ZIP)

    fetch(store, [static("a", A), static("b", B)])

    index = read_index(store)
    assert index[result_key(A)].key == index[result_key(B)].key
    assert objects(store) == {f"{PREFIX}{sha(ZIP)}.zip"}


@respx.mock
def test_a_url_the_catalog_dropped_loses_its_object(store):
    respx.get(A).respond(200, content=ZIP)
    respx.get(B).respond(200, content=ZIP2)
    fetch(store, [static("a", A), static("b", B)])

    fetch(store, [static("a", A)])

    assert set(read_index(store)) == {result_key(A)}
    assert objects(store) == {f"{PREFIX}{sha(ZIP)}.zip"}


@respx.mock
def test_a_failed_request_keeps_the_last_body(store):
    route = respx.get(A)
    route.respond(200, content=ZIP)
    fetch(store, [static("a", A)])

    route.respond(503)
    check = fetch(store, [static("a", A)])[result_key(A)]

    assert not check.ok
    assert check.error_class == "http_5xx"
    assert check.sha256 == ""
    entry = read_index(store)[result_key(A)]
    assert entry.outcome == "http_error"
    assert entry.sha256 == sha(ZIP)
    assert objects(store) == {entry.key}


@respx.mock
def test_a_connection_error_is_a_failed_check(store):
    respx.get(A).mock(side_effect=httpx.ConnectError("connection refused"))

    check = fetch(store, [static("a", A)])[result_key(A)]

    assert not check.ok
    assert check.error_class == "refused"
    assert read_index(store)[result_key(A)].outcome == "error"


@respx.mock
def test_a_small_html_body_is_kept_beside_the_zips(store):
    page = b"<!doctype html><html>moved</html>"
    respx.get(A).respond(200, content=page, headers={"content-type": "text/html"})

    fetch(store, [static("a", A)])

    entry = read_index(store)[result_key(A)]
    assert entry.kind == "html"
    assert entry.key == f"{PREFIX}{sha(page)}.body"
    assert store.get(entry.key) == page


@respx.mock
def test_a_body_over_the_cap_is_abandoned_but_answers_the_check(store):
    respx.get(A).respond(200, content=b"PK\x03\x04" + b"x" * 20_000)

    check = fetch(store, [static("a", A)])[result_key(A)]

    assert check.ok
    assert check.sha256 == ""
    entry = read_index(store)[result_key(A)]
    assert entry.outcome == "too_large"
    assert entry.key == ""
    assert objects(store) == set()


@respx.mock
def test_a_body_without_a_length_is_cut_off_at_the_cap(store):
    async def chunks():
        yield b"PK\x03\x04"
        for _ in range(30):
            yield b"x" * 1_000

    respx.get(A).mock(return_value=httpx.Response(200, content=chunks()))

    check = fetch(store, [static("a", A)])[result_key(A)]

    assert check.ok
    assert read_index(store)[result_key(A)].outcome == "too_large"
    assert objects(store) == set()


@respx.mock
def test_no_validators_are_sent_when_the_object_is_gone(store):
    route = respx.get(A)
    route.respond(200, content=ZIP, headers={"etag": '"v1"'})
    fetch(store, [static("a", A)])
    store.delete(read_index(store)[result_key(A)].key)

    fetch(store, [static("a", A)])

    assert "If-None-Match" not in route.calls[-1].request.headers
    assert objects(store) == {f"{PREFIX}{sha(ZIP)}.zip"}


@respx.mock
def test_realtime_and_keyed_urls_are_not_fetched(store):
    rt = Source(
        source_id="rt",
        catalog="transitland",
        kind="rt",
        feed_id="rt",
        name="rt",
        urls={"vehicles": "https://a.example.org/vp.pb"},
    )
    keyed = Source(
        source_id="keyed",
        catalog="transitland",
        kind="static",
        feed_id="keyed",
        name="keyed",
        urls={"scheduled": B},
        authentication_type=1,
    )

    assert fetch(store, [rt, keyed]) == {}


@respx.mock
def test_endpoint_checks_do_not_request_fetched_urls_again(store):
    respx.get(A).respond(200, content=ZIP)
    head_a = respx.head(A).respond(200)
    head_b = respx.head(B).respond(200)
    sources = [static("a", A), static("b", B)]
    fetched = fetch(store, [static("a", A)])

    results = endpoint_checks(
        config=EndpointChecksConfig(), sources=sources, feed_fetch=fetched
    )

    assert not head_a.called
    assert head_b.called
    assert results[result_key(A)].sha256 == sha(ZIP)
    assert results[result_key(B)].method == "HEAD"


@respx.mock
def test_a_failed_upload_loses_that_body_not_the_run(store, monkeypatch):
    async def broken(*_args, **_kwargs):
        raise NotImplementedError("no attributes here")

    monkeypatch.setattr("obstore.put_async", broken)
    respx.get(A).respond(200, content=ZIP)

    check = fetch(store, [static("a", A)])[result_key(A)]

    assert check.ok
    entry = read_index(store)[result_key(A)]
    assert entry.sha256 == sha(ZIP)
    assert entry.key == ""
