"""The checker: HEAD, the GET fallback, redirects and what is not checked."""

import asyncio

import httpx
import respx

from geometry_car.assets.curated_examples import example_sources, load_examples
from geometry_car.assets.endpoint_checks import (
    check_targets,
    result_key,
    run_checks,
)
from geometry_car.catalog import Source


def check(url: str) -> dict:
    return asyncio.run(run_checks({result_key(url): url}))


@respx.mock
def test_head_that_answers_is_the_whole_check(impatient):
    route = respx.head("https://example.org/feed.zip").respond(
        200, headers={"content-type": "application/zip", "content-length": "1234"}
    )
    result = check("https://example.org/feed.zip")[
        result_key("https://example.org/feed.zip")
    ]
    assert route.called
    assert result.ok
    assert result.method == "HEAD"
    assert result.content_type == "application/zip"
    assert result.content_length == 1234
    assert result.final_url == ""


@respx.mock
def test_head_rejected_falls_back_to_a_ranged_get(impatient):
    # A 405 means "this origin does not do HEAD", not "this feed is gone".
    respx.head("https://example.org/feed.zip").respond(405)
    get = respx.get("https://example.org/feed.zip").respond(
        206, headers={"content-type": "application/zip"}
    )
    result = check("https://example.org/feed.zip")[
        result_key("https://example.org/feed.zip")
    ]
    assert get.called
    assert get.calls[0].request.headers["Range"] == "bytes=0-0"
    assert result.ok
    assert result.method == "GET"


@respx.mock
def test_a_redirect_records_where_it_actually_landed(impatient):
    respx.head("https://old.example.org/feed.zip").respond(
        301, headers={"location": "https://new.example.org/feed.zip"}
    )
    respx.head("https://new.example.org/feed.zip").respond(200)
    result = check("https://old.example.org/feed.zip")[
        result_key("https://old.example.org/feed.zip")
    ]
    assert result.ok
    # A catalog entry pointing at a 301 is itself a finding.
    assert result.final_url == "https://new.example.org/feed.zip"


@respx.mock
def test_a_404_on_both_methods_is_a_failure_classed_by_status(impatient):
    respx.head("https://example.org/gone.zip").respond(404)
    respx.get("https://example.org/gone.zip").respond(404)
    result = check("https://example.org/gone.zip")[
        result_key("https://example.org/gone.zip")
    ]
    assert not result.ok
    assert result.error_class == "http_4xx"


@respx.mock
def test_a_timeout_is_classed_as_one(impatient):
    respx.head("https://example.org/slow.zip").mock(
        side_effect=httpx.ReadTimeout("too slow")
    )
    result = check("https://example.org/slow.zip")[
        result_key("https://example.org/slow.zip")
    ]
    assert not result.ok
    assert result.error_class == "timeout"


def test_one_endpoint_listed_by_two_catalogs_is_checked_once():
    rows = [
        Source(
            source_id="tl:f-a:static",
            catalog="transitland",
            kind="static",
            feed_id="f-a",
            name="A",
            urls={"scheduled": "https://Example.org:443/feed.zip"},
        ),
        Source(
            source_id="md:mdb-1:static",
            catalog="mobilitydatabase",
            kind="static",
            feed_id="mdb-1",
            name="A",
            urls={"scheduled": "https://example.org/feed.zip"},
        ),
    ]
    targets, skipped = check_targets(rows)
    assert list(targets) == ["https://example.org/feed.zip"]
    assert skipped == {}


def test_a_feed_needing_an_api_key_is_skipped_not_failed():
    rows = [
        Source(
            source_id="md:mdb-9:static",
            catalog="mobilitydatabase",
            kind="static",
            feed_id="mdb-9",
            name="Keyed",
            urls={"scheduled": "https://example.org/keyed.zip"},
            authentication_type=1,
        )
    ]
    targets, skipped = check_targets(rows)
    assert targets == {}
    assert skipped[result_key("https://example.org/keyed.zip")].error_class == (
        "auth_required"
    )


def test_path_only_curated_urls_are_skipped_as_relative():
    _targets, skipped = check_targets(example_sources(load_examples()))
    assert skipped["/amtrak/vehicle_positions.pb"].error_class == "relative"
    assert skipped["/amtrak/vehicle_positions.pb"].skipped


@respx.mock
def test_the_nested_zip_fragment_is_not_sent_to_the_server(impatient):
    url = "https://example.org/gtfs_public.zip#google_bus.zip"
    route = respx.head("https://example.org/gtfs_public.zip").respond(200)
    result = check(url)[result_key(url)]
    assert route.called
    assert result.ok
    # The result still names the URL as the catalog has it, fragment included.
    assert result.url == url


@respx.mock
def test_surrounding_whitespace_is_not_sent(impatient):
    # A leading space made httpx read the URL as a relative path.
    route = respx.head("http://example.org/vp.php").respond(200)
    url = " http://example.org/vp.php"
    assert check(url)[result_key(url)].ok
    assert route.called


@respx.mock
def test_a_url_that_cannot_be_requested_fails_only_itself(impatient):
    respx.head("https://example.org/ok.zip").respond(200)
    targets = {"bad": "http://[::1", "good": "https://example.org/ok.zip"}
    results = asyncio.run(run_checks(targets))
    assert not results["bad"].ok
    assert results["bad"].error_class == "other"
    assert results["good"].ok


def test_a_malformed_url_is_not_a_crash_when_building_targets():
    row = Source(
        source_id="tl:bad:static",
        catalog="transitland",
        feed_id="bad",
        kind="static",
        name="Bad",
        urls={"scheduled": "http://example.org:notaport/feed.zip"},
    )
    check_targets([row])
