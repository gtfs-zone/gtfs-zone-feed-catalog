"""The NTD GTFS weblinks dataset as source rows."""

import json

import httpx
import respx

from gtfs_zone_feed_catalog.assets import ntd as ntd_module
from gtfs_zone_feed_catalog.assets.ntd import build_sources, ntd
from gtfs_zone_feed_catalog.catalog import Place
from gtfs_zone_feed_catalog.settings import settings

URL = settings.ntd_weblinks_url
KING = "https://metro.kingcounty.gov/GTFS/google_transit.zip"
LTD = "https://feed.ltd.org/TMGTFSRealTimeWebService/GTFS/google_transit.zip"
RHODY = (
    "https://oregon-gtfs.trilliumtransit.com"
    "/gtfs_data/rhodyexpress-or-us/rhodyexpress-or-us.zip"
)
ZIP_INSTEAD = (
    "This agency provided the GTFS as a zip file instead of a public weblink,"
    " providing justification for why it could not be hosted."
)


def record(ntd_id: str, name: str, city: str, state: str, url: str = "", **extra):
    out = {"ntd_id": ntd_id, "agency_name": name, "city": city, "state": state}
    if url:
        out["weblink"] = {"url": url}
    return out | extra


# Cut from the live dataset: King County reports one weblink for several
# modes, Lane Transit District several weblinks, Shoshone-Bannock a zip.
RECORDS = [
    record("00001", "King County", "Seattle", "WA", KING),
    record("00001", "King County", "Seattle", "WA", f" {KING} "),
    record("00007", "Lane Transit District", "Eugene", "OR", LTD),
    record("00007", "Lane Transit District", "Eugene", "OR", RHODY),
    record(
        "00031",
        "Shoshone-Bannock Tribes",
        "Fort Hall",
        "ID",
        weblink_justification=ZIP_INSTEAD,
    ),
    record("00099", "No Link", "Nowhere", "WA"),
    record(
        "20078",
        "Metro-North Commuter Railroad Company, dba: MTA Metro-North Railroad",
        "New York",
        "NY",
        "https://web.mta.info/developers/data/mnr/google_transit.zip",
    ),
]


def test_records_collapse_to_one_row_per_agency_and_weblink():
    rows = build_sources(RECORDS)
    by_agency = {}
    for row in rows:
        by_agency.setdefault(row.feed_id.split("-")[0], []).append(row)
    assert {k: len(v) for k, v in by_agency.items()} == {
        "00001": 1,
        "00007": 2,
        "20078": 1,
    }
    (king,) = by_agency["00001"]
    assert king.catalog == "ntd"
    assert king.kind == "static"
    assert king.source_id.startswith("ntd:00001-")
    assert king.source_id.endswith(":static")
    # Surrounding whitespace is stripped before the URL is stored.
    assert king.urls == {"scheduled": KING}
    assert king.origin == "FTA National Transit Database"
    assert king.place == Place(
        country_code="US",
        country="United States",
        subdivision="Washington",
        municipality="Seattle",
    )


def test_row_ids_are_stable_whatever_the_record_order():
    forward = [r.source_id for r in build_sources(RECORDS)]
    backward = [r.source_id for r in build_sources(list(reversed(RECORDS)))]
    assert forward == backward
    lane = [r.feed_id for r in build_sources(RECORDS) if r.feed_id.startswith("00007")]
    assert len(set(lane)) == 2
    # The suffix depends only on the URL: dropping one weblink leaves the other.
    (alone,) = build_sources([RECORDS[2]])
    assert alone.feed_id in lane


def test_a_dba_name_is_used_in_place_of_the_legal_one():
    (mnr,) = [r for r in build_sources(RECORDS) if r.feed_id.startswith("20078")]
    assert (mnr.name, mnr.operator_name) == (
        "MTA Metro-North Railroad",
        "MTA Metro-North Railroad",
    )


@respx.mock
def test_the_asset_pages_until_a_short_page(monkeypatch):
    monkeypatch.setattr(ntd_module, "PAGE_SIZE", 2)
    pages = [RECORDS[0:2], RECORDS[2:4], RECORDS[4:5]]
    seen: list[int] = []

    def respond(request: httpx.Request) -> httpx.Response:
        number = json.loads(request.content)["page"]["pageNumber"]
        seen.append(number)
        return httpx.Response(200, json=pages[number - 1])

    respx.post(URL).mock(side_effect=respond)
    rows = ntd()
    assert seen == [1, 2, 3]
    assert len(rows) == 3


@respx.mock
def test_a_failing_endpoint_yields_no_rows():
    respx.post(URL).respond(500)
    assert ntd() == []


@respx.mock
def test_a_non_array_body_yields_no_rows():
    respx.post(URL).respond(json={"error": "nope"})
    assert ntd() == []


def test_a_blank_url_skips_the_catalog(monkeypatch):
    monkeypatch.setattr(settings, "ntd_weblinks_url", "")
    with respx.mock(assert_all_called=False) as mock:
        route = mock.post(URL)
        assert ntd() == []
        assert not route.called


@respx.mock
def test_the_app_token_is_sent_only_when_set(monkeypatch):
    route = respx.post(URL).respond(json=[])
    ntd()
    assert "X-App-Token" not in route.calls.last.request.headers

    monkeypatch.setattr(settings, "ntd_app_token", "tok")
    ntd()
    assert route.calls.last.request.headers["X-App-Token"] == "tok"
