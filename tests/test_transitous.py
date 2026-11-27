"""Transitous' region feed lists as source rows."""

import io
import json
import tarfile

import respx

from gtfs_zone_feed_catalog.artifacts import catalog_url, source_row
from gtfs_zone_feed_catalog.assets.endpoint_checks import check_targets, result_key
from gtfs_zone_feed_catalog.assets.feeds import build_feeds
from gtfs_zone_feed_catalog.assets.sources import drop_dangling, merge
from gtfs_zone_feed_catalog.assets.transitous import (
    build_sources,
    region_files,
    transitous,
)
from gtfs_zone_feed_catalog.catalog import Place, Source
from gtfs_zone_feed_catalog.settings import settings

URL = settings.transitous_url
ANDORRA = "https://hoermalmeister.github.io/andorra_gtfs/andorra_gtfs.zip"

# Cut from the live region files.
REGIONS = {
    "ad": [
        {
            "name": "Andorra-Bus",
            "type": "http",
            "url": ANDORRA,
            "license": {
                "spdx-identifier": "CC-BY-NC-SA-4.0",
                "url": "https://hoermalmeister.github.io/andorra_gtfs/LICENSE",
            },
        },
        {
            "name": "Andorra-Bus",
            "type": "url",
            "spec": "gtfs-rt",
            "url": "https://andorragtfsrt-production.up.railway.app/trip_updates.pb",
        },
        {
            "name": "Andorra-Bus",
            "type": "url",
            "spec": "gtfs-rt",
            "url": "https://andorragtfsrt-production.up.railway.app/vehicle.pb",
            "skip": True,
            "skip-reason": "We currently don't make use of vehicle positions feeds",
        },
    ],
    "au-sa": [
        {
            "name": "Adelaide-Metro",
            "type": "mobility-database",
            "spec": "gtfs-rt",
            "mdb-id": "mdb-1773",
        },
        {
            "name": "Adelaide-Metro",
            "type": "mobility-database",
            "mdb-id": "mdb-1772",
            "license": {"spdx-identifier": "CC-BY-4.0"},
        },
    ],
    "eu": [
        {
            "name": "sharedmobility",
            "type": "url",
            "spec": "gbfs",
            "url": "https://sharedmobility.ch/v2/gbfs",
        },
        {
            "name": "flix",
            "type": "transitland-atlas",
            "transitland-atlas-id": "f-u-flix~flixbus",
            "headers": {"Authorization": "AGE-ENCRYPTED:..."},
        },
    ],
}


def tarball(regions: dict[str, list[dict]]) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:

        def add(path: str, body: bytes) -> None:
            info = tarfile.TarInfo(path)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))

        for region, sources in regions.items():
            doc = {"maintainers": [], "sources": sources}
            add(f"transitous-main/feeds/{region}.json", json.dumps(doc).encode())
        # Neither a region file nor readable: both ignored.
        add("transitous-main/transitland-atlas/feeds/x.json", b'{"feeds": []}')
        add("transitous-main/feeds/broken.json", b"{")
    return out.getvalue()


def by_id(rows: list[Source]) -> dict[str, Source]:
    return {row.source_id: row for row in rows}


def test_region_files_are_read_from_the_tarball():
    regions = region_files(tarball(REGIONS))
    assert sorted(regions) == ["ad", "au-sa", "eu"]
    assert regions["ad"][0]["name"] == "Andorra-Bus"


def test_a_source_is_one_row_of_its_spec_kind():
    rows = by_id(build_sources(REGIONS))
    assert sorted(rows) == [
        "transitous:ad/Andorra-Bus:rt",
        "transitous:ad/Andorra-Bus:rt-2",
        "transitous:ad/Andorra-Bus:static",
        "transitous:au-sa/Adelaide-Metro:rt",
        "transitous:au-sa/Adelaide-Metro:static",
        "transitous:eu/flix:static",
    ]
    static = rows["transitous:ad/Andorra-Bus:static"]
    assert (static.catalog, static.kind, static.name) == (
        "transitous",
        "static",
        "Andorra Bus",
    )
    assert static.urls == {"scheduled": ANDORRA}
    assert static.license_url == "https://hoermalmeister.github.io/andorra_gtfs/LICENSE"
    rt = rows["transitous:ad/Andorra-Bus:rt"]
    assert rt.urls == {"realtime": REGIONS["ad"][1]["url"]}
    # One name in a region is one catalog feed.
    assert {row.feed_id for row in rows.values() if row.name == "Andorra Bus"} == {
        "ad/Andorra-Bus"
    }


def test_a_skipped_source_is_kept_with_its_reason_and_not_checked():
    rows = by_id(build_sources(REGIONS))
    skipped = rows["transitous:ad/Andorra-Bus:rt-2"]
    assert skipped.skip_reason.startswith("We currently don't")
    targets, _ = check_targets([skipped])
    assert result_key(skipped.urls["realtime"]) not in targets
    assert source_row(skipped, None)["skip_reason"] == skipped.skip_reason


def test_a_catalog_reference_carries_no_url_and_names_its_row():
    rows = by_id(build_sources(REGIONS))
    static = rows["transitous:au-sa/Adelaide-Metro:static"]
    assert static.urls == {}
    assert static.feed_references == ("md:mdb-1772",)
    assert static.license_url == "https://spdx.org/licenses/CC-BY-4.0.html"
    assert rows["transitous:au-sa/Adelaide-Metro:rt"].feed_references == (
        "md:mdb-1773",
    )
    flix = rows["transitous:eu/flix:static"]
    assert flix.feed_references == ("tl:f-u-flix~flixbus",)
    assert flix.authentication_type == 2


def test_the_region_names_the_place():
    rows = by_id(build_sources(REGIONS))
    assert rows["transitous:au-sa/Adelaide-Metro:static"].place == Place(
        country_code="AU", country="Australia", subdivision="South Australia"
    )
    assert rows["transitous:ad/Andorra-Bus:static"].place == Place(
        country_code="AD", country="Andorra"
    )
    assert rows["transitous:eu/flix:static"].place == Place()


def test_a_reference_joins_the_row_it_names_and_a_dangling_one_is_dropped():
    mdb = [
        Source(
            source_id="md:mdb-1772:static",
            catalog="mobilitydatabase",
            kind="static",
            feed_id="mdb-1772",
            name="Adelaide Metro",
            urls={"scheduled": "https://gtfs.adelaidemetro.com.au/v1/static/latest"},
        ),
        Source(
            source_id="md:mdb-1773:rt",
            catalog="mobilitydatabase",
            kind="rt",
            feed_id="mdb-1773",
            name="Adelaide Metro",
            urls={"vehicles": "https://gtfs.adelaidemetro.com.au/v1/realtime/vp"},
        ),
    ]
    rows = drop_dangling(merge(mdb, build_sources(REGIONS)))
    # The Atlas lists no f-u-flix~flixbus here.
    assert "transitous:eu/flix:static" not in by_id(rows)
    adelaide = [
        feed
        for feed in build_feeds(rows, {}, {})
        if "md:mdb-1772:static" in feed.members
    ]
    (feed,) = adelaide
    assert set(feed.members) == {
        "md:mdb-1772:static",
        "md:mdb-1773:rt",
        "transitous:au-sa/Adelaide-Metro:rt",
        "transitous:au-sa/Adelaide-Metro:static",
    }


def test_a_reference_without_a_spec_joins_a_realtime_row():
    atlas = Source(
        source_id="tl:f-r7h-translink~rt:rt",
        catalog="transitland",
        kind="rt",
        feed_id="f-r7h-translink~rt",
        name="Translink",
        urls={"trip_updates": "https://gtfsrt.api.translink.com.au/api/realtime/SEQ"},
    )
    regions = {
        "au-qld": [
            {
                "name": "Translink-SEQ",
                "type": "transitland-atlas",
                "transitland-atlas-id": "f-r7h-translink~rt",
            }
        ]
    }
    rows = drop_dangling([atlas, *build_sources(regions)])
    (feed,) = build_feeds(rows, {}, {})
    assert feed.members == (
        "tl:f-r7h-translink~rt:rt",
        "transitous:au-qld/Translink-SEQ:static",
    )


def test_the_catalog_link_is_the_region_file():
    row = by_id(build_sources(REGIONS))["transitous:au-sa/Adelaide-Metro:static"]
    assert catalog_url(row) == (
        "https://github.com/public-transport/transitous/blob/main/feeds/au-sa.json"
    )


@respx.mock
def test_the_asset_reads_the_tarball_with_one_get():
    route = respx.get(URL).respond(content=tarball(REGIONS))
    rows = transitous()
    assert route.call_count == 1
    assert len(rows) == 6


@respx.mock
def test_a_failing_or_unreadable_tarball_yields_no_rows():
    respx.get(URL).respond(500)
    assert transitous() == []
    respx.get(URL).respond(content=b"not a tarball")
    assert transitous() == []


def test_a_blank_url_skips_the_catalog(monkeypatch):
    monkeypatch.setattr(settings, "transitous_url", "")
    with respx.mock(assert_all_called=False) as mock:
        route = mock.get(URL)
        assert transitous() == []
        assert not route.called


def test_a_realtime_url_under_several_names_pairs_with_none():
    alerts = "https://api.transport.nsw.gov.au/v2/gtfs/alerts/buses"
    regions = {
        "au-nsw": [
            {
                "name": f"TfNSW-{operator}",
                "type": "http",
                "url": f"https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses/{operator}",
            }
            for operator in ("GSBC001", "GSBC002")
        ]
        + [
            {
                "name": f"TfNSW-{operator}",
                "type": "url",
                "spec": "gtfs-rt",
                "url": alerts,
            }
            for operator in ("GSBC001", "GSBC002")
        ]
    }
    rows = by_id(build_sources(regions))
    assert (
        rows["transitous:au-nsw/TfNSW-GSBC001:rt"].feed_id == "au-nsw/TfNSW-GSBC001/rt"
    )
    feeds = sorted(build_feeds(list(rows.values()), {}, {}), key=lambda f: f.members)
    assert [feed.members for feed in feeds] == [
        ("transitous:au-nsw/TfNSW-GSBC001:rt", "transitous:au-nsw/TfNSW-GSBC002:rt"),
        ("transitous:au-nsw/TfNSW-GSBC001:static",),
        ("transitous:au-nsw/TfNSW-GSBC002:static",),
    ]


def test_a_url_that_is_not_http_is_skipped():
    regions = {
        "it": [
            {"name": "STA", "type": "ftp", "url": "ftp://ftp.sta.bz.it/gtfs.zip"},
            {"name": "VIC", "type": "http", "url": "#1/google_transit.zip"},
            {"name": "KEY", "type": "url", "spec": "gtfs-rt", "url": "AGE-ENCRYPTED:x"},
        ]
    }
    assert build_sources(regions) == []
