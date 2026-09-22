"""Normalization, cross-linking and the coordinates that travel along a link."""

from geometry_car.assets.sources import cross_link, merge
from geometry_car.catalog import Place, Source
from geometry_car.urls import normalize_url

PLACE = Place(country_code="US", latitude=34.0, longitude=-118.0)


def tl(url: str, source_id: str = "tl:f-a:static") -> Source:
    return Source(
        source_id=source_id,
        catalog="transitland",
        kind="static",
        feed_id="f-a",
        name="A",
        urls={"scheduled": url},
    )


def md(url: str, place: Place = PLACE) -> Source:
    return Source(
        source_id="md:mdb-1:static",
        catalog="mobilitydatabase",
        kind="static",
        feed_id="mdb-1",
        name="A",
        urls={"scheduled": url},
        place=place,
    )


def test_normalize_url_folds_case_default_ports_and_query_order():
    assert normalize_url("HTTPS://Example.ORG:443/Feed.zip?b=2&a=1") == (
        "https://example.org/Feed.zip?a=1&b=2"
    )
    assert normalize_url("https://example.org/feed.zip/") == (
        "https://example.org/feed.zip"
    )
    # http and https stay distinct: a plain-http endpoint is a different thing
    # to fetch, and several curated entries exist only in that form.
    assert normalize_url("http://example.org/f") != normalize_url(
        "https://example.org/f"
    )
    assert normalize_url("/amtrak/vehicle_positions.pb") == ""


def test_cross_link_pairs_the_catalogs_and_propagates_the_place():
    rows = cross_link(
        [tl("https://Example.org:443/feed.zip"), md("https://example.org/feed.zip")]
    )
    transitland, mobility = rows
    assert transitland.same_endpoint_as == ("md:mdb-1:static",)
    assert mobility.same_endpoint_as == ("tl:f-a:static",)
    # DMFR has no coordinates at all; the link is the only way this row is placed.
    assert transitland.place.latitude == 34.0


def test_a_link_to_an_unplaced_row_leaves_the_row_unplaced():
    rows = cross_link(
        [tl("https://example.org/f.zip"), md("https://example.org/f.zip", Place())]
    )
    assert not rows[0].place.placed


def test_two_rows_of_one_catalog_sharing_a_url_are_not_cross_linked():
    rows = cross_link(
        [
            tl("https://example.org/f.zip"),
            tl("https://example.org/f.zip", "tl:f-b:static"),
        ]
    )
    assert all(row.same_endpoint_as == () for row in rows)


def test_merge_keeps_one_row_per_id_and_sorts():
    rows = merge([tl("https://a/1"), tl("https://b/2")], [md("https://c/3")])
    assert [r.source_id for r in rows] == ["md:mdb-1:static", "tl:f-a:static"]
    # First listed wins a collision, so the catalog order in `sources` decides.
    assert rows[1].urls["scheduled"] == "https://a/1"
