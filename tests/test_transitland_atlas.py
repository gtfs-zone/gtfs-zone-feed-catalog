"""The DMFR parse, and specifically the thing a per-file index gets wrong."""

from geometry_car.assets.transitland_atlas import (
    build_operator_index,
    build_sources,
    humanize_feed_id,
    origin_from_filename,
)

# The operator lives in a different document from the feed it claims, and claims
# it by feed_onestop_id rather than the feed declaring operators[]. This is the
# normal case in the real corpus, and the reason the index is global.
FEED_DOC = (
    "api.example.org",
    {
        "feeds": [
            {
                "id": "f-9q9-caltrain",
                "spec": "gtfs",
                "urls": {
                    "static_current": "https://example.org/caltrain.zip",
                    "realtime_vehicle_positions": "https://example.org/vp.pb",
                },
            }
        ]
    },
)
OPERATOR_DOC = (
    "operators.example.org",
    {
        "operators": [
            {
                "onestop_id": "o-9q9-caltrain",
                "name": "Peninsula Corridor Joint Powers Board",
                "short_name": "Caltrain",
                "associated_feeds": [{"feed_onestop_id": "f-9q9-caltrain"}],
            }
        ]
    },
)


def test_operator_resolves_across_files():
    index = build_operator_index([FEED_DOC, OPERATOR_DOC])
    assert index["f-9q9-caltrain"]["short_name"] == "Caltrain"


def test_operator_is_not_found_within_one_file_alone():
    # Proving the point: the feed's own document knows nothing about it.
    assert build_operator_index([FEED_DOC]) == {}


def test_rows_split_by_kind_and_take_the_operator_name():
    rows = build_sources([FEED_DOC, OPERATOR_DOC])
    assert [r.source_id for r in rows] == [
        "tl:f-9q9-caltrain:rt",
        "tl:f-9q9-caltrain:static",
    ]
    assert {r.name for r in rows} == {"Caltrain"}
    assert {r.operator_name for r in rows} == {"Peninsula Corridor Joint Powers Board"}
    static = next(r for r in rows if r.kind == "static")
    assert static.urls == {"scheduled": "https://example.org/caltrain.zip"}


def test_first_operator_to_claim_a_feed_wins():
    other = (
        "zzz.example.org",
        {
            "operators": [
                {
                    "onestop_id": "o-later",
                    "short_name": "Later",
                    "associated_feeds": [{"feed_onestop_id": "f-9q9-caltrain"}],
                }
            ]
        },
    )
    index = build_operator_index([OPERATOR_DOC, other])
    assert index["f-9q9-caltrain"]["short_name"] == "Caltrain"


def test_feeds_declaring_operators_resolve_through_the_second_pass():
    docs = [
        (
            "a",
            {
                "feeds": [
                    {
                        "id": "f-abc",
                        "urls": {"static_current": "https://example.org/a.zip"},
                        "operators": [{"onestop_id": "o-abc"}],
                    }
                ]
            },
        ),
        ("b", {"operators": [{"onestop_id": "o-abc", "short_name": "ABC"}]}),
    ]
    assert build_operator_index(docs)["f-abc"]["short_name"] == "ABC"


def test_non_gtfs_specs_and_urlless_feeds_are_dropped():
    docs = [
        (
            "a",
            {
                "feeds": [
                    {"id": "f-gbfs", "spec": "gbfs", "urls": {"static_current": "x"}},
                    {"id": "f-empty", "spec": "gtfs", "urls": {}},
                ]
            },
        )
    ]
    assert build_sources(docs) == []


def test_humanize_drops_the_geohash_only_when_a_name_remains():
    assert humanize_feed_id("f-9q8-samtrans") == "samtrans"
    assert humanize_feed_id("f-columbia~county~transit") == "columbia county transit"
    # No third segment, so the short name must survive intact.
    assert humanize_feed_id("f-bart") == "bart"


def test_origin_from_filename():
    assert origin_from_filename("feeds/511.org.dmfr.json") == "511.org"
    assert origin_from_filename("plain.json") == "plain"
