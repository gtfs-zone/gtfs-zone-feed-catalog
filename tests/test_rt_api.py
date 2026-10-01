"""rt-api's public catalog as source rows."""

from gtfs_zone_feed_catalog.assets.rt_api import build_sources

AMTRAK = {
    "feed_name": "amtrak",
    "static_url": "https://content.amtrak.com/content/gtfs/GTFS.zip",
    "vehicle_positions_url": "https://rt.gtfs.zone/amtrak/vehicle_positions.pb",
    "trip_updates_url": "https://rt.gtfs.zone/amtrak/trip_updates.pb",
    "service_alerts_url": "https://rt.gtfs.zone/amtrak/service_alerts.pb",
    "has_vehicles": True,
}


def test_an_rt_api_feed_is_a_static_and_an_rt_row():
    rt, static = build_sources([AMTRAK, {"feed_name": ""}])
    assert (static.source_id, static.catalog) == ("gz:amtrak:static", "gtfszone")
    assert static.urls == {"scheduled": AMTRAK["static_url"]}
    assert rt.source_id == "gz:amtrak:rt"
    assert rt.urls == {
        "vehicles": AMTRAK["vehicle_positions_url"],
        "trip_updates": AMTRAK["trip_updates_url"],
        "alerts": AMTRAK["service_alerts_url"],
    }
    # No name of its own beyond the slug, and no operator to rank.
    assert (rt.name, rt.operator_name) == ("amtrak", "")
