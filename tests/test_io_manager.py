"""Asset values round-trip through the bucket under dagster/."""

from dagster import Definitions, asset, materialize
from gtfs_zone_db_models.object_store import ObjectStore, ObjectStoreSettings
from obstore.store import MemoryStore

from gtfs_zone_feed_catalog.io_manager import StoreIOManager


def test_asset_values_round_trip_through_the_store():
    store = ObjectStore(ObjectStoreSettings(), store=MemoryStore())
    manager = StoreIOManager(store)

    @asset
    def upstream() -> dict[str, int]:
        return {"a": 1}

    @asset
    def downstream(upstream: dict[str, int]) -> int:
        return upstream["a"] + 1

    result = materialize([upstream, downstream], resources={"io_manager": manager})
    assert result.success
    assert store.list_prefix("dagster/") == [
        "dagster/downstream.pickle",
        "dagster/upstream.pickle",
    ]

    # A later run loads the stored upstream value without recomputing it.
    again = materialize(
        [upstream, downstream],
        resources={"io_manager": manager},
        selection=["downstream"],
    )
    assert again.output_for_node("downstream") == 2


def test_definitions_load():
    from gtfs_zone_feed_catalog import definitions

    assert isinstance(definitions.defs, Definitions)
