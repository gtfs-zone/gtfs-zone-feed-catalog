"""Asset values pickled into the schedule bucket under ``dagster/``, so a run
after a rollout can still load what an earlier run materialized."""

import pickle

from dagster import InputContext, IOManager, OutputContext
from gtfs_zone_db_models.object_store import ObjectStore

PREFIX = "dagster/"


class StoreIOManager(IOManager):
    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    @staticmethod
    def key(context: InputContext | OutputContext) -> str:
        return PREFIX + "/".join(context.get_asset_identifier()) + ".pickle"

    def handle_output(self, context: OutputContext, obj: object) -> None:
        body = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        self._store.put(
            self.key(context), body, content_type="application/octet-stream"
        )
        context.add_output_metadata({"pickle_bytes": len(body)})

    def load_input(self, context: InputContext) -> object:
        return pickle.loads(self._store.get(self.key(context)))
