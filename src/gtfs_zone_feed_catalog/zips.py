"""The private schedule bucket (``gtfs-zips``) and its index. No Dagster here:
facts workers import this in a process of their own per zip."""

from gtfs_zone_db_models.object_store import (
    ObjectNotFound,
    ObjectStore,
    ObjectStoreSettings,
)
from gtfs_zone_feed_fetch.store import INDEX_KEY, Stored, parse_index
from pydantic_settings import SettingsConfigDict


class ZipStoreSettings(ObjectStoreSettings):
    """The schedule bucket: ObjectStoreSettings read from ZIPS_S3_* instead."""

    model_config = SettingsConfigDict(
        env_prefix="ZIPS_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    s3_bucket: str = "gtfs-zips"


def read_index(store: ObjectStore) -> dict[str, Stored]:
    """The index by normalized URL, or an empty one on a new bucket."""
    try:
        return parse_index(store.get(INDEX_KEY))
    except ObjectNotFound:
        return {}
