"""The locality file timetable-sites reads for municipality place tags.

Built from the latest Overture Maps release's ``division_area`` parquet by
``gtfs_zone_feed_fetch.localities.build`` and kept in the private schedule
bucket at ``LOCALITIES_KEY``, with the release it came from at
``RELEASE_KEY``. A run whose latest release is already the stored one
writes nothing. The parquet is written before the release name, so a failed
upload is retried on the next run.

Not part of the daily catalog: its own job, weekly, tagged as a heavy run so
it never replaces the file while a sites shard is reading it.
"""

import logging
import tempfile
from collections.abc import Callable
from pathlib import Path

import obstore
from dagster import MaterializeResult, asset
from gtfs_zone_db_models.object_store import ObjectNotFound, ObjectStore
from gtfs_zone_feed_fetch.localities import (
    build,
    latest_overture_release,
    overture_source,
)

from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.zips import ZipStoreSettings

log = logging.getLogger(__name__)

LOCALITIES_KEY = "localities/localities.parquet"
RELEASE_KEY = "localities/release.txt"


def stored_release(store: ObjectStore) -> str:
    try:
        return store.get(RELEASE_KEY).decode()
    except ObjectNotFound:
        return ""


def refresh(
    store: ObjectStore,
    release: str,
    builder: Callable[[str, Path, Path], int] = build,
) -> dict[str, str | int | bool]:
    """Build and upload the file for `release` unless it is the stored one."""
    if stored_release(store) == release:
        return {"release": release, "skipped": True}
    with tempfile.TemporaryDirectory(dir=settings.fetch_workdir or None) as tmp:
        workdir = Path(tmp)
        out = workdir / "localities.parquet"
        rows = builder(overture_source(release), out, workdir)
        size = out.stat().st_size
        obstore.put(
            store.store,
            LOCALITIES_KEY,
            out,
            attributes={"Content-Type": "application/vnd.apache.parquet"},
        )
    store.put(RELEASE_KEY, release.encode(), content_type="text/plain")
    log.info("%d localities from %s, %d bytes", rows, release, size)
    return {"release": release, "skipped": False, "rows": rows, "bytes": size}


@asset(description="Overture localities for the sites' municipality place tags")
def localities() -> MaterializeResult:
    zip_settings = ZipStoreSettings()
    if not zip_settings.s3_endpoint:
        log.warning("ZIPS_S3_ENDPOINT is unset; no localities built")
        return MaterializeResult(metadata={"skipped": True})
    store = ObjectStore(zip_settings)
    return MaterializeResult(metadata=refresh(store, latest_overture_release()))
