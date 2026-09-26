"""Write the public artifacts to the Garage bucket.

The bucket is public and served over HTTP at data.gtfs.zone, so everything
written here is world-readable by design. Nothing secret goes in, and no feed
bodies are stored - only what a catalog already publishes plus whether it
answered.

``manifest.json`` is written last, on purpose: it is what a consumer polls, so
it must never advertise a hash for a file that is not up yet.

Per-feed page fragments and ``sitemap.xml`` go alongside, for list.gtfs.zone's
feed pages. A fragment is rewritten only when its content hash changes, so a
quiet day costs one index read rather than thousands of puts.
"""

import json
import logging
from datetime import datetime

from dagster import AssetExecutionContext, asset
from railroad_club.object_store import ObjectNotFound, ObjectStore, get_object_store

from geometry_car import artifacts, pages
from geometry_car.assets.check_history import SourceStatus
from geometry_car.assets.curated_examples import CuratedExample
from geometry_car.assets.feeds import Feed
from geometry_car.catalog import Source
from geometry_car.heartbeat import push_heartbeat
from geometry_car.settings import settings

log = logging.getLogger(__name__)


def _read_index(store: ObjectStore) -> list[dict]:
    """The snapshot index, or an empty one on a bucket that has none yet."""
    try:
        return json.loads(store.get(artifacts.SNAPSHOT_INDEX_KEY)).get("snapshots", [])
    except ObjectNotFound:
        return []


def _read_pages_index(store: ObjectStore) -> dict[str, dict]:
    """Each feed's page hash and lastmod, or none on a bucket without pages."""
    try:
        return json.loads(store.get(pages.PAGES_INDEX_KEY)).get("pages", {})
    except ObjectNotFound:
        return {}


def publish_pages(
    store: ObjectStore,
    sources: list[Source],
    feeds: list[Feed],
    statuses: dict[str, SourceStatus],
    generated_at: datetime,
) -> dict[str, int]:
    """Write changed feed fragments, drop vanished ones, rewrite the sitemap."""
    rows = {source.source_id: source for source in sources}
    previous = _read_pages_index(store)
    index: dict[str, dict] = {}
    written = 0

    for feed in feeds:
        members = [rows[m] for m in feed.members if m in rows]
        head, body = pages.render(feed, members, statuses, generated_at)
        digest = artifacts.sha256(f"{head}\0{body}".encode())
        since = artifacts.feed_since(feed, statuses)
        entry = {
            "sha256": digest,
            "url": pages.feed_url(feed),
            "indexed": pages.indexable(feed, since, generated_at),
            "lastmod": generated_at.date().isoformat(),
        }
        old = previous.get(feed.feed_id)
        if old and old.get("sha256") == digest:
            entry["lastmod"] = old.get("lastmod", entry["lastmod"])
        else:
            prefix = f"{pages.PAGES_PREFIX}{feed.feed_id}/"
            store.put(
                f"{prefix}head.html",
                head.encode(),
                content_type=pages.PAGE_CONTENT_TYPE,
            )
            store.put(
                f"{prefix}body.html",
                body.encode(),
                content_type=pages.PAGE_CONTENT_TYPE,
            )
            written += 1
        index[feed.feed_id] = entry

    removed = [feed_id for feed_id in previous if feed_id not in index]
    for feed_id in removed:
        store.delete_prefix(f"{pages.PAGES_PREFIX}{feed_id}/")

    store.put(
        pages.PAGES_INDEX_KEY,
        artifacts.dumps({"pages": index}),
        content_type=artifacts.ARTIFACT_CONTENT_TYPE,
    )
    indexed = sorted(
        (entry["url"], entry["lastmod"]) for entry in index.values() if entry["indexed"]
    )
    store.put(
        pages.SITEMAP_KEY,
        pages.sitemap(indexed),
        content_type=pages.SITEMAP_CONTENT_TYPE,
    )
    log.info(
        "pages: %d written, %d removed, %d in the sitemap",
        written,
        len(removed),
        len(indexed),
    )
    return {
        "pages_written": written,
        "pages_removed": len(removed),
        "pages_indexed": len(indexed),
    }


def publish(
    store: ObjectStore,
    sources: list[Source],
    feeds: list[Feed],
    statuses: dict[str, SourceStatus],
    examples: list[CuratedExample],
    run_id: str,
    generated_at: datetime | None = None,
) -> dict[str, int]:
    """Write every artifact, and return what a caller wants to report."""
    generated_at = generated_at or artifacts.utcnow()
    today = generated_at.date()

    documents = {
        "sources.json": artifacts.sources_document(sources, statuses, generated_at),
        "feeds.json": artifacts.feeds_document(feeds, statuses, generated_at),
        "status.json": artifacts.status_document(statuses, generated_at),
        "examples.json": artifacts.example_document(
            examples, statuses, feeds, generated_at
        ),
        "summary.json": artifacts.summary_document(
            sources, statuses, feeds, generated_at
        ),
    }
    for key, body in documents.items():
        store.put(key, body, content_type=artifacts.ARTIFACT_CONTENT_TYPE)
    log.info("wrote %d artifacts to %s", len(documents), store.bucket)

    page_counts = publish_pages(store, sources, feeds, statuses, generated_at)

    # A snapshot is written only when the day's answers differ from the last
    # snapshot's, so a quiet week costs one file rather than seven.
    index = _read_index(store)
    payload = artifacts.snapshot_payload(statuses)
    digest = artifacts.sha256(payload)
    previous = index[-1] if index else None
    wrote_snapshot = False

    if previous and previous.get("sha256") == digest:
        log.info("snapshot unchanged since %s; not writing one", previous["date"])
    else:
        key = artifacts.snapshot_key(today)
        body = artifacts.snapshot_body(payload, generated_at)
        store.put(key, body, content_type=artifacts.SNAPSHOT_CONTENT_TYPE)
        index = [entry for entry in index if entry["date"] != today.isoformat()]
        index.append(
            {
                "date": today.isoformat(),
                "key": key,
                "sha256": digest,
                "bytes": len(body),
            }
        )
        wrote_snapshot = True
        log.info("wrote snapshot %s (%d bytes)", key, len(body))

    dropped = artifacts.snapshots_to_delete(
        index,
        today,
        daily_days=settings.snapshot_daily_days,
        weekly_days=settings.snapshot_weekly_days,
    )
    for key in dropped:
        store.delete(key)
    if dropped:
        index = [entry for entry in index if entry["key"] not in set(dropped)]
        log.info("pruned %d snapshots", len(dropped))

    index.sort(key=lambda entry: entry["date"])
    index_body = artifacts.dumps({"snapshots": index})
    store.put(
        artifacts.SNAPSHOT_INDEX_KEY,
        index_body,
        content_type=artifacts.ARTIFACT_CONTENT_TYPE,
    )

    # Last, on purpose: the manifest is what a consumer polls, so it must never
    # advertise a hash for a file that is not up yet.
    manifest = artifacts.manifest_document(
        documents | {artifacts.SNAPSHOT_INDEX_KEY: index_body}, generated_at, run_id
    )
    store.put("manifest.json", manifest, content_type=artifacts.ARTIFACT_CONTENT_TYPE)

    return {
        "sources": len(sources),
        "feeds": len(feeds),
        "snapshots": len(index),
        "snapshots_pruned": len(dropped),
        "snapshot_written": int(wrote_snapshot),
        "bytes": sum(len(body) for body in documents.values()),
        **page_counts,
    }


@asset(
    description=(
        "sources, feeds, status, examples, summary, snapshot, manifest, "
        "feed pages and sitemap"
    ),
    # Ordering only: the CORS rule is in place before anything is published.
    deps=["bucket_cors"],
)
def published_artifacts(
    context: AssetExecutionContext,
    sources: list[Source],
    feeds: list[Feed],
    check_history: dict[str, SourceStatus],
    curated_examples: list[CuratedExample],
) -> None:
    store = get_object_store()
    metadata = publish(
        store, sources, feeds, check_history, curated_examples, context.run_id
    )
    heartbeat = push_heartbeat(settings)
    context.add_output_metadata(
        {"bucket": store.bucket, "heartbeat": heartbeat, **metadata}
    )
