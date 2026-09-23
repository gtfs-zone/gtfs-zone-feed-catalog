"""Write the public artifacts to the Garage bucket.

The bucket is public and served over HTTP at data.gtfs.zone, so everything
written here is world-readable by design. Nothing secret goes in, and no feed
bodies are stored - only what a catalog already publishes plus whether it
answered.

``manifest.json`` is written last, on purpose: it is what a consumer polls, so
it must never advertise a hash for a file that is not up yet.
"""

import json
import logging
from datetime import datetime

from dagster import AssetExecutionContext, asset
from railroad_club.object_store import ObjectNotFound, ObjectStore, get_object_store

from geometry_car import artifacts
from geometry_car.assets.check_history import SourceStatus
from geometry_car.assets.curated_examples import CuratedExample
from geometry_car.catalog import Source
from geometry_car.settings import settings

log = logging.getLogger(__name__)


def _read_index(store: ObjectStore) -> list[dict]:
    """The snapshot index, or an empty one on a bucket that has none yet."""
    try:
        return json.loads(store.get(artifacts.SNAPSHOT_INDEX_KEY)).get("snapshots", [])
    except ObjectNotFound:
        return []


def publish(
    store: ObjectStore,
    sources: list[Source],
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
        "status.json": artifacts.status_document(statuses, generated_at),
        "examples.json": artifacts.example_document(examples, statuses, generated_at),
        "summary.json": artifacts.summary_document(sources, statuses, generated_at),
    }
    for key, body in documents.items():
        store.put(key, body, content_type=artifacts.ARTIFACT_CONTENT_TYPE)
    log.info("wrote %d artifacts to %s", len(documents), store.bucket)

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
        "snapshots": len(index),
        "snapshots_pruned": len(dropped),
        "snapshot_written": int(wrote_snapshot),
        "bytes": sum(len(body) for body in documents.values()),
    }


@asset(
    description="sources, status, examples, summary, a snapshot and the manifest",
    # Ordering only: the CORS rule is in place before anything is published.
    deps=["bucket_cors"],
)
def published_artifacts(
    context: AssetExecutionContext,
    sources: list[Source],
    check_history: dict[str, SourceStatus],
    curated_examples: list[CuratedExample],
) -> None:
    store = get_object_store()
    metadata = publish(store, sources, check_history, curated_examples, context.run_id)
    context.add_output_metadata({"bucket": store.bucket, **metadata})
