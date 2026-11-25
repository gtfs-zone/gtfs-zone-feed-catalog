"""What each stored schedule holds, from the zip ``feed_fetch`` kept.

Facts are computed once per sha256 and kept beside the zip as
``gtfs/<sha256>.facts.json`` (``facts_worker.facts_doc``), so a run computes
only zips that are new, or whose facts an older ``FACTS_VERSION`` wrote. Each
zip is copied from the bucket to a temp file in ``fetch_workdir`` by this
process, then scanned by a worker process of its own capped in address space:
``facts_workers`` zips on disk and in flight at once. A run with zips to scan
first copies the locality file (``localities.LOCALITIES_KEY``) to a temp file
for the workers' municipality tags; with none stored, facts have no tags.

The result is each indexed URL's content by normalized URL, in the shape of
timetable-sites's content report: ``ok`` with facts, ``not_zip`` with what the
body looked like, or the reason the zip could not be read. URLs with no stored
body (a failed download) are left out.

The same scan writes the zip's agency fingerprints beside it as
``gtfs/<sha256>.fingerprint.parquet`` when its facts are ok. A zip whose
current facts were stored without one is scanned again only when
``overlaps`` could pair it with another zip (``overlap_candidates``).
"""

import json
import logging
import multiprocessing
import tempfile
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime
from functools import partial
from itertools import combinations
from typing import Any

import obstore
from dagster import asset
from gtfs_zone_db_models.object_store import (
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
)
from gtfs_zone_feed_fetch.facts import FACTS_VERSION
from gtfs_zone_feed_fetch.pool import WorkerError, isolated
from gtfs_zone_feed_fetch.store import PREFIX, Stored, facts_key, fingerprint_key

from gtfs_zone_feed_catalog import facts_worker
from gtfs_zone_feed_catalog.assets.feed_contents import FeedContent
from gtfs_zone_feed_catalog.assets.feeds import intersect, normalize_agency
from gtfs_zone_feed_catalog.assets.localities import LOCALITIES_KEY
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.zips import ZipStoreSettings, read_index

log = logging.getLogger(__name__)

# Facts documents read at once.
READ_CONCURRENCY = 16
CHUNK = 1 << 20
# Worker failures that are the zip's size, not a passing fault.
MEMORY_ERRORS = ("MemoryError", "BrokenProcessPool")
# Keys of the scan that are the stored object's, not the zip's contents.
OBJECT_KEYS = ("bytes", "sha256")
PARQUET = "application/vnd.apache.parquet"

type Scanned = tuple[dict[str, Any], bytes]


def read_docs(store: ObjectStore, shas: list[str]) -> dict[str, dict[str, Any]]:
    """The stored facts documents of ``shas`` that exist."""

    def one(sha: str) -> tuple[str, dict[str, Any] | None]:
        try:
            return sha, json.loads(store.get(facts_key(sha)))
        except ObjectNotFound:
            return sha, None

    with ThreadPoolExecutor(READ_CONCURRENCY) as pool:
        return {sha: doc for sha, doc in pool.map(one, shas) if doc is not None}


def result_doc(
    sha: str, result: Scanned | WorkerError
) -> tuple[dict[str, Any] | None, bytes]:
    """The document and fingerprint file to store for a worker's result; no
    document for a failure worth trying again next run."""
    if not isinstance(result, WorkerError):
        return result
    name = str(result).partition(": ")[0]
    if name in MEMORY_ERRORS:
        return facts_worker.facts_doc("memory", name), b""
    log.warning("facts of %s failed: %s", sha, result)
    return None, b""


def overlap_candidates(docs: dict[str, dict[str, Any]]) -> set[str]:
    """The zips with ok facts that share a normalized agency name with another
    zip of another cleaned hash, with intersecting station bboxes: every zip
    ``feeds.overlaps`` can pair, and other versions of the same feed."""
    boxes: dict[str, list[float]] = {}
    cleaned: dict[str, str] = {}
    by_agency: dict[str, set[str]] = defaultdict(set)
    for sha, doc in docs.items():
        facts = doc.get("facts") or {}
        box = (facts.get("location") or {}).get("bbox") or []
        if doc.get("outcome") != "ok" or len(box) != 4:
            continue
        boxes[sha] = box
        cleaned[sha] = facts.get("cleanedHash") or sha
        for agency in facts.get("agencies") or []:
            if key := normalize_agency(agency.get("name") or ""):
                by_agency[key].add(sha)
    found: set[str] = set()
    for shas in by_agency.values():
        for a, b in combinations(sorted(shas), 2):
            if cleaned[a] != cleaned[b] and intersect(boxes[a], boxes[b]):
                found.update((a, b))
    return found


@contextmanager
def copied(store: ObjectStore, key: str, suffix: str) -> Iterator[str]:
    """The object at ``key`` in a temp file in ``fetch_workdir``, by path."""
    with tempfile.NamedTemporaryFile(
        dir=settings.fetch_workdir or None, suffix=suffix
    ) as handle:
        result = obstore.get(store.store, key)
        for chunk in result.stream(min_chunk_size=CHUNK):
            handle.write(chunk)
        handle.flush()
        yield handle.name


@contextmanager
def locality_file(store: ObjectStore) -> Iterator[str | None]:
    """The stored locality file in a temp file, None when there is none."""
    try:
        store.stat(LOCALITIES_KEY)
    except ObjectNotFound:
        log.warning("no %s; facts without municipality tags", LOCALITIES_KEY)
        yield None
        return
    with copied(store, LOCALITIES_KEY, ".parquet") as path:
        yield path


def scan_stored(
    store: ObjectStore, sha: str, localities: str | None = None
) -> Scanned | WorkerError:
    """Copy ``gtfs/<sha>.zip`` to a temp file and scan it in a capped worker."""
    with copied(store, f"{PREFIX}{sha}.zip", ".zip") as path:
        return isolated(
            partial(facts_worker.scan_doc, localities=localities),
            path,
            settings.facts_worker_memory,
        )


def compute_missing(
    store: ObjectStore,
    todo: list[str],
    docs: dict[str, dict[str, Any]],
    localities: str | None = None,
) -> None:
    """Compute and store the facts and fingerprints of ``todo``, adding the
    facts to ``docs``."""
    # Workers fork from a server that has the worker module imported already.
    multiprocessing.set_forkserver_preload(["gtfs_zone_feed_catalog.facts_worker"])
    outcomes: dict[str, int] = {}
    with ThreadPoolExecutor(max(settings.facts_workers, 1)) as threads:
        futures = {
            threads.submit(scan_stored, store, sha, localities): sha for sha in todo
        }
        for done, future in enumerate(as_completed(futures), 1):
            sha = futures[future]
            try:
                doc, fingerprint = result_doc(sha, future.result())
            except Exception as exc:
                # A failed copy from the bucket: tried again next run.
                log.warning("facts of %s failed: %s: %s", sha, type(exc).__name__, exc)
                doc, fingerprint = None, b""
            if fingerprint:
                store.put(fingerprint_key(sha), fingerprint, content_type=PARQUET)
            if doc is not None:
                store.put(
                    facts_key(sha),
                    json.dumps(doc).encode(),
                    content_type="application/json",
                )
                docs[sha] = doc
                outcomes[doc["outcome"]] = outcomes.get(doc["outcome"], 0) + 1
            if done % 250 == 0:
                log.info("facts: %d of %d zips", done, len(todo))
    log.info("facts outcomes: %s", outcomes)


def day(stamp: str) -> date:
    return datetime.fromisoformat(stamp).date()


def content_of(key: str, entry: Stored, doc: dict[str, Any] | None) -> FeedContent:
    """A URL's content from its index entry and, for a zip, its facts."""
    checked = day(entry.checked)
    since = day(entry.since) if entry.since else checked
    if entry.kind != "zip":
        return FeedContent(
            url=key, outcome="not_zip", checked=checked, since=since, detail=entry.kind
        )
    doc = doc or {}
    facts = doc.get("facts") or {}
    return FeedContent(
        url=key,
        outcome=doc.get("outcome", "error"),
        checked=checked,
        since=since,
        detail=doc.get("detail", ""),
        sha256=entry.sha256,
        size=entry.bytes,
        facts={k: v for k, v in facts.items() if k not in OBJECT_KEYS},
    )


def facts(store: ObjectStore) -> dict[str, FeedContent]:
    """Compute what is missing, then every stored URL's content."""
    index = read_index(store)
    have = set(store.list_prefix(PREFIX))
    stored = {k: e for k, e in index.items() if e.key and e.key in have}
    sizes = {e.sha256: e.bytes or 0 for e in stored.values() if e.kind == "zip"}

    docs = read_docs(store, [sha for sha in sizes if facts_key(sha) in have])
    docs = {sha: doc for sha, doc in docs.items() if doc["version"] == FACTS_VERSION}
    missing = set(sizes) - set(docs)
    unprinted = {sha for sha in docs if fingerprint_key(sha) not in have}
    backfill = overlap_candidates(docs) & unprinted
    # Largest first, so the long scans start while the rest fill in around them.
    todo = sorted(missing | backfill, key=lambda sha: -sizes[sha])
    log.info(
        "facts: %d zips stored, %d to compute, %d more to fingerprint",
        len(sizes),
        len(missing),
        len(backfill),
    )
    if todo:
        with locality_file(store) as localities:
            compute_missing(store, todo, docs, localities)

    return {
        key: content_of(key, entry, docs.get(entry.sha256))
        for key, entry in stored.items()
        if entry.kind != "zip" or entry.sha256 in docs
    }


@asset(
    description="Facts of every stored schedule zip, computed once per sha256",
    deps=["feed_fetch"],
)
def feed_facts() -> dict[str, FeedContent]:
    zip_settings = ZipStoreSettings()
    if not zip_settings.s3_endpoint:
        log.warning("ZIPS_S3_ENDPOINT is unset; no facts")
        return {}
    try:
        return facts(ObjectStore(zip_settings))
    except ObjectStoreError as exc:
        log.warning("schedule bucket unavailable, no facts: %s", exc)
        return {}
