"""Download every schedule once a day, and keep the latest body of each.

One conditional GET per static normalized URL, under the same rules as the
checks: a global cap on requests in flight, one request at a time per host with
a pause between them, and the same User-Agent. The validators sent are the ones
the last download returned, so an unchanged schedule costs a 304.

A body is streamed to a temp file, hashed on the way and uploaded to the
private schedule bucket as ``gtfs/<sha256>.zip``, once per distinct sha256. A
body that is not a zip (an HTML error page, a JSON answer) is kept as
``gtfs/<sha256>.body`` when small. ``gtfs/index.json`` maps each normalized URL
to its stored object and validators; it is written after the uploads and before
any object no URL references is deleted, so it never names a missing object.
Only the latest body per URL is kept.

The response is also that URL's check: ``endpoint_checks`` does not request a
URL fetched here again. A body over ``fetch_max_bytes`` is abandoned after the
headers, which then answer the check as a ranged GET would.

Without ``ZIPS_S3_ENDPOINT``, or with the bucket unreachable, nothing is
fetched and ``endpoint_checks`` checks every URL as before.
"""

import asyncio
import hashlib
import json
import logging
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import obstore
from dagster import Config, asset
from gtfs_zone_db_models.object_store import (
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
    ObjectStoreSettings,
)
from obstore.exceptions import BaseError
from pydantic_settings import SettingsConfigDict

from gtfs_zone_feed_catalog.assets.endpoint_checks import (
    CheckResult,
    Politeness,
    body_size,
    check_targets,
    error_class_of,
    http_date,
    sample_targets,
    static_keys,
)
from gtfs_zone_feed_catalog.catalog import Source
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.urls import host_of

log = logging.getLogger(__name__)

PREFIX = "gtfs/"
INDEX_KEY = "gtfs/index.json"
ZIP_CONTENT_TYPE = "application/zip"
CHUNK = 1 << 20

# Outcomes of one URL's last request.
OK = "ok"
NOT_MODIFIED = "not_modified"
TOO_LARGE = "too_large"
HTTP_ERROR = "http_error"
ERROR = "error"


class ZipStoreSettings(ObjectStoreSettings):
    """The schedule bucket: ObjectStoreSettings read from ZIPS_S3_* instead."""

    model_config = SettingsConfigDict(
        env_prefix="ZIPS_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    s3_bucket: str = "gtfs-zips"


@dataclass(frozen=True, slots=True)
class Stored:
    """One URL's entry in ``gtfs/index.json``."""

    # The URL as requested.
    url: str
    checked: str
    outcome: str
    status: int | None = None
    # The object holding the latest body; "" when none is stored.
    key: str = ""
    sha256: str = ""
    bytes: int | None = None
    # zip, html, json, xml, empty or unknown, from the body's first bytes.
    kind: str = ""
    content_type: str = ""
    # Header values as received, sent back as If-None-Match / If-Modified-Since.
    etag: str = ""
    last_modified: str = ""
    # When the body was last downloaded, and when its sha256 last changed.
    fetched: str = ""
    since: str = ""


INDEX_FIELDS = {
    "url": "url",
    "checked": "checked",
    "outcome": "outcome",
    "status": "status",
    "key": "key",
    "sha256": "sha256",
    "bytes": "bytes",
    "kind": "kind",
    "content_type": "contentType",
    "etag": "etag",
    "last_modified": "lastModified",
    "fetched": "fetched",
    "since": "since",
}


def to_json(entry: Stored) -> dict[str, Any]:
    return {name: getattr(entry, attr) for attr, name in INDEX_FIELDS.items()}


def from_json(data: dict[str, Any]) -> Stored:
    return Stored(
        **{attr: data[name] for attr, name in INDEX_FIELDS.items() if name in data}
    )


def read_index(store: ObjectStore) -> dict[str, Stored]:
    """The index by normalized URL, or an empty one on a new bucket."""
    try:
        urls = json.loads(store.get(INDEX_KEY)).get("urls", {})
    except ObjectNotFound:
        return {}
    return {key: from_json(entry) for key, entry in urls.items()}


def index_body(entries: dict[str, Stored], generated: datetime) -> bytes:
    return json.dumps(
        {
            "generated": generated.isoformat(),
            "maxBytes": settings.fetch_max_bytes,
            "urls": {key: to_json(entry) for key, entry in entries.items()},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def sniff(head: bytes) -> str:
    """What a body is from its first bytes: zip, empty, html, json, xml or unknown."""
    if head.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    head = head[:512].lstrip().lower()
    if not head:
        return "empty"
    if head.startswith((b"<!doctype html", b"<html")) or b"<html" in head:
        return "html"
    if head.startswith((b"{", b"[")):
        return "json"
    if head.startswith(b"<"):
        return "xml"
    return "unknown"


def object_key(sha256: str, kind: str, size: int) -> str:
    """Where a body is stored, or "" for a non-zip body too large to keep."""
    if kind == "zip":
        return f"{PREFIX}{sha256}.zip"
    if size <= settings.fetch_body_max_bytes:
        return f"{PREFIX}{sha256}.body"
    return ""


def conditional_headers(previous: Stored | None, have: set[str]) -> dict[str, str]:
    """Validators of the stored body, when there is one to fall back on."""
    if previous is None or not previous.key or previous.key not in have:
        return {}
    headers = {}
    if previous.etag:
        headers["If-None-Match"] = previous.etag
    if previous.last_modified:
        headers["If-Modified-Since"] = previous.last_modified
    return headers


@dataclass(slots=True)
class Download:
    """One GET's response, with its body in a temp file when it was a 200."""

    status: int | None = None
    error_class: str = ""
    final_url: str = ""
    content_type: str = ""
    content_length: int | None = None
    etag: str = ""
    last_modified: str = ""
    too_large: bool = False
    path: Path | None = None
    sha256: str = ""
    size: int = 0
    kind: str = ""
    # Until the headers arrived (or the request failed), not the whole body.
    latency_ms: int | None = None


async def download(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    workdir: str | None,
) -> Download:
    """GET ``url``, streaming a 200 body to a temp file the caller deletes."""
    started = time.monotonic()
    try:
        response = await client.send(
            client.build_request("GET", url, headers=headers), stream=True
        )
    except Exception as exc:
        if not isinstance(exc, httpx.HTTPError):
            log.warning("fetch of %r raised %s: %s", url, type(exc).__name__, exc)
        return Download(
            error_class=error_class_of(exc),
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    final_url = str(response.url)
    got = Download(
        status=response.status_code,
        final_url="" if final_url == url else final_url,
        content_type=response.headers.get("content-type", "").split(";")[0].strip(),
        content_length=body_size(response),
        etag=response.headers.get("etag", "").strip(),
        last_modified=response.headers.get("last-modified", "").strip(),
        latency_ms=int((time.monotonic() - started) * 1000),
    )
    try:
        if response.status_code != httpx.codes.OK:
            return got
        if (got.content_length or 0) > settings.fetch_max_bytes:
            got.too_large = True
            return got
        digest = hashlib.sha256()
        head = b""
        with tempfile.NamedTemporaryFile(
            dir=workdir, suffix=".download", delete=False
        ) as handle:
            got.path = Path(handle.name)
            async for chunk in response.aiter_bytes(CHUNK):
                got.size += len(chunk)
                if got.size > settings.fetch_max_bytes:
                    got.too_large = True
                    break
                if len(head) < 512:
                    head += chunk[: 512 - len(head)]
                digest.update(chunk)
                handle.write(chunk)
        if got.too_large:
            got.path.unlink(missing_ok=True)
            got.path = None
            return got
        got.sha256 = digest.hexdigest()
        got.kind = sniff(head)
        return got
    except Exception as exc:
        if not isinstance(exc, httpx.HTTPError):
            log.warning("fetch of %r raised %s: %s", url, type(exc).__name__, exc)
        if got.path is not None:
            got.path.unlink(missing_ok=True)
        return Download(
            status=got.status,
            error_class=error_class_of(exc),
            final_url=got.final_url,
            latency_ms=got.latency_ms,
        )
    finally:
        await response.aclose()


async def upload(store: ObjectStore, key: str, path: Path, content_type: str) -> bool:
    """Put a temp file at ``key``, multipart when large. False on failure."""
    try:
        await obstore.put_async(
            store.store, key, path, attributes={"Content-Type": content_type}
        )
    except Exception as exc:
        # One failed upload loses that body for the day, never the run.
        log.warning("upload of %s failed: %s: %s", key, type(exc).__name__, exc)
        return False
    return True


def settle(
    url: str,
    previous: Stored | None,
    got: Download,
    key: str,
    now: datetime,
) -> tuple[Stored, CheckResult]:
    """The index entry and the check a download makes. ``key`` is where a 200
    body is stored ("" when it is not)."""
    stamp = now.isoformat()
    check = CheckResult(
        url=url,
        ok=got.status is not None and got.status < 400 and not got.error_class,
        checked_at=now,
        status_code=got.status,
        final_url=got.final_url,
        content_type=got.content_type,
        content_length=got.content_length,
        last_modified=http_date(got.last_modified),
        etag=got.etag,
        latency_ms=got.latency_ms,
        method="GET",
    )
    kept = previous or Stored(url=url, checked=stamp, outcome=ERROR)

    if got.status is None:
        return (
            replace(kept, url=url, checked=stamp, outcome=ERROR, status=None),
            replace(check, error_class=got.error_class),
        )
    if got.status == httpx.codes.NOT_MODIFIED and kept.key:
        entry = replace(
            kept,
            url=url,
            checked=stamp,
            outcome=NOT_MODIFIED,
            status=got.status,
            etag=got.etag or kept.etag,
            last_modified=got.last_modified or kept.last_modified,
        )
        return entry, replace(
            check,
            content_type=kept.content_type,
            content_length=kept.bytes,
            last_modified=http_date(entry.last_modified),
            etag=entry.etag,
            sha256=kept.sha256,
        )
    if got.status != httpx.codes.OK or got.error_class:
        # The last stored body stays: it is still the latest one known.
        entry = replace(
            kept,
            url=url,
            checked=stamp,
            outcome=HTTP_ERROR if got.status >= 400 else ERROR,
            status=got.status,
        )
        error = got.error_class or (
            f"http_{got.status // 100}xx" if got.status >= 400 else ""
        )
        return entry, replace(check, error_class=error)

    validators = {"etag": got.etag, "last_modified": got.last_modified}
    if got.too_large:
        entry = Stored(
            url=url,
            checked=stamp,
            outcome=TOO_LARGE,
            status=got.status,
            bytes=got.content_length,
            content_type=got.content_type,
            **validators,
        )
        return entry, check

    changed = previous is None or previous.sha256 != got.sha256
    entry = Stored(
        url=url,
        checked=stamp,
        outcome=OK,
        status=got.status,
        key=key,
        sha256=got.sha256,
        bytes=got.size,
        kind=got.kind,
        content_type=got.content_type,
        fetched=stamp,
        since=stamp if changed else previous.since,
        **validators,
    )
    return entry, replace(check, content_length=got.size, sha256=got.sha256)


async def fetch_one(
    client: httpx.AsyncClient,
    store: ObjectStore,
    url: str,
    previous: Stored | None,
    have: set[str],
    politeness: Politeness,
    slots: asyncio.Semaphore,
) -> tuple[Stored, CheckResult]:
    # A fragment is the app's own nested-zip selector, never the server's.
    request_url = url.split("#", 1)[0].strip()
    host = host_of(request_url)
    workdir = settings.fetch_workdir or None
    got: Download | None = None
    acquired = False
    try:
        # Host lock first, global slot second, as in the checks. The slot is
        # held through the upload, so temp files on disk stay bounded by it;
        # the host lock is not, so the next request to the host can start.
        async with politeness.lock(host):
            await politeness.wait(host)
            await slots.acquire()
            acquired = True
            try:
                got = await download(
                    client, request_url, conditional_headers(previous, have), workdir
                )
            finally:
                politeness.done(host)

        key = ""
        if got.path is not None:
            key = object_key(got.sha256, got.kind, got.size)
            if key and key not in have:
                content_type = (
                    ZIP_CONTENT_TYPE
                    if got.kind == "zip"
                    else got.content_type or "application/octet-stream"
                )
                if await upload(store, key, got.path, content_type):
                    have.add(key)
                else:
                    key = ""
        return settle(url, previous, got, key, datetime.now(UTC))
    finally:
        if acquired:
            slots.release()
        if got is not None and got.path is not None:
            got.path.unlink(missing_ok=True)


async def fetch_all(
    store: ObjectStore,
    targets: dict[str, str],
    index: dict[str, Stored],
    have: set[str],
) -> dict[str, tuple[Stored, CheckResult]]:
    politeness = Politeness(settings.check_per_host_delay_seconds)
    slots = asyncio.Semaphore(settings.fetch_concurrency)

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.check_user_agent},
        timeout=httpx.Timeout(settings.check_timeout_seconds),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=settings.fetch_concurrency),
    ) as client:

        async def one(key: str, url: str) -> tuple[str, tuple[Stored, CheckResult]]:
            return key, await fetch_one(
                client, store, url, index.get(key), have, politeness, slots
            )

        results = await asyncio.gather(*(one(k, u) for k, u in targets.items()))
    return dict(results)


def write_index(
    store: ObjectStore,
    entries: dict[str, Stored],
    have: set[str],
    now: datetime,
) -> int:
    """Write the index, then delete objects it does not reference. Returns how
    many were deleted."""
    store.put(INDEX_KEY, index_body(entries, now), content_type="application/json")
    referenced = {entry.key for entry in entries.values() if entry.key}
    orphans = sorted(have - referenced)
    if orphans:
        try:
            obstore.delete(store.store, orphans)
        except FileNotFoundError:
            pass
        except (BaseError, OSError) as exc:
            raise ObjectStoreError(f"deleting orphans failed: {exc}") from exc
    return len(orphans)


def fetch(
    store: ObjectStore, sources: list[Source], limit: int = 0
) -> dict[str, CheckResult]:
    """Fetch every static URL (or ``limit`` of them), update the bucket, and
    return the checks by normalized URL."""
    targets, _ = check_targets(sources)
    static = static_keys(sources)
    targets = {key: url for key, url in targets.items() if key in static}
    if limit:
        log.warning("trial run: fetching %d of %d schedules", limit, len(targets))
        targets = sample_targets(targets, limit)

    try:
        index = read_index(store)
        have = set(store.list_prefix(PREFIX)) - {INDEX_KEY}
    except ObjectStoreError as exc:
        log.warning("schedule bucket unavailable, fetching nothing: %s", exc)
        return {}
    log.info(
        "fetching %d schedules; %d indexed, %d objects stored",
        len(targets),
        len(index),
        len(have),
    )

    results = asyncio.run(fetch_all(store, targets, index, have))

    # URLs no longer listed drop out; URLs not fetched this run keep their entry.
    entries = {key: entry for key, entry in index.items() if key in static}
    entries |= {key: entry for key, (entry, _) in results.items()}
    now = datetime.now(UTC)
    try:
        deleted = write_index(store, entries, have, now)
    except ObjectStoreError as exc:
        log.warning("index not written, nothing deleted: %s", exc)
        deleted = 0

    outcomes: dict[str, int] = {}
    for entry, _ in results.values():
        outcomes[entry.outcome] = outcomes.get(entry.outcome, 0) + 1
    log.info("fetch outcomes: %s; %d orphans deleted", outcomes, deleted)
    return {key: check for key, (_, check) in results.items()}


class FeedFetchConfig(Config):
    """Run config for a trial run."""

    # Fetch at most this many schedules, spread across hosts; 0 fetches all.
    # The rest are checked by endpoint_checks as usual.
    limit: int = 0


@asset(description="Download every schedule, store the latest body per URL")
def feed_fetch(
    config: FeedFetchConfig, sources: list[Source]
) -> dict[str, CheckResult]:
    zip_settings = ZipStoreSettings()
    if not zip_settings.s3_endpoint:
        log.warning("ZIPS_S3_ENDPOINT is unset; no schedules fetched")
        return {}
    return fetch(ObjectStore(zip_settings), sources, config.limit)
