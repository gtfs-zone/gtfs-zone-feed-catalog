"""The published JSON documents, as pure functions.

Everything here returns bytes or plain data and touches no network, so the
shape of what consumers fetch is testable without a bucket.

``sources.json`` keeps the field names of the old ``public/atlas-feeds.json``
(`rowId`, `feedId`, `name`, `operator_name`, `source`, `scheduledUrl`,
`vehiclesUrl`, `tripUpdatesUrl`, `alertsUrl`) as a compatible subset, so a
consumer moves over by changing where it fetches rather than how it reads. The
new fields sit alongside in snake_case. ``rowId`` values are now namespaced by
catalog (`tl:`, `md:`, `curated:`), which is the one deliberate break.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from geometry_car.catalog import ATLAS_URL_KEYS, Source

if TYPE_CHECKING:
    from geometry_car.assets.check_history import SourceStatus
    from geometry_car.assets.curated_examples import CuratedExample

ARTIFACT_CONTENT_TYPE = "application/json"
SNAPSHOT_CONTENT_TYPE = "application/gzip"
SNAPSHOT_PREFIX = "snapshots/"
SNAPSHOT_INDEX_KEY = "snapshots/index.json"


def dumps(payload: object) -> bytes:
    """Compact, key-sorted JSON. Sorted so two equal documents hash equal."""
    return json.dumps(
        payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    ).encode()


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def source_row(source: Source, status: SourceStatus | None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "rowId": source.source_id,
        "kind": source.kind,
        "feedId": source.feed_id,
        "name": source.name,
        "operator_name": source.operator_name,
        "source": source.origin,
        "catalog": source.catalog,
    }
    for role, url in source.urls.items():
        row[ATLAS_URL_KEYS[role]] = url

    place = source.place
    if place.country_code:
        row["country_code"] = place.country_code
    if place.country:
        row["country"] = place.country
    if place.subdivision:
        row["subdivision"] = place.subdivision
    if place.municipality:
        row["municipality"] = place.municipality
    if place.placed:
        row["lat"] = round(place.latitude, 5)
        row["lon"] = round(place.longitude, 5)
    if place.bbox:
        row["bbox"] = [round(v, 5) for v in place.bbox]

    if source.same_endpoint_as:
        row["same_endpoint_as"] = list(source.same_endpoint_as)
    if source.status:
        row["feed_status"] = source.status
    if source.authentication_type:
        row["auth"] = source.authentication_type
    if source.license_url:
        row["license_url"] = source.license_url
    if source.note:
        row["note"] = source.note
    if status is not None:
        row["state"] = status.state
    return row


def sources_document(
    sources: list[Source], statuses: dict[str, SourceStatus], generated_at: datetime
) -> bytes:
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "count": len(sources),
            "sources": [source_row(s, statuses.get(s.source_id)) for s in sources],
        }
    )


def status_entries(statuses: dict[str, SourceStatus]) -> dict[str, dict[str, Any]]:
    """The small document a UI polls: reachability and nothing else."""
    entries: dict[str, dict[str, Any]] = {}
    for source_id, status in statuses.items():
        entry: dict[str, Any] = {"state": status.state}
        if status.status_code is not None:
            entry["code"] = status.status_code
        if status.error_class:
            entry["error"] = status.error_class
        if status.final_url:
            entry["final_url"] = status.final_url
        if status.latency_ms is not None:
            entry["latency_ms"] = status.latency_ms
        if status.consecutive_failures:
            entry["failures"] = status.consecutive_failures
        if status.since is not None:
            entry["since"] = status.since.isoformat()
        entries[source_id] = entry
    return entries


def status_document(statuses: dict[str, SourceStatus], generated_at: datetime) -> bytes:
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "sources": status_entries(statuses),
        }
    )


def example_document(
    examples: list[CuratedExample],
    statuses: dict[str, SourceStatus],
    generated_at: datetime,
) -> bytes:
    """The curated set in the shape interlocking's FeedSelection already has.

    Emitted ready to use rather than as raw rows, because the whole point of
    this set is that picking one entry is a complete, working choice.
    """
    entries = []
    for example in examples:
        scheduled = example.scheduled
        realtime = example.realtime
        selection: dict[str, Any] = {
            "scheduled": {
                "kind": "url",
                "url": scheduled.get("url", ""),
                "useCors": bool(scheduled.get("use_cors")),
                "label": scheduled.get("label", example.name),
            }
            if scheduled
            else None,
            "realtime": {
                key: realtime[role]
                for role, key in (
                    ("vehicles", "vehiclesUrl"),
                    ("trip_updates", "tripUpdatesUrl"),
                    ("alerts", "alertsUrl"),
                )
                if realtime.get(role)
            }
            | {
                "useCors": bool(realtime.get("use_cors")),
                "label": realtime.get("label", f"{example.name} RT"),
            }
            if realtime
            else None,
        }
        entry: dict[str, Any] = {
            "slug": example.slug,
            "name": example.name,
            "description": example.description,
            "selection": selection,
            "state": {
                half: getattr(
                    statuses.get(f"curated:{example.slug}:{suffix}"), "state", "unknown"
                )
                for half, suffix in (("scheduled", "static"), ("realtime", "rt"))
            },
        }
        if example.note:
            entry["note"] = example.note
        entries.append(entry)

    return dumps({"generated_at": generated_at.isoformat(), "examples": entries})


def summary_document(
    sources: list[Source], statuses: dict[str, SourceStatus], generated_at: datetime
) -> bytes:
    by_catalog = Counter(s.catalog for s in sources)
    by_kind = Counter(s.kind for s in sources)
    by_state = Counter(
        getattr(statuses.get(s.source_id), "state", "unknown") for s in sources
    )
    by_country = Counter(s.place.country_code for s in sources if s.place.country_code)
    placed = sum(1 for s in sources if s.place.placed)

    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "total": len(sources),
            "by_catalog": dict(by_catalog),
            "by_kind": dict(by_kind),
            "by_state": dict(by_state),
            "by_country": dict(by_country),
            "placed": placed,
            # Published, not hidden: most of the corpus has no coordinates at
            # all, and a map that silently drops them would be a lie.
            "unplaced": len(sources) - placed,
        }
    )


def snapshot_payload(statuses: dict[str, SourceStatus]) -> bytes:
    """What a snapshot's identity is hashed over.

    Deliberately excludes the timestamp: a day on which nothing changed must
    hash the same as the day before, or the dedupe never fires.
    """
    return dumps(status_entries(statuses))


def snapshot_body(payload: bytes, generated_at: datetime) -> bytes:
    return gzip.compress(
        dumps(
            {"generated_at": generated_at.isoformat(), "sources": json.loads(payload)}
        ),
        mtime=0,
    )


def snapshot_key(day: date) -> str:
    return f"{SNAPSHOT_PREFIX}{day.isoformat()}.json.gz"


def snapshots_to_delete(
    index: list[dict[str, Any]], today: date, *, daily_days: int, weekly_days: int
) -> list[str]:
    """Which snapshot keys retention drops.

    Dailies in full for ``daily_days``, then one per ISO week out to
    ``weekly_days``, then one per calendar month. The kept one in a period is
    always the oldest, so a kept date never moves once chosen.
    """
    daily_cutoff = today - timedelta(days=daily_days)
    weekly_cutoff = today - timedelta(days=weekly_days)

    kept_periods: set[tuple[str, Any]] = set()
    drop: list[str] = []
    for entry in sorted(index, key=lambda e: e["date"]):
        day = date.fromisoformat(entry["date"])
        if day >= daily_cutoff:
            continue
        period = (
            ("week", day.isocalendar()[:2])
            if day >= weekly_cutoff
            else ("month", (day.year, day.month))
        )
        if period in kept_periods:
            drop.append(entry["key"])
        else:
            kept_periods.add(period)
    return drop


def manifest_document(
    artifacts: dict[str, bytes], generated_at: datetime, run_id: str
) -> bytes:
    """What a consumer polls to decide whether to refetch anything."""
    return dumps(
        {
            "generated_at": generated_at.isoformat(),
            "run_id": run_id,
            "artifacts": {
                name: {"sha256": sha256(body), "bytes": len(body)}
                for name, body in sorted(artifacts.items())
            },
        }
    )


def utcnow() -> datetime:
    return datetime.now(UTC)
