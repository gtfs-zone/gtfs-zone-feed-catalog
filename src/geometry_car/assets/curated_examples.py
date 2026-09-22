"""The hand-curated example feeds, as source data rather than compiled code.

These used to be ``interlocking/src/gtfs/examples.ts``, where every entry's rot
note was learned one outage at a time and nothing watched them. They live here
so the daily check covers them, and they are published as ``examples.json`` for
the apps to fetch.

The notes in ``data/examples.yaml`` are the expensive part of this file and are
carried through to the published artifact verbatim. Several entries are known
to fail a bare request and work perfectly through the app's proxy, so a red
status on one of those is information, not a defect.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dagster import asset

from geometry_car.catalog import Source, make_rows

log = logging.getLogger(__name__)

EXAMPLES_PATH = Path(__file__).resolve().parent.parent / "data" / "examples.yaml"


@dataclass(frozen=True, slots=True)
class CuratedExample:
    slug: str
    name: str
    description: str = ""
    note: str = ""
    # The interlocking FeedSelection halves, kept in the shape the apps want:
    # {url, use_cors, label} and {vehicles, trip_updates, alerts, use_cors, label}.
    scheduled: dict[str, Any] = field(default_factory=dict)
    realtime: dict[str, Any] = field(default_factory=dict)


def load_examples(path: Path = EXAMPLES_PATH) -> list[CuratedExample]:
    document = yaml.safe_load(path.read_text()) or {}
    return [
        CuratedExample(
            slug=entry["slug"],
            name=entry["name"],
            description=entry.get("description", ""),
            note=entry.get("note", ""),
            scheduled=entry.get("scheduled") or {},
            realtime=entry.get("realtime") or {},
        )
        for entry in document.get("examples") or []
    ]


def example_sources(examples: list[CuratedExample]) -> list[Source]:
    rows: list[Source] = []
    for example in examples:
        rows.extend(
            make_rows(
                catalog="curated",
                id_prefix="curated",
                feed_id=example.slug,
                name=example.name,
                operator_name=example.name,
                origin="gtfs.zone",
                scheduled=example.scheduled.get("url", ""),
                vehicles=example.realtime.get("vehicles", ""),
                trip_updates=example.realtime.get("trip_updates", ""),
                alerts=example.realtime.get("alerts", ""),
                note=example.note,
            )
        )
    return rows


@asset(group_name="catalogs", description="Hand-curated example feeds")
def curated_examples() -> list[CuratedExample]:
    examples = load_examples()
    log.info("curated: %d examples", len(examples))
    return examples
