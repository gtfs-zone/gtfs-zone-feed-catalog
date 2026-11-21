"""How much the catalogs overlap, as metadata on the asset in the Dagster UI.

Counts logical feeds with a schedule, by the set of catalogs among their
members, and per catalog pair the feeds both list, the schedule URLs both list
(``urls.link_key``) and the schedule bodies both list (same sha256). Numbers
only: nothing is published or stored, and the numeric entries are plotted
across materializations by the UI.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import combinations

from dagster import MarkdownMetadataValue, MaterializeResult, asset

from gtfs_zone_feed_catalog.assets.feed_contents import FeedContent
from gtfs_zone_feed_catalog.assets.feeds import Feed, zip_sha256
from gtfs_zone_feed_catalog.catalog import Source
from gtfs_zone_feed_catalog.urls import link_key

Pair = tuple[str, str]


@dataclass(slots=True)
class Overlap:
    # Feeds with a schedule, by the sorted catalogs among their members.
    combos: Counter[tuple[str, ...]] = field(default_factory=Counter)
    # Feeds with a schedule that list this catalog, and that list only it.
    listed: Counter[str] = field(default_factory=Counter)
    alone: Counter[str] = field(default_factory=Counter)
    feeds: Counter[Pair] = field(default_factory=Counter)
    urls: Counter[Pair] = field(default_factory=Counter)
    bodies: Counter[Pair] = field(default_factory=Counter)


def _pairs(catalogs: set[str]) -> list[Pair]:
    return list(combinations(sorted(catalogs), 2))


def overlap(
    sources: list[Source], feeds: list[Feed], contents: dict[str, FeedContent]
) -> Overlap:
    catalog_of = {row.source_id: row.catalog for row in sources}
    result = Overlap()
    for feed in feeds:
        if "scheduled" not in feed.urls:
            continue
        catalogs = {catalog_of[m] for m in feed.members if m in catalog_of}
        if not catalogs:
            continue
        result.combos[tuple(sorted(catalogs))] += 1
        result.listed.update(catalogs)
        if len(catalogs) == 1:
            result.alone.update(catalogs)
        result.feeds.update(_pairs(catalogs))

    by_url: dict[str, set[str]] = defaultdict(set)
    by_body: dict[str, set[str]] = defaultdict(set)
    for row in sources:
        if not (url := row.urls.get("scheduled")):
            continue
        if key := link_key(url):
            by_url[key].add(row.catalog)
        if sha := zip_sha256(url, contents):
            by_body[sha].add(row.catalog)
    for catalogs in by_url.values():
        result.urls.update(_pairs(catalogs))
    for catalogs in by_body.values():
        result.bodies.update(_pairs(catalogs))
    return result


def markdown(result: Overlap) -> str:
    pairs = sorted(set(result.feeds) | set(result.urls) | set(result.bodies))
    lines = [
        "| Catalogs | Feeds |",
        "|---|---:|",
        *(
            f"| {' + '.join(combo)} | {n} |"
            for combo, n in sorted(result.combos.items(), key=lambda kv: -kv[1])
        ),
        "",
        "| Catalog | Feeds | Only here |",
        "|---|---:|---:|",
        *(
            f"| {c} | {n} | {result.alone[c]} ({result.alone[c] / n:.0%}) |"
            for c, n in sorted(result.listed.items())
        ),
        "",
        "| Pair | Feeds in both | Schedule URLs in both | Bodies in both |",
        "|---|---:|---:|---:|",
        *(
            f"| {a} + {b} | {result.feeds[a, b]} | {result.urls[a, b]}"
            f" | {result.bodies[a, b]} |"
            for a, b in pairs
        ),
    ]
    return "\n".join(lines)


@asset(description="How many feeds, schedule URLs and bodies the catalogs share")
def source_overlap(
    sources: list[Source],
    feeds: list[Feed],
    feed_contents: dict[str, FeedContent],
) -> MaterializeResult:
    result = overlap(sources, feeds, feed_contents)
    metadata: dict = {"overview": MarkdownMetadataValue(markdown(result))}
    for catalog, n in result.listed.items():
        metadata[f"only_{catalog}"] = result.alone[catalog]
        metadata[f"feeds_{catalog}"] = n
    for a, b in result.feeds:
        metadata[f"both_{a}_{b}"] = result.feeds[a, b]
    return MaterializeResult(metadata=metadata)
