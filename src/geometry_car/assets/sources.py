"""The three catalogs merged into one list - cross-linked, not collapsed.

Transitland and the Mobility Database describe overlapping worlds with
different ids, different names and different metadata, and neither is a superset
of the other. Collapsing them would mean choosing whose name and whose id wins
for every overlap, and quietly losing the loser. So both rows stay, and a row
carries ``same_endpoint_as``: the ids in *other* catalogs whose normalized
download URL is the same. A consumer that wants one row per endpoint can fold
on that; a consumer that wants to show which catalogs agree can show it.

Coordinates travel along those links, because the Mobility Database is the only
catalog that has any. A Transitland row cross-linked to a placed MDB row gets
its place; everything else stays unplaced, and the count of unplaced rows is
published rather than hidden.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from dagster import asset

from geometry_car.assets.curated_examples import CuratedExample, example_sources
from geometry_car.catalog import Source
from geometry_car.urls import normalize_url

log = logging.getLogger(__name__)


def normalized_urls(source: Source) -> set[str]:
    return {key for url in source.urls.values() if (key := normalize_url(url))}


def cross_link(rows: list[Source]) -> list[Source]:
    """Attach ``same_endpoint_as`` and propagate places along the links."""
    by_url: dict[str, list[Source]] = defaultdict(list)
    for row in rows:
        for key in normalized_urls(row):
            by_url[key].append(row)

    linked: list[Source] = []
    for row in rows:
        matches: dict[str, Source] = {}
        for key in normalized_urls(row):
            for other in by_url[key]:
                # Only across catalogs: two rows of the same catalog sharing a
                # URL are a catalog's own duplicate, not corroboration.
                if other.catalog != row.catalog and other.kind == row.kind:
                    matches[other.source_id] = other

        result = row.with_links(tuple(sorted(matches)))
        if not result.place.placed:
            placed = next(
                (other.place for other in matches.values() if other.place.placed), None
            )
            if placed is not None:
                result = result.with_place(placed)
        linked.append(result)

    return linked


def merge(*groups: list[Source]) -> list[Source]:
    """One row per source id, first catalog listed wins a collision."""
    by_id: dict[str, Source] = {}
    for group in groups:
        for row in group:
            by_id.setdefault(row.source_id, row)
    return sorted(by_id.values(), key=lambda r: r.source_id)


@asset(
    description="Every catalog's rows, deduplicated, cross-linked and placed",
)
def sources(
    transitland_atlas: list[Source],
    mobility_database: list[Source],
    curated_examples: list[CuratedExample],
) -> list[Source]:
    rows = cross_link(
        merge(transitland_atlas, mobility_database, example_sources(curated_examples))
    )

    placed = sum(1 for row in rows if row.place.placed)
    cross_linked = sum(1 for row in rows if row.same_endpoint_as)
    log.info(
        "%d sources, %d placed, %d unplaced, %d cross-linked",
        len(rows),
        placed,
        len(rows) - placed,
        cross_linked,
    )
    return rows
