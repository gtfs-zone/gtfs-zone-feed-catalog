"""The one row shape every catalog is normalized into.

A row is one *source kind*, not one feed: an agency that publishes both a zip
and a realtime endpoint is two rows, because the consuming apps pin one of each
and must be able to select them separately. That is the same split
``interlocking/scripts/generate-atlas-data.ts`` made, and the published
``sources.json`` keeps its field names so a consumer can move over without a
rewrite.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Literal

from geometry_car.urls import is_absolute

Kind = Literal["static", "rt"]

# Which URL roles belong to which kind. A static row carries exactly one URL; an
# rt row carries up to three, and needs at least one.
STATIC_ROLES = ("scheduled",)
RT_ROLES = ("vehicles", "trip_updates", "alerts")

# Field names the old public/atlas-feeds.json used, by role.
ATLAS_URL_KEYS = {
    "scheduled": "scheduledUrl",
    "vehicles": "vehiclesUrl",
    "trip_updates": "tripUpdatesUrl",
    "alerts": "alertsUrl",
}


@dataclass(frozen=True, slots=True)
class Place:
    country_code: str = ""
    country: str = ""
    subdivision: str = ""
    municipality: str = ""
    # Centroid of the feed's bounding box. The Mobility Database is the only
    # catalog that carries coordinates at all; DMFR has none.
    latitude: float | None = None
    longitude: float | None = None
    # (min_lat, min_lon, max_lat, max_lon), kept so a map can frame a feed.
    bbox: tuple[float, float, float, float] | None = None

    @property
    def placed(self) -> bool:
        return self.latitude is not None and self.longitude is not None


@dataclass(frozen=True, slots=True)
class Source:
    """One selectable endpoint set, from one catalog."""

    source_id: str
    catalog: Literal["transitland", "mobilitydatabase", "curated"]
    kind: Kind
    # The catalog's own id, unnamespaced: a DMFR onestop id, an mdb-NNNN, or a
    # curated slug.
    feed_id: str
    name: str
    operator_name: str = ""
    # Where the row came from: the DMFR filename's domain, or the MDB provider.
    origin: str = ""
    # role -> URL. Curated realtime URLs may be path-only; see examples.yaml.
    urls: dict[str, str] = field(default_factory=dict)
    place: Place = field(default_factory=Place)
    # MDB feed lifecycle: active / deprecated / inactive / development / future.
    status: str = ""
    # MDB source_info.authentication_type: 0 or absent is open, 1 and 2 need a
    # key we do not hold, so those endpoints are never checked.
    authentication_type: int = 0
    license_url: str = ""
    # Source ids in *other* catalogs whose normalized download URL matches this
    # one. Not a merge: both rows stay, cross-referenced.
    same_endpoint_as: tuple[str, ...] = ()
    # Free text carried from the curated set, where it is the expensive part.
    note: str = ""

    @property
    def roles(self) -> tuple[str, ...]:
        return STATIC_ROLES if self.kind == "static" else RT_ROLES

    @property
    def check_urls(self) -> tuple[str, ...]:
        """Absolute URLs worth checking, in role order.

        Path-only curated realtime URLs are excluded: they resolve against each
        app's own RT base at fetch time and there is no one host to check.
        """
        return tuple(
            url for role in self.roles if is_absolute(url := self.urls.get(role, ""))
        )

    def with_place(self, place: Place) -> Source:
        return replace(self, place=place)

    def with_links(self, links: tuple[str, ...]) -> Source:
        return replace(self, same_endpoint_as=links)


def make_rows(
    *,
    catalog: str,
    feed_id: str,
    id_prefix: str,
    name: str,
    operator_name: str = "",
    origin: str = "",
    scheduled: str = "",
    vehicles: str = "",
    trip_updates: str = "",
    alerts: str = "",
    **common: object,
) -> list[Source]:
    """Split one catalog feed into its static and rt rows.

    ``id_prefix`` is the catalog namespace (``tl``, ``md``, ``curated``). A
    static row's id ends ``:static`` and an rt row's ``:rt`` only where one feed
    can produce both; the Mobility Database gives realtime its own mdb id, so
    it passes the kind suffix it wants in ``feed_id``.
    """
    # Catalogs carry stray whitespace around URLs; httpx reads a leading space
    # as a relative path.
    scheduled, vehicles, trip_updates, alerts = (
        u.strip() for u in (scheduled, vehicles, trip_updates, alerts)
    )
    base = {
        "catalog": catalog,
        "feed_id": feed_id,
        "name": name,
        "operator_name": operator_name,
        "origin": origin,
        **common,
    }
    rows: list[Source] = []
    if scheduled:
        rows.append(
            Source(
                source_id=f"{id_prefix}:{feed_id}:static",
                kind="static",
                urls={"scheduled": scheduled},
                **base,  # type: ignore[arg-type]
            )
        )
    rt_urls = {
        role: url
        for role, url in (
            ("vehicles", vehicles),
            ("trip_updates", trip_updates),
            ("alerts", alerts),
        )
        if url
    }
    if rt_urls:
        rows.append(
            Source(
                source_id=f"{id_prefix}:{feed_id}:rt",
                kind="rt",
                urls=rt_urls,
                **base,  # type: ignore[arg-type]
            )
        )
    return rows
