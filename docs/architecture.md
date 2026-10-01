# Architecture

## Assets

Assets live one per module under `src/gtfs_zone_feed_catalog/assets/`, in dependency
order:

| Asset | What it does |
|---|---|
| `transitland_atlas` | Parse the DMFR corpus: a sibling checkout in dev, the GitHub tree API in the cluster |
| `mobility_database` | Exchange the refresh token at `POST /v1/tokens/access`, then page `/v1/gtfs_feeds` and `/v1/gtfs_rt_feeds` |
| `rt_api` | The feeds rt.gtfs.zone serves, from rt-api's public `GET /feeds` (`RT_API_CATALOG_URL`) |
| `sources` | One row per source kind, ids namespaced by catalog, cross-linked by normalized URL |
| `endpoint_checks` | HEAD (ranged GET on fallback) every download URL; size (static only), `Last-Modified`, `ETag` |
| `check_history` | Fold the checks into one answer per source; record each URL's check and state changes in Postgres |
| `feed_contents` | Read timetable-sites's content report (what each schedule's download held: ok, not_zip, ...; feed_info, service range, counts) and record it per URL, with outcome changes |
| `feeds` | Group rows into logical feeds (one per transit system) with persisted, sticky ids |
| `history_retention` | Delete old `endpoint_state` and `endpoint_content_state` rows, and absent sources, endpoints and feeds, past their window |
| `bucket_cors` | Idempotent CORS rule on the public bucket, set over S3 because Garage's admin API cannot |
| `published_artifacts` | Write the JSON artifacts and dated snapshots to the public bucket |

## Artifacts

The published documents, all shaped in `artifacts.py`:

| Artifact | What it holds |
|---|---|
| `feeds.json` | One entry per logical feed: `members` (row ids), `urls` (role to URLs, best first), `state`, `roleState`, `auth` (roles only behind a key), `staticBytes`, `lastModified`, `since`, `content` (last download's outcome and, when ok, service range, publisher, version and counts; only for feeds timetable-sites builds), `licenses` (members' license URLs), `catalogLinks` (members' Transitland / Mobility Database pages), place. What a load list reads |
| `sources.json` | The raw catalog rows, field-compatible with the old `atlas-feeds.json` |
| `status.json` | Per-row check facts (code, error, latency, failures, since); also what a snapshot hashes |
| `search.json` | `feeds.json` cut down, with short keys, to what a feed picker lists and searches: name, subtitle, alt names (rows', operators', agencies'), place, state, `roleState`, URLs, auth, size, date. What every app's picker and list.gtfs.zone's first paint fetch |
| `summary.json` | Counts by catalog, kind, state and country, for rows and under `feeds` for feeds |
| `manifest.json` | sha256 and size of every other artifact; written last |
| `pages/feed/<feedId>/{head,body}.html` | Per-feed HTML fragments (`pages.py`) that list.gtfs.zone's nginx includes into `/feed/<feedId>/<slug>`; rewritten only when their hash in `pages/index.json` changes |
| `sitemap.xml` | Every indexable feed page, `lastmod` the day its fragments last changed |

`sources.json`, `feeds.json` and `manifest.json` carry a top-level `attribution`
(`artifacts.ATTRIBUTION`): Transitland Atlas is CC-BY 4.0 and must be credited
with a link; the Mobility Database catalog is CC0. Feed contents are licensed by
their publishers.

## History storage

Storage is shaped so it does not grow with feeds x days: `endpoint_state` holds
one row per **state change**, not per check, so a URL up for a year is one row.

History is keyed by **normalized URL** (`endpoint`), because the URL is what is
checked. Catalog rows (`source`, linked by `source_endpoint`) and logical feeds
(`feed`, `feed_member`) have no stored state: both derive it from their URLs. A
row is up when all its URLs answer. A feed has a per-role state (up when any
URL for that role answers, so a consumer can load it) and an overall state (up
only when every checkable URL answers); each consumer applies its own rule.

## Logical feeds

Catalog rows stay the raw layer; `feeds` builds on top of them. A union-find
joins rows on exactly four kinds of evidence:

- a shared normalized URL, any kind, any catalog;
- an MDB realtime row's `feed_references` to an MDB static row;
- realtime URLs on the same host and path differing only in a final
  `vehicles`/`vehiclepositions`/`trips`/`tripupdates`/`alerts`/`servicealerts`
  segment (case, `_`/`-` and extension ignored; the query must match);
- the static and rt rows one catalog feed was split into.

No fuzzy name matching and no override file yet, so the rules **can
false-merge** (a regional schedule that several agencies' realtime feeds all
reference makes them one feed; PTV's nested-zip feeds sharing one download are
one feed) and a wrong link can only be undone by changing the rules. Names:
a Transitland operator, then a Mobility Database provider (its feed_name
becomes the feed's `subtitle`), then the schedule's own single agency or
publisher from the content report, then any row's name. Coordinates: any
placed member.

Feed ids (`f-<10 hex>`) go in shareable URLs, so they are persisted and sticky:
each group takes the id of the existing feed it shares the most members with
(greedy, largest overlap first; on a split the larger part keeps the id), and
a new id is minted only for a group sharing none. A feed whose group vanishes
is kept absent, with its members, for the absent-retention window, so it gets
its id back if the group returns.

Coordinates come from the Mobility Database only. DMFR carries no place data, so
Transitland-only rows are unplaced unless a cross-link supplies coordinates. The
unplaced count is published and shown, not hidden.

## Environment variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Postgres for check history and Dagster run storage |
| `DAGSTER_HOME` | Dagster instance directory; must be writable by the runtime user |
| `MOBILITY_DB_REFRESH_TOKEN` | Mobility Database refresh token; exchanged per run, never logged |
| `MOBILITY_DB_BASE_URL` | Mobility Database API base (default `https://api.mobilitydatabase.org/v1`) |
| `TRANSITLAND_ATLAS_PATH` | Sibling DMFR checkout; blank falls back to the GitHub tree API |
| `TRANSITLAND_ATLAS_REPO` | DMFR repo for the API fallback (default `transitland/transitland-atlas`) |
| `CHECK_CONCURRENCY` | Global cap on in-flight endpoint checks (default 16) |
| `CHECK_PER_HOST_DELAY_SECONDS` | Pause between requests to one host (default 1.0) |
| `CHECK_TIMEOUT_SECONDS` | Per-request timeout (default 30) |
| `CHECK_USER_AGENT` | Sent on every check; names the project and a contact URL |
| `STATE_RETENTION_DAYS` | Age at which `endpoint_state` and `endpoint_content_state` rows are deleted (default 400) |
| `SOURCE_ABSENT_RETENTION_DAYS` | Days absent before a `source`, `endpoint` or `feed` row is deleted (default 90) |
| `SNAPSHOT_DAILY_DAYS` | Days of daily snapshots kept in full (default 30) |
| `SNAPSHOT_WEEKLY_DAYS` | Days after which snapshots thin to one per month (default 365) |
| `CONTENT_REPORT_BASE` | Where timetable-sites's `_content/index.json` is served (default `https://sites.gtfs.zone/`); blank skips it |
| `RT_API_CATALOG_URL` | rt-api's public feed catalog (default `https://rt.gtfs.zone/feeds`); blank skips it |
| `GATUS_URL` | Gatus base URL; after a successful publish the run pushes a heartbeat there. Blank skips it |
| `GATUS_ENDPOINT_KEY` | Gatus external endpoint key (default `data_catalog-publish`) |
| `GATUS_TOKEN` | Bearer token for that external endpoint |
| `S3_ENDPOINT` `S3_BUCKET` `S3_ACCESS_KEY` `S3_SECRET_KEY` `S3_REGION` | Public artifact bucket; read by `gtfs_zone_db_models.object_store`, not by this repo's `Settings` |
