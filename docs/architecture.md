# Architecture

## Assets

Assets live one per module under `src/gtfs_zone_feed_catalog/assets/`, in dependency
order:

| Asset | What it does |
|---|---|
| `transitland_atlas` | Parse the DMFR corpus: a sibling checkout in dev, the GitHub tree API in the cluster |
| `mobility_database` | Exchange the refresh token at `POST /v1/tokens/access`, then page `/v1/gtfs_feeds` and `/v1/gtfs_rt_feeds` |
| `rt_api` | The feeds rt.gtfs.zone serves, from rt-api's public `GET /feeds` (`RT_API_CATALOG_URL`) |
| `ntd` | FTA's NTD GTFS weblinks, one GET of the SODA3 view (`NTD_WEBLINKS_URL`); one static row per NTD id and normalized weblink |
| `sources` | One row per source kind, ids namespaced by catalog, cross-linked by URL with http and https unified |
| `feed_fetch` | Conditional GET of every static URL; the latest body per URL in the private schedule bucket (see [Schedule store](#schedule-store)); the response is that URL's check |
| `endpoint_checks` | HEAD (ranged GET on fallback) every download URL `feed_fetch` did not request; size (static only), `Last-Modified`, `ETag` |
| `check_history` | Fold the checks into one answer per source; record each URL's check and state changes in Postgres |
| `feed_facts` | Scan every stored zip once per sha256 (`gtfs_zone_feed_fetch.facts.scan`, in memory-capped workers): feed_info, service range, agencies, counts, languages, location and place tags (municipalities from a copy of the stored locality file), the cleaned-data hash, kept as `gtfs/<sha256>.facts.json`, and the agency fingerprints from the same scan as `gtfs/<sha256>.fingerprint.parquet`; a zip with current facts and no fingerprint is scanned again only when `overlaps` could pair it |
| `feed_contents` | What each schedule's download held (ok, not_zip, missing_files, ...; feed_info, service range, counts, place tags): from `feed_facts`, else timetable-sites's content report; recorded per URL, with outcome changes |
| `feeds` | Group rows into logical feeds (one per transit system) with persisted, sticky ids; agencies and overlaps per feed; metadata: cleaned-hash joins, overlap counts and every rename against the stored name |
| `source_overlap` | Feeds with a schedule by catalog combination, and per catalog pair the feeds, schedule URLs and bodies (sha256) both list; metadata in the Dagster UI only |
| `history_retention` | Delete old `endpoint_state`, `endpoint_content_state` and `endpoint_version` rows, and absent sources, endpoints and feeds, past their window |
| `published_artifacts` | Write the JSON artifacts and dated snapshots to the public bucket; reads `gtfs/index.json` for `source-stats.json` |
| `localities` | Own weekly job (`weekly_localities`, a heavy run): when Overture has a release newer than the stored one, build the locality file (`gtfs_zone_feed_fetch.localities.build`) into the schedule bucket for the municipality tags of `feed_facts` and timetable-sites |

## Artifacts

The published documents, all shaped in `artifacts.py`:

| Artifact | What it holds |
|---|---|
| `feeds.json` | One entry per logical feed: `members` (row ids), `urls` (role to URLs, best first), `state`, `roleState`, `auth` (roles only behind a key), `staticBytes`, `lastModified`, `since`, `neverUp` (down, and never seen up by any check), `content` (last download's outcome and, when ok, service range, publisher, version and counts), `licenses` (members' license URLs), `catalogLinks` (members' Transitland / Mobility Database pages), `agencies` and `agencyCount` (over every scheduled URL, one per normalized name), `overlaps` (`feedId`, `kind`: `contains` / `containedIn` / `shares`, shared `agencies`), place. Top-level `moved`: an absent feed id to the present feed its members joined. What a load list reads |
| `sources.json` | The raw catalog rows, field-compatible with the old `atlas-feeds.json` |
| `status.json` | Per-row check facts (code, error, latency, failures, since, never_up); also what a snapshot hashes |
| `search.json` | `feeds.json` cut down, with short keys, to what a feed picker lists and searches: name, subtitle, alt names (rows', operators', agencies'), place, place tags (`pt`: the countries, regions and municipalities of the feed's stations, from `feed_contents`, minus the place's parts), state, `roleState`, URLs, auth, size, date, agency count (`ac`), overlaps (`ov`: `[feedId, kind]`). What every app's picker and list.gtfs.zone's first paint fetch |
| `summary.json` | Counts by catalog, kind, state and country, for rows and under `feeds` for feeds |
| `source-stats.json` | Per catalog (`catalogs`): `entries` (distinct catalog feed ids), `feeds`, `scheduleFeeds`, `onlyHere` (schedule feeds no other catalog lists), `fetch` (last fetch outcome per schedule URL, from `gtfs/index.json`), `facts` (content outcome per schedule URL), `bytes` and `distinctBytes` (stored bodies, per URL and per sha256), `countries`. `combinations` and `pairs` as `source_overlap` counts them. `duplication`: `feeds`, `multiEntry` (feeds built from more than one catalog entry), `byEvidence` (feeds where `url`, `sha256`, `cleanedHash`, `reference` or `realtimeSibling` evidence joins two entries; once per kind) and `entriesPerFeed`. Rendered in the feed-list mirror's README |
| `manifest.json` | sha256 and size of every other artifact; written last |
| `pages/feed/<feedId>/{head,body}.html` | Per-feed HTML fragments (`pages.py`) that list.gtfs.zone's nginx includes into `/feed/<feedId>/<slug>`; rewritten only when their hash in `pages/index.json` changes |
| `sitemap.xml` | Every indexable feed page, `lastmod` the day its fragments last changed |

`sources.json`, `feeds.json` and `manifest.json` carry a top-level `attribution`
(`artifacts.ATTRIBUTION`): Transitland Atlas is CC-BY 4.0 and must be credited
with a link; the Mobility Database catalog is CC0; the NTD weblinks are a US
government work, credited to the Federal Transit Administration. Feed contents
are licensed by their publishers. `search.json` carries the same block plus
`places`, the credit for its municipality tags (Overture Maps, from
OpenStreetMap, ODbL).

## History storage

Storage is shaped so it does not grow with feeds x days: `endpoint_state` holds
one row per **state change**, not per check, so a URL up for a year is one row.

History is keyed by **normalized URL** (`endpoint`), because the URL is what is
checked. Catalog rows (`source`, linked by `source_endpoint`) and logical feeds
(`feed`, `feed_member`) have no stored state: both derive it from their URLs. A
row is up when all its URLs answer. A feed has a per-role state (up when any
URL for that role answers, so a consumer can load it) and an overall state (up
only when every checkable URL answers); each consumer applies its own rule.

`endpoint.sha256` is the last body `feed_fetch` downloaded, and
`endpoint_version` holds one row per change of it, so a schedule republished
daily with the same bytes adds nothing.

## Schedule store

`feed_fetch` keeps the latest body of every static URL in a private Garage
bucket (`ZIPS_S3_*`, default `gtfs-zips`), not the public one:

| Key | Contents |
|---|---|
| `gtfs/<sha256>.zip` | A zip body, stored once however many URLs serve it |
| `gtfs/<sha256>.body` | A body that is not a zip (HTML, JSON, ...), when at most `FETCH_BODY_MAX_BYTES` |
| `gtfs/<sha256>.facts.json` | `feed_facts`' scan of that zip: `version` (`FACTS_VERSION`), `outcome` (`ok`, `not_zip`, `missing_files`, `parse_error`, `memory`), `detail`, `facts` (the content report's facts; empty unless ok) |
| `gtfs/<sha256>.fingerprint.parquet` | `feed_facts`' agency fingerprints of that zip, when its facts are ok (`gtfs_zone_feed_fetch.fingerprint`): a row per agency with `agency_id`, `name`, sorted `cells` and aligned `edge_from` / `edge_to` on the 150 m grid; `version` (`FINGERPRINT_VERSION`), `cell_metres` and `reappearances` in the file metadata |
| `dagster/<asset>.pickle` | The last value of each asset, read by the next run's downstream steps (`io_manager.StoreIOManager`); without `ZIPS_S3_ENDPOINT`, Dagster's filesystem IO manager under `DAGSTER_HOME` instead |
| `localities/localities.parquet` | The `localities` asset's file: Overture `division_area` localities, Hilbert-sorted, read by timetable-sites over S3 and copied by `feed_facts` for its scans |
| `localities/release.txt` | The Overture release that file was built from, written after it |
| `gtfs/index.json` | `generated`, `maxBytes`, and `urls`: normalized URL to `url` (as requested), `checked`, `outcome` (`ok`, `not_modified`, `too_large`, `http_error`, `error`), `status`, `key` (the stored object, `""` when none), `sha256`, `bytes`, `kind` (`zip`, `html`, `json`, `xml`, `empty`, `unknown`), `contentType`, `etag`, `lastModified` (header values as received), `fetched` (last download), `since` (last sha256 change) |

The request is a conditional GET with the stored body's validators; a 304 or a
failure keeps the stored body. A body over `FETCH_MAX_BYTES` is abandoned after
the headers and nothing is stored for it. The index is written after the
uploads, then every object under `gtfs/` it does not reference is deleted: only the latest
body per URL is kept, and a URL no catalog lists loses its object. A facts
document lives as long as its zip. The layout is `gtfs_zone_feed_fetch.store`,
shared with the other pipelines that read the bucket.

## Logical feeds

Catalog rows stay the raw layer; `feeds` builds on top of them. A union-find
joins rows on exactly seven kinds of evidence:

- a shared URL (http and https unified), any kind, any catalog;
- byte-identical schedules: scheduled URLs whose stored zip has the same
  sha256 (the same day's download);
- identical cleaned data: readable schedules with trips and the same `cleanedHash`
  (`gtfs_zone_feed_fetch.cleaned`: every GTFS file but feed_info, row and
  column order, BOM, CRLF, quoting and zip layout ignored, ids and column sets
  kept);
- an MDB realtime row's `feed_references` to an MDB static row;
- a Transitland realtime feed to its operator's schedule, when the operator
  (top-level or nested in a feed) has exactly one schedule and every operator
  of the realtime feed names the same one;
- realtime URLs on the same host and path differing only in a final
  `vehicles`/`vehiclepositions`/`trips`/`tripupdates`/`alerts`/`servicealerts`
  segment (case, `_`/`-` and extension ignored; the query must match);
- the static and rt rows one catalog feed was split into.

No fuzzy name matching and no override file yet, so the rules **can
false-merge** (a regional schedule that several agencies' realtime feeds all
reference makes them one feed, as does a regional schedule that is several
Transitland operators' only one; PTV's nested-zip feeds sharing one download
are one feed) and a wrong link can only be undone by changing the rules. Names:
a Transitland operator, then a Mobility Database provider (its feed_name
becomes the feed's `subtitle`), then the schedule's own single agency or
publisher from `feed_contents`, then an NTD agency, then any row's name.
Aggregates are not renamed: their `agencies` and `overlaps` describe them.
Coordinates: any placed member.

Feeds that are not joined can overlap. A feed's agencies and station bbox are
the union over all its scheduled URLs' facts. Two feeds sharing a normalized
agency name with intersecting bboxes overlap: the one whose agencies are a
strict subset of the other's is `containedIn` it and the other `contains` it;
otherwise both `share`. Overlaps are published, never joined.

Feed ids (`f-<10 hex>`) go in shareable URLs, so they are persisted and sticky:
each group takes the id of the existing feed it shares the most members with
(greedy, largest overlap first; on a split the larger part keeps the id), and
a new id is minted only for a group sharing none. A feed whose group vanishes
is kept absent, with its members, for the absent-retention window, so it gets
its id back if the group returns.

Coordinates come from the Mobility Database only. DMFR carries no place data, so
Transitland-only rows are unplaced unless a cross-link supplies coordinates. NTD
rows carry a text place (US, state, city) but no coordinates; a feed with no
placed member falls back to that. The unplaced count is published and shown,
not hidden.

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
| `CHECK_USER_AGENT` | Sent on every check and fetch; names the project and a contact URL |
| `FETCH_CONCURRENCY` | Global cap on in-flight schedule downloads (default 8); the per-host delay is `CHECK_PER_HOST_DELAY_SECONDS` |
| `FETCH_MAX_BYTES` | Largest body `feed_fetch` stores (default 2,000,000,000) |
| `FETCH_BODY_MAX_BYTES` | Largest non-zip body it stores (default 1,000,000) |
| `FETCH_WORKDIR` | Where downloads are written before upload, and zips and the locality file copied for `feed_facts`; blank is the system temp dir |
| `FACTS_WORKERS` | Zips `feed_facts` scans at once, each in a process of its own (default 2) |
| `LOCALITIES_MEMORY` | duckdb `memory_limit` for the `localities` build (default `4GB`) |
| `FACTS_WORKER_MEMORY` | Address space cap per facts worker in bytes (default 3,000,000,000); a zip over it is stored as `memory` |
| `STATE_RETENTION_DAYS` | Age at which `endpoint_state`, `endpoint_content_state` and `endpoint_version` rows are deleted (default 400) |
| `SOURCE_ABSENT_RETENTION_DAYS` | Days absent before a `source`, `endpoint` or `feed` row is deleted (default 90) |
| `SNAPSHOT_DAILY_DAYS` | Days of daily snapshots kept in full (default 30) |
| `SNAPSHOT_WEEKLY_DAYS` | Days after which snapshots thin to one per month (default 365) |
| `CONTENT_REPORT_BASE` | Where timetable-sites's `_content/index.json` is served (default `https://sites.gtfs.zone/`), the fallback for URLs `feed_facts` has nothing on; blank skips it |
| `RT_API_CATALOG_URL` | rt-api's public feed catalog (default `https://rt.gtfs.zone/feeds`); blank skips it |
| `NTD_WEBLINKS_URL` | NTD GTFS weblinks SODA3 view, fetched with a bare GET (default `https://data.transportation.gov/api/v3/views/2u7n-ub22/query.json`); blank skips it |
| `NTD_APP_TOKEN` | Optional Socrata app token, sent as `X-App-Token`; never logged |
| `GATUS_URL` | Gatus base URL; after a successful publish the run pushes a heartbeat there. Blank skips it |
| `GATUS_ENDPOINT_KEY` | Gatus external endpoint key (default `data_catalog-publish`) |
| `GATUS_TOKEN` | Bearer token for that external endpoint |
| `S3_ENDPOINT` `S3_BUCKET` `S3_ACCESS_KEY` `S3_SECRET_KEY` `S3_REGION` | Public artifact bucket; read by `gtfs_zone_db_models.object_store`, not by this repo's `Settings` |
| `ZIPS_S3_ENDPOINT` `ZIPS_S3_BUCKET` `ZIPS_S3_ACCESS_KEY` `ZIPS_S3_SECRET_KEY` `ZIPS_S3_REGION` | Private schedule bucket (default `gtfs-zips`); blank endpoint skips `feed_fetch` and `feed_facts` |
