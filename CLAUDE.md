# Geometry Car: Claude Guide

## Project Overview

Dagster pipeline that inventories the world of GTFS. It ingests the Transitland
Atlas DMFR corpus and the Mobility Database daily, checks whether each feed
endpoint is reachable, keeps history in Postgres, and publishes public JSON
artifacts to a Garage bucket served at `data.gtfs.zone`.

The artifacts replace the build-time `public/atlas-feeds.json` that
`coloring-book` and `test-track` each shipped their own stale copy of, and the
hand-curated `EXAMPLES` list that used to live in `interlocking`.

## Commands

```bash
uv sync              # install dependencies
ruff check .         # lint
ruff format .        # format
uv run pytest        # tests
uv run dagster dev   # run the pipeline locally, UI on :3000
uv run alembic upgrade head   # migrate the history tables (needs DATABASE_URL)
pre-commit install   # install git hooks
```

## Architecture

Assets live one per module under `src/geometry_car/assets/`, in dependency
order:

| Asset | What it does |
|---|---|
| `transitland_atlas` | Parse the DMFR corpus: a sibling checkout in dev, the GitHub tree API in the cluster |
| `mobility_database` | Exchange the refresh token at `POST /v1/tokens/access`, then page `/v1/gtfs_feeds` and `/v1/gtfs_rt_feeds` |
| `curated_examples` | The hand-curated set, as declarative source data in `data/examples.yaml` |
| `sources` | One row per source kind, ids namespaced by catalog, cross-linked by normalized URL |
| `endpoint_checks` | HEAD (ranged GET on fallback) every download URL; size (static only), `Last-Modified`, `ETag` |
| `check_history` | Fold the checks into one answer per source; record each URL's check and state changes in Postgres |
| `feeds` | Group rows into logical feeds (one per transit system) with persisted, sticky ids |
| `history_retention` | Delete old `endpoint_state` rows, and absent sources, endpoints and feeds, past their window |
| `bucket_cors` | Idempotent CORS rule on the public bucket, set over S3 because Garage's admin API cannot |
| `published_artifacts` | Write the JSON artifacts and dated snapshots to the public bucket |

Storage is shaped so it does not grow with feeds x days: `endpoint_state` holds
one row per **state change**, not per check, so a URL up for a year is one row.

History is keyed by **normalized URL** (`endpoint`), because the URL is what is
checked. Catalog rows (`source`, linked by `source_endpoint`) and logical feeds
(`feed`, `feed_member`) have no stored state: both derive it from their URLs. A
row is up when all its URLs answer. A feed has a per-role state (up when any
URL for that role answers, so a consumer can load it) and an overall state (up
only when every checkable URL answers); each consumer applies its own rule.

### Logical feeds

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
curated > Mobility Database > Transitland. Coordinates: any placed member.

Feed ids (`f-<10 hex>`) go in shareable URLs, so they are persisted and sticky:
each group takes the id of the existing feed it shares the most members with
(greedy, largest overlap first; on a split the larger part keeps the id), and
a new id is minted only for a group sharing none. A feed whose group vanishes
is kept absent, with its members, for the absent-retention window, so it gets
its id back if the group returns.

Coordinates come from the Mobility Database only. DMFR carries no place data, so
Transitland-only rows are unplaced unless a cross-link supplies coordinates. The
unplaced count is published and shown, not hidden.

## Environment Variables

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
| `STATE_RETENTION_DAYS` | Age at which `endpoint_state` rows are deleted (default 400) |
| `SOURCE_ABSENT_RETENTION_DAYS` | Days absent before a `source`, `endpoint` or `feed` row is deleted (default 90) |
| `SNAPSHOT_DAILY_DAYS` | Days of daily snapshots kept in full (default 30) |
| `SNAPSHOT_WEEKLY_DAYS` | Days after which snapshots thin to one per month (default 365) |
| `GATUS_URL` | Gatus base URL; after a successful publish the run pushes a heartbeat there. Blank skips it |
| `GATUS_ENDPOINT_KEY` | Gatus external endpoint key (default `data_catalog-publish`) |
| `GATUS_TOKEN` | Bearer token for that external endpoint |
| `S3_ENDPOINT` `S3_BUCKET` `S3_ACCESS_KEY` `S3_SECRET_KEY` `S3_REGION` | Public artifact bucket; read by `railroad_club.object_store`, not by this repo's `Settings` |

## Rules

- Never include `Co-Authored-By: Claude ...` trailers in commit messages.
- Cross-repo work is allowed: sibling gtfs.zone repos live under the same parent
  directory and may be read and edited when a change spans repos.
- Module loggers are named `log`, never `logger`: `log = logging.getLogger(__name__)`
- `DAGSTER_HOME` must point at a directory the image already owns
  (`/app/dagster_home`). `/app` is root-owned and the process runs as `bridge`,
  so the default lands somewhere unwritable and fails late.
- The refresh token goes in `.env` (gitignored) and in the cluster's
  `gtfs-app-secrets` only. Neither it nor the access token is ever logged.
- Endpoint checks are polite by construction: bounded concurrency, one request
  at a time per host, a delay between them and an honest User-Agent. Ten
  thousand requests a day from a home IP is a monitor only while that holds.
- Retention is load-bearing, not tidiness: one 40Gi Garage volume holds every
  snapshot, and snapshots are written only when the content hash changes.
- Tests run against no services. `respx` for HTTP, `moto` for the bucket,
  SQLite with `StaticPool` for the history tables.
- An asset's parameter annotations are resolved by Dagster at import time, so a
  type an asset signature names must be imported at runtime, never under
  `TYPE_CHECKING`, and the module must not use `from __future__ import
  annotations` if it annotates `context`. Both failures are import-time, loud.
- `geometry_car/assets/__init__.py` stays empty of imports on purpose:
  re-exporting the asset objects shadows the submodule names, and then anything
  addressing a module by its dotted path gets the asset instead.
- Both catalogs list many of the same feeds. Catalog rows are cross-linked by
  normalized URL (`same_endpoint_as`), never merged - merging means picking
  whose id and name win and silently losing the loser. Logical feeds sit on
  top and reference rows by id; they never replace or rewrite them.
- Migrations touching tables with history must be tried against a copy of the
  live tables (`pg_dump -t` into a local Postgres), not only SQLite: the tests
  stamp past `7c2e5a1d9b30` because SQLite cannot `ALTER COLUMN ... TYPE`.

## Related Repos

| Repo | Description | URL |
|---|---|---|
| globe-of-contents | Frontend for these artifacts at list.gtfs.zone | https://git.kcfam.us/gtfs.zone/globe-of-contents |
| railroad-club | Shared Python library, including the object-store client | https://git.kcfam.us/gtfs.zone/railroad-club |
| interlocking | Shared frontend library; consumes the published catalog | https://git.kcfam.us/gtfs.zone/interlocking |
| cafe-car | GTFS-RT HTTP API serving real-time feeds | https://git.kcfam.us/gtfs.zone/cafe-car |
| schedule-foamer | Worker that ingests and processes GTFS schedule data | https://git.kcfam.us/gtfs.zone/schedule-foamer |
| deploy-gtfs-rt | ArgoCD-managed k3s deployment | https://git.kcfam.us/gtfs.zone/deploy-gtfs-rt |
