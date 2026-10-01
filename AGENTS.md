# AGENTS.md

Dagster pipeline that inventories the world of GTFS: it ingests the Transitland
Atlas and the Mobility Database daily, checks every feed endpoint, keeps history
in Postgres, and publishes JSON artifacts to a Garage bucket served at
`data.gtfs.zone`. A `v*` tag publishes the image.

## Commands

```bash
uv run dagster dev            # run the pipeline locally, UI on :3000
uv run alembic upgrade head   # migrate the history tables (needs DATABASE_URL)
```

## Architecture

One asset per module under `src/gtfs_zone_feed_catalog/assets/`; the artifact
shapes are in `artifacts.py`. Assets, artifacts, history storage, logical-feed
grouping and env vars are in [docs/architecture.md](docs/architecture.md).

- **Rows are never merged.** Catalog rows are cross-linked by normalized URL
  (`same_endpoint_as`). Logical feeds sit on top and reference rows by id.
- **History is keyed by normalized URL** and stored per state change, not per
  check. Rows and feeds derive their state from their URLs.
- **Feed ids are sticky**: they go in shareable URLs. See
  [Logical feeds](docs/architecture.md#logical-feeds).
- **Artifact shapes are a contract** with feed-list (`src/data/artifacts.ts`),
  gtfs-zone-web-common and timetable-sites. Change them here first.
- **Attribution**: `sources.json`, `feeds.json` and `manifest.json` carry
  `artifacts.ATTRIBUTION`. Transitland Atlas is CC-BY 4.0 and must be credited.
- **Polite checks**: bounded concurrency, one request at a time per host, a
  delay between them and an honest User-Agent. Keep it that way.
- **Retention is load-bearing**: one 40Gi Garage volume holds every snapshot.
- `DAGSTER_HOME` must be a directory the image owns (`/app/dagster_home`); the
  process runs as `bridge` and `/app` is root-owned.
- The Mobility Database refresh token lives in `.env` and the cluster's
  `gtfs-app-secrets` only. Never log it or the access token.
- Dagster resolves asset annotations at import time: a type an asset signature
  names is imported at runtime, never under `TYPE_CHECKING`, and a module
  annotating `context` must not use `from __future__ import annotations`.
- `assets/__init__.py` stays empty: re-exports shadow the submodule names.
- Tests run against no services: `respx` for HTTP, `moto` for the bucket,
  SQLite with `StaticPool`. A migration touching history tables is also tried
  against a `pg_dump -t` copy of the live tables (SQLite cannot
  `ALTER COLUMN ... TYPE`; the tests stamp past `7c2e5a1d9b30`).

## Conventions

- **Commits**: Conventional Commits, enforced by the `commit-msg` hook. Never add
  Co-Authored-By trailers. Setup and release are in [CONTRIBUTING.md](CONTRIBUTING.md).
- **Logging**: module loggers are named `log`, never `logger`.
- **Cross-repo work**: sibling gtfs.zone repos live under the same parent
  directory and may be read and edited when a change spans repos.
- **Plans**: write plans to `CURRENT_PLAN.md` at the repo root as a
  checklist (`- [ ]`), ticked off as work lands. It is neither tracked nor
  gitignored: never stage or commit it.
