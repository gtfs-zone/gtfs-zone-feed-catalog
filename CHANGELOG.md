## v0.15.0 (2026-11-22)

### Feat

- **localities**: duckdb memory limit from LOCALITIES_MEMORY, feed-fetch v0.4.0
- **localities**: weekly job building the Overture locality file into the schedule bucket

## v0.14.0 (2026-11-21)

### Feat

- **alerts**: post failed Dagster runs in every code location to Telegram

## v0.13.0 (2026-11-21)

### Feat

- **overlap**: source_overlap asset shows catalog overlap in the Dagster UI

## v0.12.1 (2026-11-21)

### Fix

- **ntd**: read the weblinks dataset with one bare GET

## v0.12.0 (2026-11-19)

### Feat

- **facts**: feed_facts scans every stored zip, feed_contents and feeds read it

### Fix

- **facts**: gtfs-zone-feed-fetch v0.1.1, facts of feeds with unknown language codes keep their languages

## v0.11.0 (2026-11-19)

### Feat

- **search**: place tags from the content report as pt in search.json

## v0.10.0 (2026-11-16)

### Feat

- **feed-fetch**: download every schedule once a day into the gtfs-zips bucket

## v0.9.0 (2026-11-12)

### Feat

- **published-artifacts**: send Cache-Control with every object

## v0.8.0 (2026-11-05)

### Feat

- db-models v0.3.0 over obstore, drop boto3 and the bucket_cors asset

## v0.7.1 (2026-11-01)

### Fix

- **feeds**: pair Transitland realtime feeds with their operator's only schedule

## v0.7.0 (2026-10-31)

### Feat

- **feeds**: group byte-identical schedules and subtitle feeds that share a name

### Fix

- **feeds**: group rows whose URLs differ only by a leading www.

## v0.6.0 (2026-10-28)

### Feat

- **feeds**: record the feed an absent feed's members joined and publish it as moved in feeds.json

### Fix

- **feeds**: group and cross-link rows whose URLs differ only in http/https

## v0.5.1 (2026-10-27)

### Fix

- **catalog**: add https:// to URLs without a scheme
- **deps**: upgrade packages with published security advisories

## v0.5.0 (2026-10-18)

### Feat

- **status**: mark down feeds and rows no check has ever seen up

## v0.4.0 (2026-10-10)

### Feat

- **pages**: romanize feed page slugs in every script

## v0.3.0 (2026-10-02)

### Feat

- ingest the FTA NTD GTFS weblinks as a catalog

## v0.2.2 (2026-10-02)

## v0.2.1 (2026-10-01)

## v0.2.0 (2026-10-01)

### BREAKING CHANGE

- the module is now gtfs_zone_feed_catalog

### Feat

- **feeds**: partial state, catalog-first names and search.json
- **artifacts**: credit the catalogs and carry feed licenses
- **contents**: record cape-flier's content report per URL and publish it
- **pages**: publish crawlable feed pages and sitemap
- **cors**: allow localhost:8080-8090 on the artifact bucket
- **artifacts**: publish feeds.json
- **feeds**: per-URL endpoint history and logical feeds
- **heartbeat**: push a Gatus heartbeat after each successful publish
- **schedule**: run the daily schedule by default
- the catalog pipeline
- scaffold the geometry-car repo

### Fix

- **bucket-cors**: allow the localhost ports the Vite apps actually use
- **published-artifacts**: depend on bucket_cors for ordering only
- **history**: widen source.name to text; add a limit run config to endpoint_checks for trial runs
- **endpoint-checks**: take a global slot only after the host lock, and time the request alone
- **check-history**: start a new source's failure count at zero
- **endpoint-checks**: strip catalog URLs and record any per-URL error as a failed check
- **mobility-database**: split failing pages and skip records the API cannot serve
- **bucket-cors**: one CORS rule per origin so Garage sends a single allowed origin
- **mobility-database**: page gtfs_rt_feeds at its 1000-row limit
- **alembic**: keep this repo's revision out of Dagster's version table

### Refactor

- rename the package to gtfs-zone-feed-catalog
