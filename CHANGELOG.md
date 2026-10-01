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
