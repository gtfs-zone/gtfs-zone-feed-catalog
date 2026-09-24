from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # extra="ignore": the .env also carries the S3_* keys railroad-club's
    # ObjectStoreSettings reads, which are not this model's to declare.
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Check history and Dagster run storage, in one database. Dagster owns and
    # creates its own tables; `source`, `endpoint`, `feed` and the rest of
    # `history.models` are this repo's, under this repo's Alembic, and collide
    # with none of Dagster's.
    database_url: str = ""

    # A local checkout of the DMFR corpus is preferred in development; empty
    # falls back to the GitHub tree API, which is what runs in the cluster.
    transitland_atlas_path: str = ""
    transitland_atlas_repo: str = "transitland/transitland-atlas"
    transitland_atlas_ref: str = "main"

    # Long-lived refresh token, exchanged for a short-lived access token at the
    # start of each run. Never logged, at either end of the exchange.
    mobility_db_refresh_token: str = ""
    mobility_db_base_url: str = "https://api.mobilitydatabase.org/v1"

    # Endpoint checking. Ten thousand outbound requests from one home IP is a
    # monitor only if it stays polite: a global cap, one request at a time per
    # host, and a pause between them.
    check_concurrency: int = 16
    check_per_host_delay_seconds: float = 1.0
    check_timeout_seconds: float = 30.0
    # Named so an origin operator who sees us in their logs can find out who we
    # are and how to complain.
    check_user_agent: str = (
        "geometry-car/0.1 (+https://list.gtfs.zone; GTFS catalog reachability check)"
    )

    # Retention. Both are load-bearing: one 40Gi Garage volume holds every
    # snapshot, and endpoint_state grows with every up/down flap. The absent
    # window covers sources, endpoints and feeds alike.
    state_retention_days: int = 400
    source_absent_retention_days: int = 90
    snapshot_daily_days: int = 30
    snapshot_weekly_days: int = 365

    # Gatus external endpoint pushed after a successful publish. Gatus pages when
    # no push arrives within its heartbeat interval, which covers a failed run, a
    # dead daemon and a schedule that never fired alike. Empty URL disables it.
    gatus_url: str = ""
    gatus_endpoint_key: str = "data_catalog-publish"
    gatus_token: str = ""

    # The object store the published artifacts are written to is configured by
    # railroad_club.object_store.ObjectStoreSettings, off the same .env:
    # S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION. Not
    # mirrored here, so nothing can point the writer and the reader at
    # different buckets by accident.


settings = Settings()
