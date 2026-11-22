"""Dagster entrypoint. The code server and ``dagster dev`` both load this module.

Assets are collected by walking ``gtfs_zone_feed_catalog.assets``, so adding an asset is
adding a module. One job over all of them but ``localities``, on one daily
schedule; ``localities`` has its own weekly job. Nothing
retries a run: the UI's re-execute button is the retry mechanism, and a catalog
that is late by an hour is not worth a second trigger path. The one sensor,
``run_failure_alert``, only reports failed runs in every code location.

With ``ZIPS_S3_ENDPOINT`` set, asset values live in the schedule bucket
(``io_manager.StoreIOManager``), so any asset can be rerun after a rollout;
without it, Dagster's default filesystem IO manager keeps them under
``DAGSTER_HOME``.

The hour is late-UTC, which is the small hours across North America where most
of the checked hosts are. Forty thousand requests is easier on someone else's
origin outside their peak.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from gtfs_zone_db_models.object_store import ObjectStore

from gtfs_zone_feed_catalog import assets
from gtfs_zone_feed_catalog.failure_alert import run_failure_alert
from gtfs_zone_feed_catalog.io_manager import StoreIOManager
from gtfs_zone_feed_catalog.zips import ZipStoreSettings

ALL_ASSETS = load_assets_from_package_module(assets)

daily_catalog = define_asset_job(
    "daily_catalog",
    selection=AssetSelection.all() - AssetSelection.assets("localities"),
    description="Ingest every catalog, check every endpoint, publish the artifacts",
)

daily_schedule = ScheduleDefinition(
    name="daily_catalog_schedule",
    job=daily_catalog,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",
    # Running by default, because the alternative is a toggle that exists only
    # in Dagster's database: a fresh deploy would come up with the schedule off
    # and nothing anywhere would say so. This repo has been bitten by
    # configuration that lives only in a UI's database before.
    default_status=DefaultScheduleStatus.RUNNING,
)

# Overture releases about monthly; a run with no new release writes nothing.
# The heavy tag keeps it out of the sites runs that read the file.
weekly_localities = define_asset_job(
    "weekly_localities",
    selection=AssetSelection.assets("localities"),
    description="Rebuild the locality file from a new Overture release",
    tags={"gtfs.zone/memory": "heavy"},
)

weekly_localities_schedule = ScheduleDefinition(
    name="weekly_localities_schedule",
    job=weekly_localities,
    cron_schedule="0 6 * * 1",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

zip_settings = ZipStoreSettings()
resources = (
    {"io_manager": StoreIOManager(ObjectStore(zip_settings))}
    if zip_settings.s3_endpoint
    else {}
)

defs = Definitions(
    assets=ALL_ASSETS,
    resources=resources,
    jobs=[daily_catalog, weekly_localities],
    schedules=[daily_schedule, weekly_localities_schedule],
    sensors=[run_failure_alert],
)
