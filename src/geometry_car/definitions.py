"""Dagster entrypoint. The code server and ``dagster dev`` both load this module.

Assets are collected by walking ``geometry_car.assets``, so adding an asset is
adding a module. One job over all of them, on one daily schedule: there is no
sensor, because the UI's re-execute button is the retry mechanism and a catalog
that is late by an hour is not worth a second trigger path.

The hour is late-UTC, which is the small hours across North America where most
of the checked hosts are. Forty thousand requests is easier on someone else's
origin outside their peak.
"""

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from geometry_car import assets

ALL_ASSETS = load_assets_from_package_module(assets)

daily_catalog = define_asset_job(
    "daily_catalog",
    selection=AssetSelection.all(),
    description="Ingest both catalogs, check every endpoint, publish the artifacts",
)

daily_schedule = ScheduleDefinition(
    name="daily_catalog_schedule",
    job=daily_catalog,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",
)

defs = Definitions(assets=ALL_ASSETS, jobs=[daily_catalog], schedules=[daily_schedule])
