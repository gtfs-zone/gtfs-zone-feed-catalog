"""Let a browser on another origin read the artifacts.

The apps are served from gtfs.zone, edit.gtfs.zone, viz.rt.gtfs.zone and
list.gtfs.zone; the artifacts come from data.gtfs.zone. Without a CORS rule on
the bucket every one of those fetches is blocked, and the whole point of
publishing the catalog is that those apps fetch it.

This lives here rather than in the deploy's ``garage-init`` job because Garage's
admin API does not set CORS - it is an S3 call, and hand-rolling SigV4 in a
shell script is exactly the pain worth avoiding. geometry-car already holds
credentials for this bucket and already speaks S3 through boto3.

The client is built here rather than borrowed from ``ObjectStore``, which
deliberately exposes put/get/delete and not its boto3 handle. Same settings,
same addressing style, so it points at the same bucket by construction.
"""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dagster import asset
from railroad_club.object_store import ObjectStoreSettings

log = logging.getLogger(__name__)

ALLOWED_ORIGINS = [
    "https://gtfs.zone",
    "https://edit.gtfs.zone",
    "https://viz.rt.gtfs.zone",
    "https://manage.rt.gtfs.zone",
    "https://list.gtfs.zone",
    # Vite dev servers, so a developer reads the live artifacts rather than a
    # stale local copy. The apps pin ports from 8080 and Vite moves up when one
    # is taken, so 8080-8090 is allowed; 5173/5174 are Vite's defaults for apps
    # that pin nothing.
    *(f"http://localhost:{port}" for port in range(8080, 8091)),
    "http://localhost:5173",
    "http://localhost:5174",
]

# One rule per origin. Garage answers with the matched rule's whole origin list
# joined by ", " in Access-Control-Allow-Origin, which browsers reject when it
# holds more than one origin.
CORS_RULES = {
    "CORSRules": [
        {
            "AllowedMethods": ["GET", "HEAD"],
            "AllowedOrigins": [origin],
            "AllowedHeaders": ["*"],
            # The hash a consumer compares against lives in the ETag, so a
            # browser has to be allowed to read it.
            "ExposeHeaders": ["ETag", "Content-Length", "Content-Type"],
            "MaxAgeSeconds": 3600,
        }
        for origin in ALLOWED_ORIGINS
    ]
}


def set_cors(settings: ObjectStoreSettings) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )
    try:
        client.put_bucket_cors(Bucket=settings.s3_bucket, CORSConfiguration=CORS_RULES)
    except ClientError as exc:
        # A run whose artifacts are written but whose CORS call failed is worse
        # than a run that stops here: the browsers would fail silently.
        raise RuntimeError(
            f"setting CORS on {settings.s3_bucket} failed: {exc}"
        ) from exc
    log.info("CORS set on %s for %d origins", settings.s3_bucket, len(ALLOWED_ORIGINS))


@asset(description="Idempotent CORS rule on the public artifact bucket")
def bucket_cors() -> None:
    set_cors(ObjectStoreSettings())
