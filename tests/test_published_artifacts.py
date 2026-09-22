"""Publishing against moto: what lands in the bucket, and what does not."""

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest
from railroad_club.object_store import ObjectNotFound, ObjectStoreSettings

from geometry_car import artifacts
from geometry_car.assets.bucket_cors import ALLOWED_ORIGINS, set_cors
from geometry_car.assets.check_history import SourceStatus
from geometry_car.assets.curated_examples import load_examples
from geometry_car.assets.published_artifacts import publish
from geometry_car.catalog import Source
from tests.conftest import BUCKET, ENDPOINT

DAY_ONE = datetime(2026, 9, 20, 6, 0, tzinfo=UTC)

ROWS = [
    Source(
        source_id="tl:f-a:static",
        catalog="transitland",
        kind="static",
        feed_id="f-a",
        name="A",
        urls={"scheduled": "https://example.org/a.zip"},
    )
]
UP = {"tl:f-a:static": SourceStatus("tl:f-a:static", "up", status_code=200)}
DOWN = {"tl:f-a:static": SourceStatus("tl:f-a:static", "down", status_code=500)}


def test_a_first_run_writes_every_artifact(store):
    metadata = publish(store, ROWS, UP, load_examples(), "run-1", DAY_ONE)

    assert metadata["snapshot_written"] == 1
    for key in ("sources.json", "status.json", "examples.json", "summary.json"):
        assert json.loads(store.get(key))
    assert json.loads(store.get("sources.json"))["sources"][0]["rowId"] == (
        "tl:f-a:static"
    )

    manifest = json.loads(store.get("manifest.json"))
    assert manifest["run_id"] == "run-1"
    assert set(manifest["artifacts"]) == {
        "sources.json",
        "status.json",
        "examples.json",
        "summary.json",
        artifacts.SNAPSHOT_INDEX_KEY,
    }

    snapshot = json.loads(gzip.decompress(store.get("snapshots/2026-09-20.json.gz")))
    assert snapshot["sources"]["tl:f-a:static"]["state"] == "up"


def test_an_unchanged_day_writes_no_second_snapshot(store):
    publish(store, ROWS, UP, load_examples(), "run-1", DAY_ONE)
    metadata = publish(
        store, ROWS, UP, load_examples(), "run-2", DAY_ONE + timedelta(days=1)
    )

    assert metadata["snapshot_written"] == 0
    assert metadata["snapshots"] == 1
    with pytest.raises(ObjectNotFound):
        store.get("snapshots/2026-09-21.json.gz")
    # The daily artifacts are still refreshed; only the snapshot is skipped.
    assert json.loads(store.get("manifest.json"))["run_id"] == "run-2"


def test_a_changed_answer_writes_the_next_snapshot(store):
    publish(store, ROWS, UP, load_examples(), "run-1", DAY_ONE)
    metadata = publish(
        store, ROWS, DOWN, load_examples(), "run-2", DAY_ONE + timedelta(days=1)
    )

    assert metadata["snapshot_written"] == 1
    assert [
        e["date"]
        for e in json.loads(store.get(artifacts.SNAPSHOT_INDEX_KEY))["snapshots"]
    ] == ["2026-09-20", "2026-09-21"]


def test_snapshot_retention_deletes_the_objects_it_drops(store, monkeypatch):
    from geometry_car.settings import settings

    # A one-day daily window and a one-day weekly window, so the second run's
    # retention pass has to thin the first run's snapshot away.
    monkeypatch.setattr(settings, "snapshot_daily_days", 1)
    monkeypatch.setattr(settings, "snapshot_weekly_days", 1)

    publish(store, ROWS, UP, load_examples(), "run-1", DAY_ONE)
    publish(store, ROWS, DOWN, load_examples(), "run-2", DAY_ONE + timedelta(days=40))
    publish(
        store,
        ROWS,
        UP,
        load_examples(),
        "run-3",
        DAY_ONE + timedelta(days=80),
    )

    index = json.loads(store.get(artifacts.SNAPSHOT_INDEX_KEY))["snapshots"]
    kept = {entry["key"] for entry in index}
    assert (
        set(store.list_prefix(artifacts.SNAPSHOT_PREFIX))
        - {artifacts.SNAPSHOT_INDEX_KEY}
        == kept
    )


def test_cors_is_set_on_the_bucket(store):
    settings = ObjectStoreSettings(
        s3_endpoint=ENDPOINT,
        s3_bucket=BUCKET,
        s3_access_key="key",
        s3_secret_key="secret",
        s3_region="us-east-1",
    )
    set_cors(settings)
    # Idempotent: the daily run sets it every time.
    set_cors(settings)

    import boto3

    client = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1")
    rules = client.get_bucket_cors(Bucket=BUCKET)["CORSRules"]
    assert rules[0]["AllowedOrigins"] == ALLOWED_ORIGINS
    assert rules[0]["AllowedMethods"] == ["GET", "HEAD"]
