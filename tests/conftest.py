"""Fixtures for a suite that needs no running service.

SQLite with StaticPool for the history tables, moto for the bucket, respx for
every HTTP call. Nothing here talks to Postgres, Garage, GitHub or either
catalog.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws
from railroad_club.object_store import ObjectStore, ObjectStoreSettings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from geometry_car.assets import check_history as check_history_module
from geometry_car.assets import history_retention as retention_module
from geometry_car.history.models import Base
from geometry_car.settings import settings

BUCKET = "test-artifacts"
# Deliberately not an AWS hostname: the store points at Garage, and moto only
# intercepts a custom endpoint when told which one.
ENDPOINT = "http://s3.local"


@pytest.fixture
def session_factory(monkeypatch):
    """A sessionmaker over an in-memory database, patched in where it is used.

    StaticPool because every other `sqlite://` connection gets its own empty
    database, so tables created on one are invisible to the next. Patched onto
    the consuming modules rather than onto `database`, because they imported the
    name at import time.
    """
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    for module in (check_history_module, retention_module):
        monkeypatch.setattr(module, "get_session_factory", lambda: factory)
    monkeypatch.setattr(settings, "database_url", "sqlite://")
    return factory


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("MOTO_S3_CUSTOM_ENDPOINTS", ENDPOINT)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield ObjectStore(
            ObjectStoreSettings(
                s3_endpoint=ENDPOINT,
                s3_bucket=BUCKET,
                s3_access_key="key",
                s3_secret_key="secret",
                s3_region="us-east-1",
            )
        )


@pytest.fixture
def impatient(monkeypatch):
    """The checker without its politeness delay. Never set this in production."""
    monkeypatch.setattr(settings, "check_per_host_delay_seconds", 0.0)
    monkeypatch.setattr(settings, "check_timeout_seconds", 5.0)
