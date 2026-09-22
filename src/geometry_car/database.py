"""The history database, as a lazily built sync engine.

Sync rather than async because the only callers are Dagster ops, which are
plain functions on a worker thread; an async engine here would buy nothing and
cost a driver. Built on first use, not at import, so a process that never
touches history (the webserver, a test) does not need a database at all.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from geometry_car.settings import settings

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.orm import Session


def sync_url(url: str) -> str:
    """Whatever DATABASE_URL says, as a psycopg2 URL.

    The cluster hands out `postgresql://` and CNPG sometimes `postgres://`;
    SQLAlchemy 2 refuses the latter outright.
    """
    for prefix in ("postgresql+psycopg2://", "postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix) :]
    return url


@lru_cache
def get_engine() -> Engine:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set; history storage is unconfigured")
    # pool_pre_ping: the daemon holds connections between daily runs, and
    # Postgres or the network will have dropped them by the next one.
    return create_engine(sync_url(settings.database_url), pool_pre_ping=True)


@lru_cache
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), expire_on_commit=False)
