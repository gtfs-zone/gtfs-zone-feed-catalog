"""Alembic, synchronously, against this repo's tables only.

Synchronous where railroad-club's env.py is async, because this repo's engine is
psycopg2 and a migration has no reason to be either.

``include_object`` is the load-bearing part: Dagster owns its own run, event-log
and schedule tables in the same database and creates them itself. Without the
filter, an autogenerate run here would cheerfully write a migration dropping all
of them.

``VERSION_TABLE`` is the other half of sharing a database with Dagster. Dagster
migrates itself with Alembic too, and it records its revision in the default
``alembic_version``. Reading that table here finds a revision from Dagster's
history and fails with "Can\'t locate revision identified by ...", which says
nothing about the real cause.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from geometry_car.database import sync_url
from geometry_car.history.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

OUR_TABLES = set(target_metadata.tables)

VERSION_TABLE = "alembic_version_geometry_car"


def include_object(_object, name, type_, _reflected, _compare_to) -> bool:
    if type_ == "table":
        return name in OUR_TABLES
    return True


def get_url() -> str:
    return sync_url(os.environ["DATABASE_URL"])


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object,
        version_table=VERSION_TABLE,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            version_table=VERSION_TABLE,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
