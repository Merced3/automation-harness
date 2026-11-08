"""Alembic environment; URLs are supplied by the library or CLI."""

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
target_metadata = None


def run_migrations_offline() -> None:
    context.configure(
        url=config.attributes["database_url"],
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = config.attributes["database_url"]
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            # A transaction-scoped advisory lock remains on one backend even
            # through PgBouncer transaction pooling. Serialize fresh-schema DDL
            # and version-table creation across racing application replicas.
            connection.exec_driver_sql("SELECT pg_advisory_xact_lock(7263841559806511449)")
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
