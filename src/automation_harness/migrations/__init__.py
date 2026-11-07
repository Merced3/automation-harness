"""Packaged Alembic migrations for the Postgres operational store."""

from __future__ import annotations

import argparse
from pathlib import Path

from alembic import command
from alembic.config import Config


def _config(database_url: str) -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).resolve().parent))
    # Keep the URL out of ConfigParser: encoded credentials/options contain '%'.
    config.attributes["database_url"] = database_url
    return config


def upgrade(database_url: str, revision: str = "head") -> None:
    command.upgrade(_config(database_url), revision)


def downgrade(database_url: str, revision: str) -> None:
    command.downgrade(_config(database_url), revision)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database_url")
    parser.add_argument("action", choices=("upgrade", "downgrade"))
    parser.add_argument("revision", nargs="?", default="head")
    args = parser.parse_args()
    if args.action == "upgrade":
        upgrade(args.database_url, args.revision)
    else:
        downgrade(args.database_url, args.revision)


if __name__ == "__main__":
    main()
