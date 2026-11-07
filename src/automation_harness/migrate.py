"""Read-only, idempotent SQLite-to-Postgres operational-state copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import uuid
from pathlib import Path
from typing import Any

from .postgres import PostgresStore

_RUN_NAMESPACE = uuid.UUID("44ca1bc2-e992-40ba-a0f2-4ef961ad08a2")


def _rows(conn: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(f"SELECT * FROM {table}").fetchall()


def _portable_run_id(source: str, value: Any) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return uuid.uuid5(_RUN_NAMESPACE, f"{source}:sqlite-run:{value}")


def migrate_sqlite(
    source_path: str | Path,
    database_url: str,
    *,
    source: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Merge one SQLite source without modifying it; safe to run repeatedly."""
    path = Path(source_path).resolve()
    if not path.is_file():
        raise ValueError(f"SQLite source database does not exist: {path}")
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    inserted = 0
    try:
        runs = _rows(conn, "runs")
        schedules = _rows(conn, "schedules")
        states = _rows(conn, "kv_state")
        with PostgresStore(database_url) as store:
            for row in runs:
                run_id = _portable_run_id(source, row["id"])
                cursor = store._conn.execute(
                    """INSERT INTO runs
                       (id, job_name, status, started_at, finished_at, error, pid, node)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT(id) DO NOTHING""",
                    (
                        run_id, row["job_name"], row["status"], row["started_at"],
                        row["finished_at"], row["error"], row["pid"], row["node"],
                    ),
                )
                inserted += cursor.rowcount
                store._conn.execute(
                    """INSERT INTO run_sources (source, source_run_id, run_id)
                       VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                    (source, str(row["id"]), run_id),
                )
            schedule_conflict = (
                """DO UPDATE SET interval_s=excluded.interval_s,
                   next_run_at=excluded.next_run_at, last_run_at=excluded.last_run_at,
                   enabled=excluded.enabled"""
                if overwrite else "DO NOTHING"
            )
            for row in schedules:
                store._conn.execute(
                    f"""INSERT INTO schedules
                       (job_name, interval_s, next_run_at, last_run_at, enabled)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT(job_name) {schedule_conflict}""",
                    (
                        row["job_name"], row["interval_s"], row["next_run_at"],
                        row["last_run_at"], bool(row["enabled"]),
                    ),
                )
            state_conflict = "DO UPDATE SET value=excluded.value" if overwrite else "DO NOTHING"
            for row in states:
                store._conn.execute(
                    f"""INSERT INTO kv_state (key, value) VALUES (%s, %s::jsonb)
                       ON CONFLICT(key) {state_conflict}""",
                    (row["key"], row["value"]),
                )
            destination_counts = {
                table: store._conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("runs", "schedules", "kv_state", "run_sources")
            }
            destination_checksums = {}
            for table, order in (
                ("runs", "id"), ("schedules", "job_name"),
                ("kv_state", "key"), ("run_sources", "source, source_run_id"),
            ):
                cursor = store._conn.execute(f"SELECT * FROM {table} ORDER BY {order}")
                keys = [column.name for column in cursor.description]
                canonical = [dict(zip(keys, row, strict=True)) for row in cursor.fetchall()]
                payload = json.dumps(canonical, sort_keys=True, default=str, separators=(",", ":"))
                destination_checksums[table] = hashlib.sha256(payload.encode()).hexdigest()
    finally:
        conn.close()
    return {
        "source": source,
        "source_path": str(path),
        "source_checksum": checksum,
        "source_counts": {
            "runs": len(runs), "schedules": len(schedules), "kv_state": len(states)
        },
        "runs_inserted": inserted,
        "destination_counts": destination_counts,
        "destination_checksums": destination_checksums,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="existing SQLite harness.db (opened read-only)")
    parser.add_argument("database_url", help="destination Postgres URL")
    parser.add_argument("--source-name", required=True, help="stable provenance, e.g. droplet")
    parser.add_argument("--overwrite", action="store_true", help="overwrite schedule/KV conflicts")
    args = parser.parse_args(argv)
    try:
        report = migrate_sqlite(
            args.source, args.database_url, source=args.source_name, overwrite=args.overwrite
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(2, f"SQLite migration failed: {exc}\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
