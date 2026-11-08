"""Postgres operational store and transaction-pool-safe instance leases."""

from __future__ import annotations

import re
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
from psycopg.types.json import Jsonb

from . import migrations
from .store import utcnow


def isolated_test_url(database_url: str, label: str) -> tuple[str, str]:
    """Return a URL selecting a random test-only schema and its name."""
    schema = f"harness_test_{re.sub('[^a-z0-9_]', '', label)}_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query))
    options = f"{query.get('options', '')} -csearch_path={schema}".strip()
    query["options"] = options
    return urlunsplit(parts._replace(query=urlencode(query))), schema


def _json_row(row: dict[str, Any]) -> dict[str, Any]:
    for key, value in row.items():
        if isinstance(value, uuid.UUID):
            row[key] = str(value)
        elif isinstance(value, datetime):
            row[key] = value.astimezone(UTC).isoformat()
    return row


class PostgresStore:
    """Thread-safe Postgres implementation of the operational Store contract."""

    def __init__(self, database_url: str):
        migrations.upgrade(database_url)
        self._database_url = database_url
        self._lock = threading.Lock()
        self._conn = psycopg.connect(database_url, autocommit=True)
        self._lease_name: str | None = None
        self._lease_owner: uuid.UUID | None = None
        self._lease_seconds: float = 30.0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> PostgresStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- process coordination --------------------------------------------

    def acquire_instance_lock(self, name: str, *, lease_seconds: float = 30.0) -> bool:
        """Acquire a database-clock lease; safe through transaction pooling."""
        if self._lease_name == name and self._lease_owner is not None:
            return self.renew_instance_lock()
        owner = uuid.uuid4()
        with self._lock:
            row = self._conn.execute(
                """INSERT INTO instance_leases
                   (harness_name, owner, acquired_at, expires_at)
                   VALUES (%s, %s, NOW(), NOW() + (%s * INTERVAL '1 second'))
                   ON CONFLICT(harness_name) DO UPDATE SET
                       owner=excluded.owner, acquired_at=NOW(), expires_at=excluded.expires_at
                   WHERE instance_leases.expires_at <= NOW()
                   RETURNING owner""",
                (name, owner, lease_seconds),
            ).fetchone()
        if row is None:
            return False
        self._lease_name = name
        self._lease_owner = owner
        self._lease_seconds = lease_seconds
        return True

    def renew_instance_lock(self) -> bool:
        """Extend only a live lease still owned by this store."""
        if self._lease_name is None or self._lease_owner is None:
            return False
        with self._lock:
            cursor = self._conn.execute(
                """UPDATE instance_leases
                   SET expires_at=NOW() + (%s * INTERVAL '1 second')
                   WHERE harness_name=%s AND owner=%s AND expires_at > NOW()""",
                (self._lease_seconds, self._lease_name, self._lease_owner),
            )
        return cursor.rowcount == 1

    def release_instance_lock(self) -> None:
        name, self._lease_name = self._lease_name, None
        owner, self._lease_owner = self._lease_owner, None
        if name is None or owner is None or self._conn.closed:
            return
        with self._lock:
            self._conn.execute(
                "DELETE FROM instance_leases WHERE harness_name=%s AND owner=%s",
                (name, owner),
            )

    # -- schedules -------------------------------------------------------

    def upsert_schedule(
        self, job_name: str, interval_s: float, next_run_at: datetime, *, reset: bool = False
    ) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT interval_s FROM schedules WHERE job_name=%s", (job_name,)
            ).fetchone()
            if row is not None and not reset and abs(row[0] - interval_s) < 1e-9:
                return
            self._conn.execute(
                """INSERT INTO schedules (job_name, interval_s, next_run_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT(job_name) DO UPDATE SET
                       interval_s=excluded.interval_s,
                       next_run_at=excluded.next_run_at,
                       enabled=true""",
                (job_name, interval_s, next_run_at),
            )

    def due_jobs(self, now: datetime) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_name FROM schedules WHERE enabled AND next_run_at<=%s"
                " ORDER BY next_run_at",
                (now,),
            ).fetchall()
            return [row[0] for row in rows]

    def advance_schedule(self, job_name: str, interval_s: float, after: datetime) -> None:
        next_at = after + timedelta(seconds=interval_s)
        with self._lock:
            self._conn.execute(
                "UPDATE schedules SET next_run_at=%s, last_run_at=%s WHERE job_name=%s",
                (next_at, after, job_name),
            )

    def schedule_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT job_name, interval_s, next_run_at, last_run_at, enabled
                   FROM schedules ORDER BY job_name"""
            ).fetchall()
            keys = ("job_name", "interval_s", "next_run_at", "last_run_at", "enabled")
            return [_json_row(dict(zip(keys, row, strict=True))) for row in rows]

    # -- runs ------------------------------------------------------------

    def start_run(self, job_name: str, pid: int, node: str) -> str:
        run_id = uuid.uuid4()
        with self._lock:
            self._conn.execute(
                """INSERT INTO runs (id, job_name, status, started_at, pid, node)
                   VALUES (%s, %s, 'running', %s, %s, %s)""",
                (run_id, job_name, utcnow(), pid, node),
            )
        return str(run_id)

    def finish_run(self, run_id: str, status: str, error: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET status=%s, finished_at=%s, error=%s WHERE id=%s",
                (status, utcnow(), error, run_id),
            )

    def mark_interrupted(self) -> int:
        with self._lock:
            cursor = self._conn.execute(
                """UPDATE runs SET status='interrupted', finished_at=%s,
                   error=COALESCE(error, 'process terminated before run completed')
                   WHERE status='running'""",
                (utcnow(),),
            )
            return cursor.rowcount

    def last_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute("SELECT * FROM runs ORDER BY seq DESC LIMIT %s", (limit,))
            keys = [column.name for column in cursor.description]
            return [_json_row(dict(zip(keys, row, strict=True))) for row in cursor.fetchall()]

    def last_run_per_job(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                """SELECT r.* FROM runs r
                   JOIN (SELECT job_name, MAX(seq) AS max_seq FROM runs GROUP BY job_name) m
                     ON r.seq=m.max_seq"""
            )
            keys = [column.name for column in cursor.description]
            rows = [_json_row(dict(zip(keys, row, strict=True))) for row in cursor.fetchall()]
            return {row["job_name"]: row for row in rows}

    # -- key/value state --------------------------------------------------

    def get_state(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT value FROM kv_state WHERE key=%s", (key,)).fetchone()
            return row[0] if row else default

    def set_state(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO kv_state (key, value) VALUES (%s, %s)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (key, Jsonb(value)),
            )

    def drop_test_schema(self, schema: str) -> None:
        """Drop only schemas generated by isolated_test_url (test cleanup)."""
        if not re.fullmatch(r"harness_test_[a-z0-9_]+", schema):
            raise ValueError("refusing to drop a non-test schema")
        self._conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
