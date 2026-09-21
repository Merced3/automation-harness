"""SQLite operational store: migrations, schedules, runs, and key/value state."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_MIGRATIONS: dict[int, str] = {
    1: """
    CREATE TABLE meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE schedules (
        job_name     TEXT PRIMARY KEY,
        interval_s   REAL NOT NULL,
        next_run_at  TEXT NOT NULL,
        last_run_at  TEXT,
        enabled      INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE runs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name    TEXT NOT NULL,
        status      TEXT NOT NULL,            -- running | succeeded | failed | interrupted
        started_at  TEXT NOT NULL,
        finished_at TEXT,
        error       TEXT,
        pid         INTEGER NOT NULL,
        node        TEXT NOT NULL
    );
    CREATE INDEX idx_runs_job ON runs (job_name, id);
    CREATE TABLE kv_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """,
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _parse(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


class Store:
    """Durable operational state. One instance per process; thread-safe."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- migrations ------------------------------------------------------

    def _migrate(self) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='meta'"
            ).fetchone()
            version = 0
            if row:
                r = self._conn.execute(
                    "SELECT value FROM meta WHERE key='schema_version'"
                ).fetchone()
                version = int(r["value"]) if r else 0
            for v in range(version + 1, SCHEMA_VERSION + 1):
                self._conn.executescript(_MIGRATIONS[v])
                self._conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
                    (str(v),),
                )
            self._conn.commit()

    # -- schedules -------------------------------------------------------

    def upsert_schedule(
        self, job_name: str, interval_s: float, next_run_at: datetime, *, reset: bool = False
    ) -> None:
        """Create or update a schedule. Existing schedules keep their persisted
        next_run_at across restarts unless reset=True (schedule changed)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT interval_s, next_run_at FROM schedules WHERE job_name=?", (job_name,)
            ).fetchone()
            if row is not None and not reset and abs(row["interval_s"] - interval_s) < 1e-9:
                return  # keep persisted timing across restarts
            self._conn.execute(
                """INSERT INTO schedules (job_name, interval_s, next_run_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(job_name) DO UPDATE SET
                       interval_s=excluded.interval_s,
                       next_run_at=excluded.next_run_at,
                       enabled=1""",
                (job_name, interval_s, _iso(next_run_at)),
            )
            self._conn.commit()

    def due_jobs(self, now: datetime) -> list[str]:
        """Jobs whose next_run_at is at or before now (includes missed runs)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_name FROM schedules WHERE enabled=1 AND next_run_at<=?"
                " ORDER BY next_run_at",
                (_iso(now),),
            ).fetchall()
            return [r["job_name"] for r in rows]

    def advance_schedule(self, job_name: str, interval_s: float, after: datetime) -> None:
        """Move next_run_at strictly into the future relative to `after`.

        Skips missed occurrences so a long downtime causes one catch-up run,
        not a storm.
        """
        next_at = after + timedelta(seconds=interval_s)
        with self._lock:
            self._conn.execute(
                "UPDATE schedules SET next_run_at=?, last_run_at=? WHERE job_name=?",
                (_iso(next_at), _iso(after), job_name),
            )
            self._conn.commit()

    def schedule_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_name, interval_s, next_run_at, last_run_at, enabled"
                " FROM schedules ORDER BY job_name"
            ).fetchall()
            return [dict(r) for r in rows]

    # -- runs ------------------------------------------------------------

    def start_run(self, job_name: str, pid: int, node: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO runs (job_name, status, started_at, pid, node)"
                " VALUES (?, 'running', ?, ?, ?)",
                (job_name, _iso(utcnow()), pid, node),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, error: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET status=?, finished_at=?, error=? WHERE id=?",
                (status, _iso(utcnow()), error, run_id),
            )
            self._conn.commit()

    def mark_interrupted(self) -> int:
        """On startup, mark runs from dead processes as interrupted. Returns count."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE runs SET status='interrupted', finished_at=?,"
                " error=COALESCE(error, 'process terminated before run completed')"
                " WHERE status='running'",
                (_iso(utcnow()),),
            )
            self._conn.commit()
            return cur.rowcount

    def last_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def last_run_per_job(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT r.* FROM runs r
                   JOIN (SELECT job_name, MAX(id) AS max_id FROM runs GROUP BY job_name) m
                     ON r.id = m.max_id"""
            ).fetchall()
            return {r["job_name"]: dict(r) for r in rows}

    # -- key/value state (for applications) -------------------------------

    def get_state(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT value FROM kv_state WHERE key=?", (key,)).fetchone()
            return json.loads(row["value"]) if row else default

    def set_state(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO kv_state (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
            self._conn.commit()
