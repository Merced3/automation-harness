"""SQLite operational store: migrations, schedules, runs, and key/value state."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2
_LEGACY_RUN_NAMESPACE = uuid.UUID("db53f6db-b52f-4a13-a996-fcdb4680e3c3")

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
        status      TEXT NOT NULL,
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
    """SQLite durable operational state. One instance per process; thread-safe."""

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
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"SQLite schema version {version} is newer than supported {SCHEMA_VERSION}"
                )
            for v in range(version + 1, SCHEMA_VERSION + 1):
                if v == 1:
                    self._conn.executescript(_MIGRATIONS[v])
                    self._conn.commit()
                elif v == 2:
                    self._migrate_uuid_runs()
                self._conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
                    (str(v),),
                )
                self._conn.commit()

    def _migrate_uuid_runs(self) -> None:
        """Give run records portable UUIDs while retaining their ordering.

        Avoid executescript here: its implicit commits would make an interrupted
        table replacement unrecoverable on the next startup. The caller writes
        schema_version before committing this same transaction.
        """
        try:
            self._conn.execute("BEGIN")
            self._conn.execute(
                """CREATE TABLE runs_new (
                    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
                    id          TEXT NOT NULL UNIQUE,
                    job_name    TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    started_at  TEXT NOT NULL,
                    finished_at TEXT,
                    error       TEXT,
                    pid         INTEGER NOT NULL,
                    node        TEXT NOT NULL
                )"""
            )
            rows = self._conn.execute("SELECT * FROM runs ORDER BY id").fetchall()
            for row in rows:
                legacy_id = str(uuid.uuid5(_LEGACY_RUN_NAMESPACE, f"sqlite-run:{row['id']}"))
                self._conn.execute(
                    """INSERT INTO runs_new
                       (seq, id, job_name, status, started_at, finished_at, error, pid, node)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["id"], legacy_id, row["job_name"], row["status"], row["started_at"],
                        row["finished_at"], row["error"], row["pid"], row["node"],
                    ),
                )
            self._conn.execute("DROP TABLE runs")
            self._conn.execute("ALTER TABLE runs_new RENAME TO runs")
            self._conn.execute("CREATE INDEX idx_runs_job ON runs (job_name, seq)")
        except Exception:
            self._conn.rollback()
            raise

    # -- schedules -------------------------------------------------------

    def upsert_schedule(
        self, job_name: str, interval_s: float, next_run_at: datetime, *, reset: bool = False
    ) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT interval_s, next_run_at FROM schedules WHERE job_name=?", (job_name,)
            ).fetchone()
            if row is not None and not reset and abs(row["interval_s"] - interval_s) < 1e-9:
                return
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
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_name FROM schedules WHERE enabled=1 AND next_run_at<=?"
                " ORDER BY next_run_at",
                (_iso(now),),
            ).fetchall()
            return [r["job_name"] for r in rows]

    def advance_schedule(self, job_name: str, interval_s: float, after: datetime) -> None:
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

    def start_run(self, job_name: str, pid: int, node: str) -> str:
        run_id = str(uuid.uuid4())
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs (id, job_name, status, started_at, pid, node)"
                " VALUES (?, ?, 'running', ?, ?, ?)",
                (run_id, job_name, _iso(utcnow()), pid, node),
            )
            self._conn.commit()
        return run_id

    def finish_run(self, run_id: str, status: str, error: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET status=?, finished_at=?, error=? WHERE id=?",
                (status, _iso(utcnow()), error, run_id),
            )
            self._conn.commit()

    def mark_interrupted(self) -> int:
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
                "SELECT * FROM runs ORDER BY seq DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def last_run_per_job(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT r.* FROM runs r
                   JOIN (SELECT job_name, MAX(seq) AS max_seq FROM runs GROUP BY job_name) m
                     ON r.seq = m.max_seq"""
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
