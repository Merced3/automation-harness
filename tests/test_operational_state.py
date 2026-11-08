"""Backend-neutral operational-state contracts and SQLite compatibility."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import timedelta

import pytest

from automation_harness import Harness, HarnessConfig, Store
from automation_harness.store import utcnow


def exercise_store_contract(store: Store) -> None:
    """The public Store contract every backend must satisfy."""
    next_run = utcnow() - timedelta(minutes=5)
    store.upsert_schedule("job", 30, next_run)
    store.upsert_schedule("job", 30, utcnow() + timedelta(hours=1))
    assert store.schedule_snapshot()[0]["next_run_at"] == next_run.isoformat()
    assert store.due_jobs(utcnow()) == ["job"]

    run_id = store.start_run("job", pid=123, node="node-a")
    assert isinstance(run_id, str)
    assert len(run_id) == 36
    assert store.last_run_per_job()["job"]["status"] == "running"
    store.finish_run(run_id, "failed", "boom")
    assert store.last_runs(1)[0]["error"] == "boom"

    store.set_state("cursor", {"offset": 42})
    assert store.get_state("cursor") == {"offset": 42}
    store.advance_schedule("job", 30, utcnow())
    assert store.due_jobs(utcnow()) == []


def test_sqlite_store_contract_and_uuid_run_ids(tmp_path):
    with Store(tmp_path / "harness.db") as store:
        exercise_store_contract(store)


def test_sqlite_v1_database_upgrades_without_losing_history(tmp_path):
    """Existing integer run IDs remain readable after the UUID migration."""
    path = tmp_path / "harness.db"
    with Store(path) as store:
        store._conn.executescript(
            """
            DROP TABLE runs;
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error TEXT,
                pid INTEGER NOT NULL,
                node TEXT NOT NULL
            );
            INSERT INTO runs VALUES
                (7, 'legacy', 'succeeded', '2026-01-01T00:00:00+00:00',
                 '2026-01-01T00:00:01+00:00', NULL, 1, 'old');
            UPDATE meta SET value='1' WHERE key='schema_version';
            """
        )
        store._conn.commit()

    with Store(path) as store:
        rows = store.last_runs()
        assert len(rows) == 1
        assert rows[0]["job_name"] == "legacy"
        assert len(rows[0]["id"]) == 36
        new_id = store.start_run("new", pid=2, node="new")
        assert store.last_run_per_job()["new"]["id"] == new_id


def test_sqlite_uuid_migration_rolls_back_partial_replacement(tmp_path):
    path = tmp_path / "harness.db"
    with Store(path) as store:
        store.start_run("legacy", pid=1, node="old")
        store._conn.executescript(
            """
            DROP TABLE runs;
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL,
                finished_at TEXT, error TEXT, pid INTEGER NOT NULL, node TEXT NOT NULL
            );
            INSERT INTO runs VALUES
                (7, 'legacy', 'succeeded', '2026-01-01T00:00:00+00:00',
                 '2026-01-01T00:00:01+00:00', NULL, 1, 'old');
            UPDATE meta SET value='1' WHERE key='schema_version';
            """
        )

        def deny_drop(action, *args):
            return 1 if action == sqlite3.SQLITE_DROP_TABLE else 0

        store._conn.set_authorizer(deny_drop)
        with pytest.raises(sqlite3.DatabaseError):
            store._migrate_uuid_runs()
        store._conn.set_authorizer(None)
        assert store._conn.execute(
            "SELECT name FROM sqlite_master WHERE name='runs_new'"
        ).fetchone() is None
        store._migrate()
        assert store.last_runs()[0]["job_name"] == "legacy"


def test_database_url_defaults_to_sqlite_and_reads_generic_env(tmp_path, monkeypatch):
    monkeypatch.delenv("HARNESS_DATABASE_URL", raising=False)
    monkeypatch.setenv("AUTOMATION_HARNESS_DATA_DIR", str(tmp_path))
    config = HarnessConfig.from_env(env_file=None)
    assert config.database_url is None
    assert config.database_path == tmp_path / "harness.db"

    monkeypatch.setenv("HARNESS_DATABASE_URL", "postgresql://example.invalid/harness")
    assert HarnessConfig.from_env(env_file=None).database_url == (
        "postgresql://example.invalid/harness"
    )


def test_postgres_status_is_disposable_and_uses_no_file_lock(tmp_path, monkeypatch):
    postgres_url = os.environ.get("HARNESS_TEST_DATABASE_URL")
    if not postgres_url:
        pytest.skip("HARNESS_TEST_DATABASE_URL is not configured")
    pytest.importorskip("psycopg")

    from automation_harness.postgres import PostgresStore, isolated_test_url

    url, schema = isolated_test_url(postgres_url, "status")
    config = HarnessConfig(data_dir=tmp_path, database_url=url, node_id="postgres-test")
    harness = Harness(config, name="status-test")
    try:
        harness.start()
        assert config.status_path.is_file()
        assert not config.lock_path.exists()
        config.status_path.unlink()
        harness._write_status()
        snapshot = json.loads(config.status_path.read_text())
        assert snapshot["harness"] == "status-test"
    finally:
        if harness._lock_acquired:
            harness.shutdown()
        with PostgresStore(url) as store:
            store.drop_test_schema(schema)


def test_sqlite_migrations_are_idempotent(tmp_path):
    path = tmp_path / "harness.db"
    with Store(path) as store:
        first = store.start_run("one", pid=1, node="node")
    with Store(path) as store:
        assert store.last_runs()[0]["id"] == first


def test_migration_cli_rejects_non_sqlite_source(tmp_path):
    result = subprocess.run(
        [
            sys.executable, "-m", "automation_harness.migrate", str(tmp_path),
            "postgresql://example.invalid/harness", "--source-name", "invalid",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "SQLite" in result.stderr


def test_lock_competitor_module_is_importable():
    import automation_harness.lock_competitor as competitor

    assert callable(competitor.main)
