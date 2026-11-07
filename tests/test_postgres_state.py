"""Live Postgres contracts, including process-boundary lock/recovery tests.

These tests require HARNESS_TEST_DATABASE_URL. Every module uses a random,
isolated schema and drops it on completion; application tables are untouched.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from datetime import timedelta

import pytest
from test_operational_state import exercise_store_contract

from automation_harness import AlreadyRunningError, Harness, HarnessConfig, Store
from automation_harness.store import utcnow

psycopg = pytest.importorskip("psycopg")
pytestmark = pytest.mark.skipif(
    not os.environ.get("HARNESS_TEST_DATABASE_URL"),
    reason="HARNESS_TEST_DATABASE_URL is not configured",
)


@pytest.fixture()
def postgres_schema():
    from automation_harness.postgres import isolated_test_url

    url, schema = isolated_test_url(os.environ["HARNESS_TEST_DATABASE_URL"], "contract")
    try:
        yield url, schema
    finally:
        with psycopg.connect(os.environ["HARNESS_TEST_DATABASE_URL"], autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_postgres_store_contract(postgres_schema):
    from automation_harness.postgres import PostgresStore

    url, _ = postgres_schema
    with PostgresStore(url) as store:
        exercise_store_contract(store)


def test_postgres_restart_recovery_and_persistent_schedule(postgres_schema, tmp_path):
    url, _ = postgres_schema
    config = HarnessConfig(data_dir=tmp_path, database_url=url, node_id="node-a")
    first = Harness(config, name="restart-test")
    first.add_job("job", lambda ctx: None, every_s=60)
    first.start()
    schedule_before = first.store.schedule_snapshot()[0]["next_run_at"]
    first.store.set_state("cursor", 42)
    interrupted_id = first.store.start_run("orphaned", pid=999, node="dead-node")
    # Simulate a dead process without cleanly finishing its synthetic run.
    first._release_lock()
    first.store.close()

    second = Harness(config, name="restart-test")
    second.add_job("job", lambda ctx: None, every_s=60)
    second.start()
    try:
        assert second.store.schedule_snapshot()[0]["next_run_at"] == schedule_before
        assert second.store.get_state("cursor") == 42
        runs = {row["id"]: row for row in second.store.last_runs()}
        assert runs[interrupted_id]["status"] == "interrupted"

        past = utcnow() - timedelta(hours=2)
        second.store.advance_schedule("job", 60, past)
        assert second.store.due_jobs(utcnow()) == ["job"]
        second.store.advance_schedule("job", 60, utcnow())
        assert second.store.due_jobs(utcnow()) == []
    finally:
        second.shutdown()


def test_postgres_lock_competes_in_parent_process(postgres_schema, tmp_path):
    url, _ = postgres_schema
    config = HarnessConfig(data_dir=tmp_path, database_url=url)
    first = Harness(config, name=f"lock-{uuid.uuid4()}")
    first.start()
    try:
        with pytest.raises(AlreadyRunningError):
            Harness(config, name=first.name).start()
    finally:
        first.shutdown()
    second = Harness(config, name=first.name)
    second.start()
    second.shutdown()


def test_postgres_lock_across_two_processes_and_crash_release(postgres_schema, tmp_path):
    url, _ = postgres_schema
    name = f"process-lock-{uuid.uuid4()}"
    ready = tmp_path / "ready"
    env = os.environ.copy()
    command = [sys.executable, "-m", "automation_harness.lock_competitor", url, name, str(ready)]
    first = subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        deadline = time.monotonic() + 15
        while not ready.exists() and first.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert ready.exists(), first.communicate(timeout=1)[1]
        time.sleep(2.5)  # heartbeat keeps a live holder exclusive beyond its initial TTL

        competitor = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
        assert competitor.returncode == 3
        assert "AlreadyRunningError" in competitor.stderr
    finally:
        first.terminate()
        first.communicate(timeout=15)

    # Crash recovery occurs after the short lease expires, without stale-file cleanup.
    deadline = time.monotonic() + 15
    while True:
        recovered = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
        if recovered.returncode == 0:
            break
        assert recovered.returncode == 3
        assert time.monotonic() < deadline, "expired lease was not recoverable"
        time.sleep(0.25)


def test_postgres_lease_loss_requests_shutdown(postgres_schema, tmp_path):
    url, _ = postgres_schema
    config = HarnessConfig(data_dir=tmp_path, database_url=url, lock_lease_seconds=1)
    harness = Harness(config, name=f"lost-lease-{uuid.uuid4()}")
    harness.start()
    try:
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("DELETE FROM instance_leases WHERE harness_name=%s", (harness.name,))
        deadline = time.monotonic() + 5
        while not harness._stop.is_set() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert harness._stop.is_set()
    finally:
        harness.shutdown()


def test_alembic_upgrade_is_idempotent_and_downgrade_upgrades(postgres_schema):
    from automation_harness.migrations import downgrade, upgrade
    from automation_harness.postgres import PostgresStore

    url, _ = postgres_schema
    upgrade(url)
    upgrade(url)
    with PostgresStore(url) as store:
        run_id = store.start_run("before", pid=1, node="node")
    downgrade(url, "base")
    upgrade(url)
    with PostgresStore(url) as store:
        assert store.last_runs() == []
        assert store.start_run("after", pid=1, node="node") != run_id


def test_sqlite_copy_merges_both_sources_idempotently(postgres_schema, tmp_path):
    from automation_harness.migrate import migrate_sqlite
    from automation_harness.postgres import PostgresStore

    url, _ = postgres_schema
    droplet = tmp_path / "droplet.db"
    laptop = tmp_path / "laptop.db"
    with Store(droplet) as store:
        store.set_state("cursor", "new")
        store.upsert_schedule("job", 60, utcnow())
        store.start_run("shared-history", pid=1, node="droplet")
        store.start_run("droplet-only", pid=1, node="droplet")
    with Store(laptop) as store:
        store.set_state("cursor", "old")
        store.start_run("laptop-only", pid=2, node="laptop")

    first = migrate_sqlite(droplet, url, source="droplet")
    second = migrate_sqlite(laptop, url, source="laptop", overwrite=False)
    repeat = migrate_sqlite(laptop, url, source="laptop", overwrite=False)
    assert first["runs_inserted"] == 2
    assert second["runs_inserted"] == 1
    assert repeat["runs_inserted"] == 0
    assert all(report["source_checksum"] for report in (first, second, repeat))

    with PostgresStore(url) as store:
        assert store.get_state("cursor") == "new"
        assert len(store.schedule_snapshot()) == 1
        names = {row["job_name"] for row in store.last_runs(20)}
        assert names == {"shared-history", "droplet-only", "laptop-only"}
        assert store.last_run_per_job()["laptop-only"]["node"] == "laptop"
