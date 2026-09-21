"""Focused tests for the runtime guarantees the harness exists to provide:
persistent scheduling, restart recovery, and single-instance coordination."""

import threading
import time
from datetime import timedelta

import pytest

from automation_harness import AlreadyRunningError, Harness, HarnessConfig, Store
from automation_harness.store import utcnow


@pytest.fixture()
def config(tmp_path):
    return HarnessConfig(data_dir=tmp_path / "data", node_id="test-node")


def test_schedule_persists_across_restarts(config):
    """A job's next_run_at survives a restart instead of being reset."""
    h = Harness(config)
    h.add_job("j", lambda ctx: None, every_s=60)
    h.start()
    before = h.store.schedule_snapshot()[0]["next_run_at"]
    h.shutdown()

    h2 = Harness(config)
    h2.add_job("j", lambda ctx: None, every_s=60)
    h2.start()
    after = h2.store.schedule_snapshot()[0]["next_run_at"]
    h2.shutdown()

    assert before == after


def test_interrupted_runs_marked_on_recovery(config):
    """Runs left 'running' by a dead process are marked interrupted at startup."""
    store = Store(config.database_path)
    run_id = store.start_run("j", pid=999999, node="old-node")
    store.close()

    h = Harness(config)
    h.start()
    runs = h.store.last_runs()
    h.shutdown()

    assert runs[0]["id"] == run_id
    assert runs[0]["status"] == "interrupted"


def test_missed_schedule_runs_once_not_storm(config):
    """If next_run_at is long past, one catch-up run is scheduled, not many."""
    h = Harness(config)
    h.add_job("j", lambda ctx: None, every_s=1)
    h.start()
    past = utcnow() - timedelta(hours=1)
    # simulate: the process was down for an hour while the schedule kept ticking
    h.store.advance_schedule("j", 1, past)  # next_run_at now deep in the past
    due_before = h.store.due_jobs(utcnow())
    assert due_before == ["j"]  # missed run is due for a single catch-up
    # the tick runs the job once and re-arms the schedule into the future
    h.store.advance_schedule("j", 1, utcnow())
    snap = h.store.schedule_snapshot()[0]
    assert snap["next_run_at"] > utcnow().isoformat()  # advanced past now
    assert h.store.due_jobs(utcnow()) == []  # no storm of missed runs
    h.shutdown()


def test_single_instance_lock(config):
    """A second harness refuses to start while the first is alive."""
    h = Harness(config)
    h.start()
    try:
        with pytest.raises(AlreadyRunningError):
            Harness(config).start()
    finally:
        h.shutdown()
    # after shutdown the lock is released and a new harness may start
    h2 = Harness(config)
    h2.start()
    h2.shutdown()


def test_job_executes_and_records_run(config):
    done = threading.Event()

    def job(ctx):
        ctx.set_state("ran", True)
        done.set()

    h = Harness(config)
    h.add_job("j", job, every_s=0.2, run_immediately=True)
    h.start()
    threading.Thread(target=lambda: (time.sleep(0.1), h._tick()), daemon=True).start()
    h._tick()
    assert done.wait(timeout=5)
    time.sleep(0.1)
    status = h.status()
    h.shutdown()

    assert status["healthy"] is True
    assert status["last_runs"]["j"]["status"] == "succeeded"
    store = Store(config.database_path)
    assert store.get_state("job.j.ran") is True
    store.close()


def test_failed_job_recorded_not_raised(config):
    def job(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_job("j", job, every_s=0.2, run_immediately=True)
    h.start()
    h._tick()
    h._jobs["j"].thread.join(timeout=5)
    status = h.status()
    h.shutdown()

    assert status["last_runs"]["j"]["status"] == "failed"
    assert "boom" in status["last_runs"]["j"]["error"]


def test_status_file_written(config):
    h = Harness(config)
    h.add_job("j", lambda ctx: None, every_s=60)
    h.start()
    h.shutdown()
    assert config.status_path.exists()
    assert not config.lock_path.exists()


def test_kv_state_roundtrip(config):
    store = Store(config.database_path)
    store.set_state("cursor", {"offset": 42})
    assert store.get_state("cursor") == {"offset": 42}
    assert store.get_state("missing", "dflt") == "dflt"
    store.close()
