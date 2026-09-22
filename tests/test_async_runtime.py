"""Tests for the async runtime: supervised services and asyncio-hosted jobs."""

import asyncio
import threading
import time

import pytest

from automation_harness import Harness, HarnessConfig


@pytest.fixture()
def config(tmp_path):
    return HarnessConfig(data_dir=tmp_path / "data", node_id="test-node")


def run_harness_thread(h: Harness) -> threading.Thread:
    t = threading.Thread(target=h.run, daemon=True)
    t.start()
    return t


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_service_runs_until_stopped(config):
    """A service runs on the event loop and is asked to stop at shutdown."""
    started = asyncio.Event()
    stopped_cleanly = threading.Event()

    async def service(ctx):
        started.set()
        await ctx.stop_event.wait()
        stopped_cleanly.set()

    h = Harness(config)
    h.add_service("svc", service)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "running")
    h.stop()
    t.join(timeout=10)

    assert not t.is_alive()
    assert stopped_cleanly.is_set()
    assert h._services["svc"].state == "stopped"
    assert h._services["svc"].restart_count == 0
    assert not config.lock_path.exists()  # lock released
    assert config.status_path.exists()  # final status written


def test_service_restarts_on_failure_with_backoff(config):
    """A failing service is restarted; backoff delays and failures are recorded."""
    attempts = threading.Event()

    async def flaky(ctx):
        if ctx.restart_count >= 2:
            attempts.set()  # prove restart_count is threaded through
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.05, backoff_max_s=0.1)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].restart_count >= 2)
    assert attempts.wait(timeout=5)  # restart_count threaded into the next invocation
    h.stop()
    t.join(timeout=10)

    svc = h._services["svc"]
    assert svc.last_error is not None and "boom" in svc.last_error
    assert svc.restart_count >= 2


def test_service_failure_recorded_in_runs(config):
    async def flaky(ctx):
        raise ValueError("recorded")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.05, backoff_max_s=0.05)
    t = run_harness_thread(h)
    assert wait_for(lambda: h._services["svc"].restart_count >= 1)
    h.stop()
    t.join(timeout=10)

    from automation_harness import Store

    store = Store(config.database_path)
    runs = [r for r in store.last_runs(50) if r["job_name"] == "svc"]
    store.close()
    assert any(r["status"] == "failed" and "recorded" in (r["error"] or "") for r in runs)


def test_service_no_restart_when_disabled(config):
    async def flaky(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, restart=False, backoff_initial_s=0.01, backoff_max_s=0.01)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "stopped")
    time.sleep(0.1)
    assert h._services["svc"].restart_count == 0
    h.stop()
    t.join(timeout=10)


def test_service_clean_return_not_restarted(config):
    runs = []

    async def quick(ctx):
        runs.append(ctx.run_id)

    h = Harness(config)
    h.add_service("svc", quick, backoff_initial_s=0.01, backoff_max_s=0.01)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "stopped")
    time.sleep(0.1)
    assert len(runs) == 1
    h.stop()
    t.join(timeout=10)


def test_service_state_persists(config):
    async def service(ctx):
        ctx.set_state("seen", ctx.get_state("seen", 0) + 1)
        await ctx.stop_event.wait()

    h = Harness(config)
    h.add_service("svc", service)
    t = run_harness_thread(h)
    assert wait_for(lambda: h._services["svc"].state == "running")
    assert wait_for(lambda: h.store.get_state("service.svc.seen") == 1)
    h.stop()
    t.join(timeout=10)

    from automation_harness import Store

    store = Store(config.database_path)
    assert store.get_state("service.svc.seen") == 1
    store.close()


def test_async_job_runs_on_event_loop(config):
    done = threading.Event()

    async def job(ctx):
        ctx.set_state("ran", True)
        done.set()

    h = Harness(config)
    h.add_job("ajob", job, every_s=0.2, run_immediately=True)
    t = run_harness_thread(h)

    assert done.wait(timeout=5)
    h.stop()
    t.join(timeout=10)


def test_sync_job_runs_alongside_services(config):
    """Sync jobs still work in async mode (via a worker thread)."""
    ran = threading.Event()
    svc_started = threading.Event()

    def sync_job(ctx):
        ran.set()

    async def service(ctx):
        svc_started.set()
        await ctx.stop_event.wait()

    h = Harness(config)
    h.add_job("sjob", sync_job, every_s=0.2, run_immediately=True)
    h.add_service("svc", service)
    t = run_harness_thread(h)

    assert ran.wait(timeout=5)
    assert svc_started.wait(timeout=5)
    h.stop()
    t.join(timeout=10)


def test_status_includes_services(config):
    async def service(ctx):
        await ctx.stop_event.wait()

    h = Harness(config)
    h.add_service("svc", service)
    t = run_harness_thread(h)
    assert wait_for(lambda: h._services["svc"].state == "running")

    status = h.status()
    assert status["services"]["svc"]["state"] == "running"
    assert status["services"]["svc"]["restart_count"] == 0
    h.stop()
    t.join(timeout=10)


def test_single_instance_lock_still_enforced_in_async_mode(config):
    async def service(ctx):
        await ctx.stop_event.wait()

    h = Harness(config)
    h.add_service("svc", service)
    t = run_harness_thread(h)
    assert wait_for(lambda: config.lock_path.exists())

    from automation_harness import AlreadyRunningError

    with pytest.raises(AlreadyRunningError):
        Harness(config).start()
    h.stop()
    t.join(timeout=10)


def test_add_service_validates_input(config):
    h = Harness(config)

    def not_async(ctx):
        pass

    with pytest.raises(TypeError):
        h.add_service("sync", not_async)

    async def svc(ctx):
        pass

    with pytest.raises(ValueError, match="backoff"):
        h.add_service("bad", svc, backoff_initial_s=0)
    h.add_service("ok", svc, backoff_initial_s=0.1, backoff_max_s=0.1)
    with pytest.raises(ValueError, match="already registered"):
        h.add_service("ok", svc, backoff_initial_s=0.1, backoff_max_s=0.1)
