"""Tests for Phase 3 supervision: restart escalation and alert hooks."""

import asyncio
import threading
import time

import pytest
from conftest import run_harness_thread, wait_for

from automation_harness import Harness, HarnessConfig


@pytest.fixture()
def config(tmp_path):
    return HarnessConfig(data_dir=tmp_path / "data", node_id="test-node")


def test_escalation_stops_restarting_and_marks_failed(config):
    """After max_consecutive_failures, the service is failed, not restarted."""
    attempts = 0

    async def flaky(ctx):
        nonlocal attempts
        attempts += 1
        raise RuntimeError(f"crash {attempts}")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=3)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "failed")
    time.sleep(0.15)  # several backoff windows pass
    assert attempts == 3  # no further restarts
    h.stop()
    t.join(timeout=10)
    assert h._services["svc"].state == "failed"  # stays failed through shutdown


def test_alert_hook_fires_once_with_event(config):
    events = []

    async def flaky(ctx):
        raise ValueError("dying")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=2)
    h.on_alert(events.append)
    t = run_harness_thread(h)

    assert wait_for(lambda: len(events) >= 1)
    h.stop()
    t.join(timeout=10)

    assert len(events) == 1
    event = events[0]
    assert event["event"] == "service_escalated"
    assert event["service"] == "svc"
    assert event["consecutive_failures"] == 2
    assert event["max_consecutive_failures"] == 2
    assert "dying" in event["last_error"]
    assert event["node"] == "test-node"
    assert event["ts"]


def test_async_alert_hook_is_awaited(config):
    delivered = threading.Event()

    async def hook(event):
        delivered.set()

    async def flaky(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=1)
    h.on_alert(hook)
    t = run_harness_thread(h)

    assert delivered.wait(timeout=5)
    h.stop()
    t.join(timeout=10)


def test_alert_hook_error_does_not_crash_supervisor(config):
    def bad_hook(event):
        raise RuntimeError("delivery exploded")

    async def flaky(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=1)
    h.on_alert(bad_hook)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "failed")
    h.stop()
    t.join(timeout=10)
    assert not t.is_alive()


def test_healthy_run_resets_consecutive_failures(config):
    """A run surviving reset_after_s resets the count; the service keeps
    restarting instead of escalating on later quick crashes."""
    attempts = 0

    async def flap(ctx):
        nonlocal attempts
        attempts += 1
        if attempts % 2 == 1:
            await asyncio.sleep(0.12)  # survive past reset_after_s: a "healthy" run
        raise RuntimeError("boom")

    h = Harness(config)
    # pattern alternates healthy/quick crashes: counts go 1, 2, 1, 2, ...
    # so max=3 is never reached — but only if healthy runs reset the count
    h.add_service("svc", flap, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=3, reset_after_s=0.1)
    t = run_harness_thread(h)

    assert wait_for(lambda: attempts >= 5)
    time.sleep(0.1)
    assert h._services["svc"].state in ("running", "backoff")
    h.stop()
    t.join(timeout=10)


def test_flapping_service_still_escalates(config):
    """Runs that never survive reset_after_s escalate even with gaps between."""
    attempts = 0

    async def flap(ctx):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("instant crash")

    h = Harness(config)
    h.add_service("svc", flap, backoff_initial_s=0.01, backoff_max_s=0.01,
                  max_consecutive_failures=3, reset_after_s=60)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].state == "failed")
    assert attempts == 3
    h.stop()
    t.join(timeout=10)


def test_no_escalation_by_default(config):
    """max_consecutive_failures=None keeps Phase 2 behavior: restart forever."""

    async def flaky(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.01, backoff_max_s=0.01)
    t = run_harness_thread(h)

    assert wait_for(lambda: h._services["svc"].restart_count >= 5)
    assert h._services["svc"].state in ("running", "backoff")
    h.stop()
    t.join(timeout=10)


def test_status_shows_consecutive_failures(config):
    async def flaky(ctx):
        raise RuntimeError("boom")

    h = Harness(config)
    h.add_service("svc", flaky, backoff_initial_s=0.05, backoff_max_s=0.05,
                  max_consecutive_failures=5)
    t = run_harness_thread(h)
    assert wait_for(lambda: h._services["svc"].consecutive_failures >= 1)

    status = h.status()
    svc = status["services"]["svc"]
    assert svc["consecutive_failures"] >= 1
    assert svc["state"] in ("running", "backoff")
    h.stop()
    t.join(timeout=10)


def test_escalation_validation(config):
    async def svc(ctx):
        pass

    h = Harness(config)
    with pytest.raises(ValueError, match="max_consecutive_failures"):
        h.add_service("bad1", svc, backoff_initial_s=0.1, backoff_max_s=0.1,
                      max_consecutive_failures=0)
    with pytest.raises(ValueError, match="reset_after_s"):
        h.add_service("bad2", svc, backoff_initial_s=0.1, backoff_max_s=0.1,
                      reset_after_s=0)
