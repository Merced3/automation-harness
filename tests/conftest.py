"""Shared helpers for runtime tests."""

import threading
import time

from automation_harness import Harness


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
