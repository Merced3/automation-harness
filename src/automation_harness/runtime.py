"""Runtime: lifecycle, scheduling, coordination, recovery, and status."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from .config import HarnessConfig
from .logging_setup import configure_logging
from .store import Store, utcnow

logger = logging.getLogger("automation_harness.runtime")

JobFn = Callable[["JobContext"], None]


@dataclass
class JobContext:
    """Passed to every job invocation."""

    harness: Harness
    job_name: str
    run_id: int

    def get_state(self, key: str, default: Any = None) -> Any:
        return self.harness.store.get_state(f"job.{self.job_name}.{key}", default)

    def set_state(self, key: str, value: Any) -> None:
        self.harness.store.set_state(f"job.{self.job_name}.{key}", value)


@dataclass
class _Job:
    name: str
    fn: JobFn
    interval_s: float
    run_immediately: bool = False
    thread: threading.Thread | None = field(default=None, repr=False)


class AlreadyRunningError(RuntimeError):
    """Raised when another live process holds this deployment's lock."""


class Harness:
    """Long-running runtime for scheduled automation work.

    Usage::

        harness = Harness(HarnessConfig(data_dir="data"))
        harness.add_job("heartbeat", every_s=60, fn=my_job)
        harness.run()   # blocks until SIGINT/SIGTERM or harness.stop()
    """

    def __init__(self, config: HarnessConfig | None = None, *, name: str = "harness"):
        self.config = config or HarnessConfig.from_env()
        self.name = name
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        configure_logging(
            level=os.environ.get("AUTOMATION_HARNESS_LOG_LEVEL", "INFO"),
            json_output=os.environ.get("AUTOMATION_HARNESS_LOG_JSON", "1") != "0",
        )
        self.store = Store(self.config.database_path)
        self._jobs: dict[str, _Job] = {}
        self._stop = threading.Event()
        self._started_at: datetime | None = None
        self._lock_acquired = False
        self._health_checks: dict[str, Callable[[], bool]] = {}

    # -- registration -----------------------------------------------------

    def add_job(
        self,
        name: str,
        fn: JobFn,
        *,
        every_s: float,
        run_immediately: bool = False,
    ) -> None:
        """Register a job to run on a persistent interval schedule."""
        if name in self._jobs:
            raise ValueError(f"job {name!r} already registered")
        if every_s <= 0:
            raise ValueError("every_s must be positive")
        self._jobs[name] = _Job(name=name, fn=fn, interval_s=float(every_s),
                                run_immediately=run_immediately)

    def add_health_check(self, name: str, check: Callable[[], bool]) -> None:
        self._health_checks[name] = check

    # -- lifecycle --------------------------------------------------------

    def _acquire_lock(self) -> None:
        """Single-instance coordination: refuse to start if a live process
        holds the deployment lock. Stale locks from dead processes are reclaimed."""
        path = self.config.lock_path
        if path.exists():
            try:
                pid = int(path.read_text().strip())
            except ValueError:
                pid = -1
            if pid > 0 and _pid_alive(pid):
                raise AlreadyRunningError(
                    f"another harness process (pid {pid}) holds {path}; refusing to start"
                )
            logger.warning("reclaiming stale lock from dead pid %s", pid)
        path.write_text(str(os.getpid()))
        self._lock_acquired = True

    def _release_lock(self) -> None:
        if self._lock_acquired:
            self.config.lock_path.unlink(missing_ok=True)
            self._lock_acquired = False

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: object) -> None:
            logger.info("received signal %s; shutting down", signum)
            self.stop()

        signal.signal(signal.SIGINT, handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handler)

    def start(self) -> None:
        """Acquire the lock, recover from any previous termination, arm schedules."""
        self._acquire_lock()
        interrupted = self.store.mark_interrupted()
        if interrupted:
            logger.warning("marked %d interrupted run(s) from previous process", interrupted)
        now = utcnow()
        for job in self._jobs.values():
            next_at = now if job.run_immediately else now + timedelta(seconds=job.interval_s)
            self.store.upsert_schedule(job.name, job.interval_s, next_at)
        self._started_at = utcnow()
        self._write_status()
        logger.info("harness %r started (node=%s, pid=%d, jobs=%s)",
                    self.name, self.config.node_id, os.getpid(), sorted(self._jobs))

    def run(self) -> None:
        """Start and block in the scheduler loop until stopped."""
        self.start()
        self._install_signal_handlers()
        try:
            while not self._stop.is_set():
                self._tick()
                self._stop.wait(timeout=0.5)
        finally:
            self.shutdown()

    def stop(self) -> None:
        """Request shutdown; safe to call from signals or other threads."""
        self._stop.set()

    def shutdown(self, timeout_s: float = 30.0) -> None:
        """Wait for in-flight jobs, then release resources."""
        self._stop.set()
        deadline = time.monotonic() + timeout_s
        for job in self._jobs.values():
            t = job.thread
            if t and t.is_alive():
                t.join(timeout=max(0.0, deadline - time.monotonic()))
                if t.is_alive():
                    logger.warning("job %r did not finish before shutdown timeout", job.name)
        self._write_status(stopped=True)
        self._release_lock()
        self.store.close()
        logger.info("harness %r stopped", self.name)

    # -- scheduling -------------------------------------------------------

    def _tick(self) -> None:
        now = utcnow()
        for name in self.store.due_jobs(now):
            job = self._jobs.get(name)
            if job is None:
                continue
            if job.thread and job.thread.is_alive():
                logger.warning("job %r still running; skipping overlapping run", name)
                # push schedule forward so we don't re-trigger every tick
                self.store.advance_schedule(name, job.interval_s, now)
                continue
            self.store.advance_schedule(name, job.interval_s, now)
            job.thread = threading.Thread(
                target=self._run_job, args=(job,), name=f"job-{name}", daemon=True
            )
            job.thread.start()
        self._write_status()

    def _run_job(self, job: _Job) -> None:
        run_id = self.store.start_run(job.name, os.getpid(), self.config.node_id)
        log = logging.LoggerAdapter(logger, {"job": job.name, "run_id": run_id})
        log.info("job started")
        try:
            job.fn(JobContext(harness=self, job_name=job.name, run_id=run_id))
        except Exception as exc:  # noqa: BLE001 - failures must be recorded, not crash the loop
            self.store.finish_run(run_id, "failed", error=f"{type(exc).__name__}: {exc}")
            log.exception("job failed")
        else:
            self.store.finish_run(run_id, "succeeded")
            log.info("job succeeded")

    # -- status -----------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """Operational status snapshot: health, schedules, recent runs."""
        checks = {}
        for name, check in self._health_checks.items():
            try:
                checks[name] = bool(check())
            except Exception:  # noqa: BLE001
                checks[name] = False
        healthy = all(checks.values()) if checks else True
        return {
            "harness": self.name,
            "node": self.config.node_id,
            "pid": os.getpid(),
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "running": not self._stop.is_set(),
            "healthy": healthy,
            "health_checks": checks,
            "schedules": self.store.schedule_snapshot(),
            "last_runs": self.store.last_run_per_job(),
            "generated_at": datetime.now(UTC).isoformat(),
        }

    def _write_status(self, stopped: bool = False) -> None:
        try:
            snapshot = self.status()
            if stopped:
                snapshot["running"] = False
            tmp = self.config.status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(snapshot, indent=2, default=str))
            tmp.replace(self.config.status_path)
        except Exception:  # noqa: BLE001 - status writing must never crash the runtime
            logger.exception("failed to write status file")

    def __enter__(self) -> Harness:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()


def _pid_alive(pid: int) -> bool:
    if pid == os.getpid():
        return True
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            return bool(ok) and code.value == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
