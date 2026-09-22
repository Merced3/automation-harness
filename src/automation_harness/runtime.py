"""Runtime: lifecycle, scheduling, coordination, recovery, and status."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Coroutine
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
    task: asyncio.Task[None] | None = field(default=None, repr=False)

    def busy(self) -> bool:
        if self.thread is not None and self.thread.is_alive():
            return True
        return self.task is not None and not self.task.done()


ServiceFn = Callable[["ServiceContext"], Coroutine[Any, Any, None]]


@dataclass
class ServiceContext:
    """Passed to every service invocation.

    A service should run until ``stop_event`` is set (shutdown requested),
    then return promptly. ``restart_count`` is how many times this service
    has been restarted after a failure within the current process lifetime.
    """

    harness: Harness
    service_name: str
    run_id: int
    stop_event: asyncio.Event
    restart_count: int = 0

    def get_state(self, key: str, default: Any = None) -> Any:
        return self.harness.store.get_state(f"service.{self.service_name}.{key}", default)

    def set_state(self, key: str, value: Any) -> None:
        self.harness.store.set_state(f"service.{self.service_name}.{key}", value)


@dataclass
class _Service:
    name: str
    fn: ServiceFn
    restart: bool = True
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 60.0
    stop_event: asyncio.Event | None = field(default=None, repr=False)
    task: asyncio.Task[None] | None = field(default=None, repr=False)
    state: str = "pending"  # pending | running | backoff | stopped
    restart_count: int = 0
    last_error: str | None = None


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
        self._services: dict[str, _Service] = {}
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

    def add_service(
        self,
        name: str,
        fn: ServiceFn,
        *,
        restart: bool = True,
        backoff_initial_s: float = 1.0,
        backoff_max_s: float = 60.0,
    ) -> None:
        """Register a long-running supervised coroutine.

        A service is expected to run until its ``ServiceContext.stop_event``
        is set. If it raises, the failure is recorded and the service is
        restarted with exponential backoff (unless restart=False). A service
        that returns cleanly is finished and is not restarted.

        Services require the async runtime: call run() as usual and it will
        delegate to the event loop, or drive it yourself with ``arun()``.
        """
        if name in self._services:
            raise ValueError(f"service {name!r} already registered")
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("service fn must be a coroutine function (async def)")
        if backoff_initial_s <= 0 or backoff_max_s < backoff_initial_s:
            raise ValueError("require 0 < backoff_initial_s <= backoff_max_s")
        self._services[name] = _Service(
            name=name,
            fn=fn,
            restart=restart,
            backoff_initial_s=float(backoff_initial_s),
            backoff_max_s=float(backoff_max_s),
        )

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
        # signal.signal only works on the main thread; when embedded in a
        # worker thread, shutdown must be requested via stop() instead.
        if threading.current_thread() is not threading.main_thread():
            return

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
        """Start and block until stopped.

        Uses the thread-based scheduler for sync-only workloads. If any
        service or coroutine job is registered, delegates to the asyncio
        runtime (``arun``) so the harness owns the event loop.
        """
        if self._needs_async():
            asyncio.run(self.arun())
            return
        self.start()
        self._install_signal_handlers()
        try:
            while not self._stop.is_set():
                self._tick()
                self._stop.wait(timeout=0.5)
        finally:
            self.shutdown()

    def _needs_async(self) -> bool:
        return bool(self._services) or any(
            inspect.iscoroutinefunction(job.fn) for job in self._jobs.values()
        )

    # -- async runtime ----------------------------------------------------

    async def arun(self) -> None:
        """Async entry point: start, supervise services, schedule jobs, block.

        Keeps every guarantee of the thread-based runtime (single-instance
        lock, restart recovery, persistent schedules, status file) while the
        harness owns an asyncio event loop. Signal handlers request shutdown
        via the thread-safe stop event, so this also works on Windows where
        loop.add_signal_handler is unavailable.
        """
        loop = asyncio.get_running_loop()
        self._install_signal_handlers()
        try:
            await asyncio.to_thread(self.start)
            for service in self._services.values():
                service.stop_event = asyncio.Event()
                service.task = asyncio.create_task(
                    self._supervise(service), name=f"service-{service.name}"
                )
            while not self._stop.is_set():
                self._tick_async(loop)
                await asyncio.sleep(0.5)
        finally:
            await self._ashutdown()

    def _tick_async(self, loop: asyncio.AbstractEventLoop) -> None:
        now = utcnow()
        for name in self.store.due_jobs(now):
            job = self._jobs.get(name)
            if job is None:
                continue
            if job.busy():
                logger.warning("job %r still running; skipping overlapping run", name)
                self.store.advance_schedule(name, job.interval_s, now)
                continue
            self.store.advance_schedule(name, job.interval_s, now)
            job.task = loop.create_task(self._run_job_async(job), name=f"job-{name}")
        self._write_status()

    async def _run_job_async(self, job: _Job) -> None:
        run_id = self.store.start_run(job.name, os.getpid(), self.config.node_id)
        log = logging.LoggerAdapter(logger, {"job": job.name, "run_id": run_id})
        log.info("job started")
        try:
            ctx = JobContext(harness=self, job_name=job.name, run_id=run_id)
            if inspect.iscoroutinefunction(job.fn):
                await job.fn(ctx)
            else:
                await asyncio.to_thread(job.fn, ctx)
        except Exception as exc:  # noqa: BLE001 - failures must be recorded, not crash the loop
            self.store.finish_run(run_id, "failed", error=f"{type(exc).__name__}: {exc}")
            log.exception("job failed")
        else:
            self.store.finish_run(run_id, "succeeded")
            log.info("job succeeded")

    async def _supervise(self, service: _Service) -> None:
        log = logging.LoggerAdapter(logger, {"service": service.name})
        backoff = service.backoff_initial_s
        assert service.stop_event is not None
        try:
            while not self._stop.is_set():
                service.state = "running"
                run_id = self.store.start_run(service.name, os.getpid(), self.config.node_id)
                ctx = ServiceContext(
                    harness=self,
                    service_name=service.name,
                    run_id=run_id,
                    stop_event=service.stop_event,
                    restart_count=service.restart_count,
                )
                try:
                    await service.fn(ctx)
                except asyncio.CancelledError:
                    self.store.finish_run(run_id, "interrupted", error="shutdown")
                    raise
                except Exception as exc:  # noqa: BLE001 - supervise, never crash the loop
                    error = f"{type(exc).__name__}: {exc}"
                    self.store.finish_run(run_id, "failed", error=error)
                    service.last_error = error
                    log.exception("service failed")
                    if not service.restart:
                        break
                    service.restart_count += 1
                    service.state = "backoff"
                    log.warning("restarting in %.1fs (restart #%d)",
                                backoff, service.restart_count)
                    try:
                        await asyncio.wait_for(service.stop_event.wait(), timeout=backoff)
                        break  # stop requested during backoff
                    except TimeoutError:
                        pass
                    backoff = min(service.backoff_max_s, backoff * 2)
                else:
                    self.store.finish_run(run_id, "succeeded")
                    log.info("service returned cleanly; not restarting")
                    break
        finally:
            service.state = "stopped"

    async def _ashutdown(self, timeout_s: float = 30.0) -> None:
        """Signal services to stop, wait for them and in-flight jobs, release resources."""
        self._stop.set()
        for service in self._services.values():
            if service.stop_event is not None:
                service.stop_event.set()
        pending: list[asyncio.Task[None]] = [
            s.task for s in self._services.values() if s.task is not None and not s.task.done()
        ]
        pending += [
            j.task for j in self._jobs.values() if j.task is not None and not j.task.done()
        ]
        if pending:
            done, not_done = await asyncio.wait(pending, timeout=timeout_s)
            for task in not_done:
                logger.warning("task %r did not finish before shutdown timeout; cancelling",
                               task.get_name())
                task.cancel()
            if not_done:
                await asyncio.gather(*not_done, return_exceptions=True)
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    logger.error("task %r raised during shutdown: %s",
                                 task.get_name(), task.exception())
        self._write_status(stopped=True)
        self._release_lock()
        self.store.close()
        logger.info("harness %r stopped", self.name)

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
            "services": {
                name: {
                    "state": s.state,
                    "restart_count": s.restart_count,
                    "last_error": s.last_error,
                }
                for name, s in self._services.items()
            },
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
