"""Async ticker example: an asyncio application hosted by the harness.

Demonstrates the Phase 2 runtime without any application-specific behavior:

- a supervised *service* (long-running coroutine) that loops forever and is
  restarted with backoff if it ever fails
- an async interval *job* running on the same event loop
- the same guarantees as the thread-based runtime: single-instance lock,
  graceful shutdown on Ctrl+C, restart recovery, structured logs, and
  data/status.json

Run:
    python examples/async_ticker/ticker.py

Stop it (Ctrl+C or kill the process) and start it again: the schedule, run
history, service state, and tick counts persist in data/.
"""

import asyncio
import contextlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from automation_harness import Harness, HarnessConfig, JobContext, ServiceContext


async def ticker(ctx: ServiceContext) -> None:
    """A long-running service: tick once a second until asked to stop."""
    ticks = ctx.get_state("ticks", 0)
    while not ctx.stop_event.is_set():
        ticks += 1
        ctx.set_state("ticks", ticks)
        print(f"service tick #{ticks} (restart #{ctx.restart_count})")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ctx.stop_event.wait(), timeout=1.0)


async def pulse(ctx: JobContext) -> None:
    """An async interval job, scheduled every 7 seconds."""
    beats = ctx.get_state("beats", 0) + 1
    ctx.set_state("beats", beats)
    print(f"job pulse #{beats} (run_id={ctx.run_id})")


def main() -> None:
    harness = Harness(HarnessConfig(data_dir=Path(__file__).parent / "data"), name="ticker")
    harness.add_service("ticker", ticker, backoff_initial_s=1, backoff_max_s=30,
                        max_consecutive_failures=5)
    harness.add_job("pulse", pulse, every_s=7, run_immediately=True)
    # Alert delivery is application code: this example prints, a real app
    # would page its operator however it likes.
    harness.on_alert(lambda event: print(f"ALERT: {event['service']} escalated "
                                         f"after {event['consecutive_failures']} failures"))
    harness.run()  # services/async jobs present: harness owns the event loop


if __name__ == "__main__":
    main()
