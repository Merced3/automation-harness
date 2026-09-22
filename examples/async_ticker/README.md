# Async ticker example

A minimal, intentionally neutral asyncio application hosted by the harness.
Demonstrates the Phase 2 runtime: supervised long-running services, async
interval jobs, and the harness owning the event loop — with the same
guarantees as the thread-based runtime.

Run it:

```bash
python examples/async_ticker/ticker.py
```

A service ticks once per second; an async job pulses every 7 seconds. Stop it
(Ctrl+C, or kill the process) and start it again:

- the tick and pulse counts persist via `ctx.get_state` / `ctx.set_state`
- the job schedule survives in `data/harness.db`
- runs cut off mid-execution are marked `interrupted`
- a second copy refuses to start while the first is alive (single-instance lock)
- current operational status, including service state and restart counts, is
  always in `data/status.json`
