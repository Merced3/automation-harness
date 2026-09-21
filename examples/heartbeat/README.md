# Heartbeat example

A minimal, intentionally neutral automation that demonstrates the harness runtime:
lifecycle, persistent scheduling, graceful shutdown, restart recovery, job state,
and status reporting.

Run it:

```bash
python examples/heartbeat/heartbeat.py
```

It writes a heartbeat every 5 seconds. Stop it (Ctrl+C, or kill the process) and
start it again:

- the beat count persists via job state (`ctx.get_state` / `ctx.set_state`)
- the schedule survives in `data/harness.db`
- runs cut off mid-execution are marked `interrupted`
- a second copy refuses to start while the first is alive (single-instance lock)
- current operational status is always in `data/status.json`
