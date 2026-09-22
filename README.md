# Automation Harness

A reliable Python foundation for AI agents and algorithmic automations that need to run continuously.

Automation projects often begin as scripts and gradually accumulate schedules, saved state, retries, integrations, health checks, and recovery logic. Automation Harness provides a consistent runtime for those operational concerns so each application can concentrate on the work it is meant to perform.

## Goals

- Run continuously and recover predictably after restarts or failures
- Preserve schedules and operational state across process restarts
- Make failures visible through structured logs and health reporting
- Keep application-specific behavior — including all external services — outside the shared runtime
- Support AI-driven agents as well as deterministic, algorithmic workflows
- Remain portable enough to move between machines and deployments

The project is intentionally focused on orchestration and reliability. It does not prescribe what an automation must do, which model it must use, or which external services it must connect to.

## How applications use it

An automation is developed as its own application and installs Automation Harness as a versioned dependency. The application supplies its workflow while the harness manages the long-running runtime around it.

```text
┌───────────────────────────────────────────┐
│ Automation application                    │
│ AI agent, algorithm, monitor, or workflow │
├───────────────────────────────────────────┤
│ Automation Harness                        │
│ Lifecycle, scheduling, state, recovery    │
├───────────────────────────────────────────┤
│ Operating system and external services    │
└───────────────────────────────────────────┘
```

Keeping applications in separate repositories allows them to be released and deployed independently while sharing improvements to the runtime.

## Status

**Phase 1: reliable runtime** is implemented. The harness provides:

- **Scheduled activation** — interval jobs persisted in SQLite; schedules survive restarts
- **Process coordination** — single-instance lock (a second copy refuses to start; stale locks from dead processes are reclaimed) and graceful shutdown on SIGINT/SIGTERM
- **Restart recovery** — runs cut off mid-execution are marked `interrupted`; missed schedules run one catch-up, never a storm
- **Operation status** — structured JSON logs, `harness.status()` snapshots, and a continuously updated `status.json`
- **Job state** — persistent per-job key/value state via `ctx.get_state` / `ctx.set_state`

**Phase 2: asyncio hosting and services** is implemented. The harness additionally provides:

- **Async runtime** — applications built on asyncio are hosted directly: when a
  service or coroutine job is registered, `harness.run()` owns the event loop and
  keeps every Phase 1 guarantee (lock, recovery, schedules, status file)
- **Supervised services** — long-running coroutines (`harness.add_service`) that run
  until asked to stop, with restart-on-failure and exponential backoff
- **Async jobs** — interval jobs may be plain functions (run in a worker thread) or
  coroutine functions (run on the loop)

```python
from automation_harness import Harness, HarnessConfig

harness = Harness(HarnessConfig(data_dir="data"), name="my-automation")
harness.add_job("my_job", my_function, every_s=300, run_immediately=True)
harness.add_service("my_service", my_async_loop, backoff_max_s=60)
harness.run()  # blocks until Ctrl+C or SIGTERM; owns the event loop when needed
```

Planned next (Phase 3, documented but not yet built): restart escalation for
permanently failing services and a generic, delivery-agnostic alert hook.

See [`examples/heartbeat`](examples/heartbeat/) for the thread-based runtime and
[`examples/async_ticker`](examples/async_ticker/) for the async runtime. The harness is
deliberately agnostic: it has no knowledge of Discord, model providers, or any other
external service. Anything further is built only when a real application proves the need.

## Development

Requires Python 3.11 or newer.

```bash
# Windows
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1

# Linux/macOS
python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install -e ".[dev]"
pytest
ruff check .
```

## Configuration and private data

- `.env.example` documents deployment settings and contains no real values.
- `.env` contains local secrets and deployment identifiers and is ignored by Git.
- Runtime databases, logs, and ingested data are not committed.
- Each application owns its private configuration and runtime data.

## License

No license has been selected yet. See [`LICENSE.md`](LICENSE.md).
