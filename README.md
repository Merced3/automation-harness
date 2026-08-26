# Automation Harness

A reliable Python foundation for AI agents and algorithmic automations that need to run continuously.

Automation projects often begin as scripts and gradually accumulate schedules, saved state, retries, integrations, health checks, and recovery logic. Automation Harness provides a consistent runtime for those operational concerns so each application can concentrate on the work it is meant to perform.

## Goals

- Run continuously and recover predictably after restarts or failures
- Preserve schedules and operational state across process restarts
- Make failures visible through structured logs and health reporting
- Connect applications to external services through replaceable adapters
- Keep application-specific behavior outside the shared runtime
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

This repository is in **Phase 0: design and foundation**. The production runtime has not yet been implemented.

See [`docs/architecture.md`](docs/architecture.md) for the current technical plan.

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
