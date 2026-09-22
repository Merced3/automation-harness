# Architecture Plan

## 1. Purpose

Automation Harness is a Python runtime for continuously operating AI agents and algorithmic automations. It provides the durable operational foundation that is commonly rebuilt around individual scripts: lifecycle management, persistent scheduling, state, recovery, logging, health reporting, and integrations.

The harness is deliberately independent of application purpose. An application may use model-driven reasoning, deterministic algorithms, or a combination of both. Its workflow remains outside the harness.

## 2. Design goals

### Reliability

The runtime should fail visibly, preserve enough state to recover safely, and behave predictably after process or machine restarts.

### Long-running operation

Scheduling, lifecycle, shutdown, retries, and health reporting are first-class concerns rather than additions left to each application.

### Clear application boundaries

Applications define their own workflows, prompts, algorithms, source selection, output formatting, and domain state. Shared runtime code remains reusable across unrelated projects.

### Replaceable surroundings

The harness persists to local files and SQLite and talks to nothing else. Any
external system an application needs is added by that application, so replacing
one never requires changing the harness.

### Local-first portability

A deployment should run on one machine with minimal external infrastructure. Its operational state should remain portable and under the operator's control, while leaving room for future distributed deployments.

### Observable operation

Operators should be able to determine whether the runtime is healthy, what it is doing, what last failed, and what is scheduled next.

## 3. Responsibilities

### Runtime responsibilities

- Process lifecycle and graceful shutdown
- Configuration loading and validation
- Structured logging and health reporting
- Persistent scheduling and restart recovery
- Application registration
- Operational state and database migrations
- Failure records
- Version and runtime information

### Application responsibilities

- The purpose and behavior of the automation
- Prompts, algorithms, decision policies, and workflows
- Selection and interpretation of data sources
- Application-specific state transitions and completion rules
- User-facing message content and templates
- Provider and cost strategy
- Permissions appropriate to the application's task

The boundary is architectural rather than a restriction on what applications can accomplish. Applications remain free to implement autonomous, interactive, or deterministic behavior.

## 4. Repository and package boundary

Automation Harness is released as a versioned Python package. Each deployed automation is a separate application and normally a separate repository that pins a known harness version. Git branches are used for temporary development work, not as permanent product variants.

During development, an application may install a neighboring harness checkout as an editable or local dependency. Stable deployments should use a selected tag or packaged release.

## 5. Application registration

The first public API will use explicit registration. An application constructs the runtime and registers its jobs and health checks. This keeps startup behavior visible and testable.

Automatic plugin discovery may be considered after multiple real applications demonstrate an clear need for it.

## 6. Configuration and data ownership

The harness defines configuration mechanisms and validates shared runtime settings. Each application defines its own settings and owns its runtime data.

- Secrets and private deployment identifiers remain in ignored local configuration or an external secret store.
- Repositories contain blank, documented configuration examples.
- Runtime databases and ingested records belong to the deployed application, not the installed harness package.
- Integrations require explicit destinations and permissions; they do not silently fall back to a production target.

## 7. External services are the application's problem

The harness does not know that messaging platforms, model providers, or any
other external service exist. There are no adapter contracts, no messaging
abstractions, and no provider interfaces in this package.

An application that needs Discord, an LLM, or an HTTP API writes or installs
that code itself, inside its own repository. If several applications end up
repeating the same integration code, that shared code can become its own
library (for example, a shared Discord adapter package) — still outside the
harness.

Only if multiple real applications demonstrate the same integration need will
anything be considered for promotion into the harness, and only as a small
capability (like a retry helper), never as knowledge of a specific service.

## 8. Data and future compatibility

Initial records should use stable identifiers, UTC timestamps, node identity, provenance, idempotent operations where practical, and explicit migrations.

These choices preserve future options such as centralized storage, synchronization between nodes, and durable outbox delivery without requiring distributed infrastructure in the first version.

## 9. Updates and deployment

The harness publishes versioned releases. Applications pin a known version and adopt updates deliberately.

Initial updates are operator-controlled: stop or pause new work, allow active work to reach a safe point, back up state, install the selected version, run migrations, restart, and verify health. The runtime will not update its own source code.

A separate deployment supervisor may eventually coordinate safe updates and rollback. That is outside the initial scope.

## 10. Delivery phases

### Phase 0 — Foundation (complete)

- Package and repository skeleton
- Tests and lint configuration
- Configuration template and ignore rules
- Architecture and decision records
- Continuous integration foundation

### Phase 1 — Reliable runtime (complete)

- Application lifecycle
- Structured logs and health state
- SQLite initialization and migrations
- Persistent scheduler
- Graceful shutdown and restart recovery
- Neutral heartbeat example

Acceptance: scheduled work remains correct after process termination and restart. **Met.**

### Phase 2 — Asyncio hosting and services (complete)

The first real applications (starting with discord-hub) are asyncio programs,
and they shared two needs that are generic runtime concerns, not application
behavior:

- **Async runtime.** The harness can own an asyncio event loop. When any
  service or coroutine job is registered, `run()` delegates to `arun()`,
  which supervises coroutines as loop tasks and runs synchronous job
  functions in a worker thread. Every Phase 1 guarantee (single-instance
  lock, restart recovery, persistent schedules, structured logs, status file)
  is unchanged. Signal handling uses the thread-safe stop event rather than
  loop-specific signal APIs, so behavior is identical on Windows and POSIX.
- **Supervised services.** A service is a long-running coroutine, registered
  with `add_service`, expected to run until its stop event is set. Failures
  are recorded as runs and the service is restarted with exponential backoff
  (configurable initial and maximum delay); a clean return ends the service
  without a restart. Service state, restart counts, and last errors appear in
  the status snapshot.

Acceptance: an asyncio application embedding the harness keeps its event loop
and all Phase 1 guarantees, and a crashing service is restarted automatically
without operator intervention. **Met.**

### Phase 3 and beyond — decided by real applications

There is no predetermined roadmap past this point. The next thing built into
the harness will be whatever real applications (Socratic Partner, teaching
agent, discord-hub, and others) turn out to genuinely need and share. Features
are promoted into the harness only after being proven in at least one
application, never in advance.

## 11. Deferred decisions

These are explicitly out of scope until a real application forces the question:

- Distributed scheduling and leader election
- Central database synchronization
- Semantic or vector retrieval
- Multi-model routing and adjudication
- Dynamic plugin discovery
- Self-directed updates
- External deployment supervisor
- Platform-specific service installers
- Messaging, data-source, and model-provider abstractions of any kind
