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

### Replaceable integrations

External systems are reached through small contracts and adapters. Adding or replacing a storage engine, messaging service, model provider, or data source should not require changing an application's core workflow unnecessarily.

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
- Retry policies and failure records
- Contracts for messaging, data sources, storage, and model providers
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

The first public API will use explicit registration. An application constructs the runtime and registers its jobs, commands, adapters, and health checks. This keeps startup behavior visible and testable.

Automatic plugin discovery may be considered after multiple real applications demonstrate an clear need for it.

## 6. Configuration and data ownership

The harness defines configuration mechanisms and validates shared runtime settings. Each application defines its own settings and owns its runtime data.

- Secrets and private deployment identifiers remain in ignored local configuration or an external secret store.
- Repositories contain blank, documented configuration examples.
- Runtime databases and ingested records belong to the deployed application, not the installed harness package.
- Integrations require explicit destinations and permissions; they do not silently fall back to a production target.

## 7. Adapter architecture

Adapters isolate external systems from the runtime and from application logic. Initial contracts are expected in four broad areas:

### Messaging

Send, receive, and update messages through an external interaction service. Discord is the first planned messaging adapter, but it is not part of the harness's identity or required by its core runtime.

### Data sources

Read external records incrementally while retaining source identity and provenance. Possible sources include messaging platforms, files, repositories, databases, and HTTP APIs.

### Operational storage

Persist schedules, run history, cursors, failure records, and application state. SQLite is the first implementation because it is local, durable, and portable.

### Model providers

Provide a replaceable boundary for model invocation and usage reporting. Prompting, context construction, model selection, and cost policy remain application concerns.

Source adapters and operational stores are separate concepts. A database may be an external data source, a runtime store, or both in different deployments, but those roles should not be conflated in one universal connector.

## 8. Data and future compatibility

Initial records should use stable identifiers, UTC timestamps, node identity, provenance, idempotent operations where practical, and explicit migrations.

These choices preserve future options such as centralized storage, synchronization between nodes, and durable outbox delivery without requiring distributed infrastructure in the first version.

## 9. Updates and deployment

The harness publishes versioned releases. Applications pin a known version and adopt updates deliberately.

Initial updates are operator-controlled: stop or pause new work, allow active work to reach a safe point, back up state, install the selected version, run migrations, restart, and verify health. The runtime will not update its own source code.

A separate deployment supervisor may eventually coordinate safe updates and rollback. That is outside the initial scope.

## 10. Delivery phases

### Phase 0 — Foundation (current)

- Package and repository skeleton
- Tests and lint configuration
- Configuration template and ignore rules
- Architecture and decision records
- Continuous integration foundation

### Phase 1 — Reliable runtime

- Application lifecycle
- Structured logs and health state
- SQLite initialization and migrations
- Persistent scheduler
- Graceful shutdown and restart recovery
- Neutral heartbeat example

Acceptance: scheduled work remains correct after process termination and restart.

### Phase 2 — Adapter foundation

- Small public contracts for messaging, sources, storage, and model providers
- Explicit registration and dependency construction
- Common retry and error semantics
- Test doubles for application development

Acceptance: a small external application can replace an adapter without changing its workflow.

### Phase 3 — First operational integrations

- SQLite operational store
- Initial messaging adapter
- Incremental source ingestion with provenance
- Safe handling of edits, retries, and duplicate delivery

Acceptance: an external application can schedule persistent work, exchange test messages, and incrementally synchronize source records across a restart.

### Phase 4 — External package-boundary test

A small application outside this repository installs a tagged harness version and registers a job, adapter, and health check. This validates the public API outside the source tree.

### Phase 5 — Production hardening

- Extended restart and failure-injection tests
- Backup and migration verification
- Operational status reporting
- Deployment documentation
- Compatibility and release policy

## 11. Deferred decisions

- Distributed scheduling and leader election
- Central database synchronization
- Semantic or vector retrieval
- Multi-model routing and adjudication
- Dynamic plugin discovery
- Self-directed updates
- External deployment supervisor
- Platform-specific service installers
