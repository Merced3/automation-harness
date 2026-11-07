# ADR 0004: Pluggable off-box operational state

- Status: Accepted
- Date: 2026-11-07

## Context

Applications originally stored harness schedules, run history, job/service state,
and process locks in their local `data/` directory. That prevents an application
Droplet from being discarded without also copying operational state. The owner
has deliberately un-deferred centralized operational storage; the earlier
architecture text describing the harness as local-only is superseded for this
concern.

The shared data platform provides one isolated Postgres database per application.
Applications connect through PgBouncer in transaction-pooling mode. Existing local
development and tests must remain useful without managed infrastructure.

## Decision

- `HarnessConfig.database_url` selects the storage backend. An absent URL retains
  SQLite at `data/harness.db`; `HARNESS_DATABASE_URL` configures environment-based
  construction. Deployments map their per-app `HARNESS_<APP>_DATABASE_URL` secret
  to this generic library setting. Postgres support is installed with the
  `postgres` package extra.
- SQLite and Postgres implement the same operational Store contract. Run IDs are
  UUIDs so histories from multiple SQLite sources can be merged without integer
  sequence collisions. Existing SQLite databases migrate in place without losing
  run history.
- Packaged Alembic revisions own the Postgres schema. Store startup upgrades to
  the current revision, and `python -m automation_harness.migrations` supports
  explicit upgrade/downgrade operations. SQLite retains its small built-in
  migration mechanism.
- Postgres uses a database-clock lease keyed by harness name with a unique owner
  UUID, a 30-second default expiry, and heartbeats at one-third of the expiry.
  A competing process refuses to start while the lease is live. If renewal fails
  or ownership is lost, the harness requests shutdown and stops new scheduling.
  A crashed holder's lease becomes acquirable after expiry; clean shutdown
  deletes it immediately. This works through transaction-pooled PgBouncer.
- SQLite retains the existing PID-file lock. Under Postgres no PID lock file is
  created.
- `status.json` remains a disposable local health projection, and structured logs
  remain on stdout/stderr. Neither is authoritative operational state.
- `automation_harness.migrate` opens each SQLite source read-only and performs an
  idempotent merge of runs, schedules, and KV state. Run provenance is retained in
  `run_sources`; source checksums, destination counts, and deterministic table
  checksums are reported. Droplet state is loaded first and later laptop copies
  do not overwrite schedule/KV conflicts by default.

## Alternatives considered

- **Postgres-only:** rejected because it imposes managed infrastructure on local
  development and every existing consumer.
- **Session advisory locking:** initially selected, then rejected after live
  PgBouncer tests showed two holders. Transaction pooling can return the backend
  after each statement even while the client connection remains open, so session
  ownership is not retained. Direct connections passed but are reserved for
  migrations and would consume scarce permanent backends.
- **Longer-lived direct connections:** rejected because direct port 25060 is
  reserved for migrations and one permanent backend per app does not fit the
  shared-cluster connection budget as the application count grows.
- **Discarding history:** rejected because merging is idempotent and avoids the
  unsafe assumption that larger/newer SQLite files contain every older row.
- **Central durable status files:** rejected because health snapshots are derived
  and local health checks already consume `status.json`; centralizing them would
  add writes without improving recovery.

## Consequences

Deployed applications can become stateless after setting their database URL and
adopting a released harness version. SQLite-only consumers retain their previous
setup but migrate to schema version 2 on first open. Applications using Postgres
must install `automation-harness[postgres]` and pin the new release deliberately.

A Postgres outage prevents the runtime from opening its operational store or
acquiring its lease; an established runtime requests shutdown when renewal fails.
The harness never silently falls back to SQLite. Lease expiry means a crashed
instance can take up to the lease interval to recover, and jobs already executing
when ownership is lost finish through normal graceful-shutdown behavior rather
than being killed forcibly. Rollback to an older harness is possible while old SQLite sources remain
unchanged, but rows written only to Postgres after cutover will not appear in
those old files. Source deletion remains subject to the platform deletion gate.
