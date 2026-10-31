# Core constraints — paths and rules where changes stay human-reviewed

Everything listed here is load-bearing for what automation-harness IS.
Automated agents may propose changes in discussion, but merges touching
these paths always require the owner's explicit code-owner approval.
A merge rebuilds ALL deployed services; the ✅ reaction remains the
verified human click, with an additional explicit in-thread confirmation
required before merging harness changes.

## Generic runtime, never application meaning

- The harness owns lifecycle, scheduling, persistent operational state,
  supervision, recovery, structured status, and shared Pi RPC plumbing.
- It stays agnostic of Discord, model providers, prompts, pedagogy,
  conversation policy, and every application's private data.
- Machinery belongs here only after real consumers demonstrate a shared
  need. Application policy must not leak in through a "generic" option.
- Consumers install a versioned dependency; compatibility and recovery
  contracts matter more than convenience for one application.

Guardian files:

- `README.md` and `docs/architecture.md`
- `src/automation_harness/` — lifecycle, locking, schedules, supervision,
  persistent state, alert hooks, and Pi RPC contracts

## State, recovery, and rollout safety

- Preserve single-instance behavior, graceful shutdown, restart recovery,
  and no catch-up storm. State/schema changes need a rollback story.
- Failures stay visible and structured; the harness does not hide or
  interpret application-specific failures.
- A harness merge rolls every service sequentially through health gates.
  Tests must cover shared behavior across supported sync/async consumers.
- `.github/` — CI, Dependabot, and auto-merge wiring.
- `pyproject.toml` — version, runtime dependencies, and packaging.

## Rule of thumb

If a change can break every consumer at once, weaken lock/recovery/state
guarantees, leak application meaning into the runtime, or change a shared
contract without migration evidence, it belongs to a human-reviewed change.
