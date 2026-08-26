# ADR 0003: Integration identities and destinations are deployment configuration

- Status: Accepted
- Date: 2026-08-25

## Context

External integrations require credentials, account identities, and destination identifiers. Publishing or hard-coding those values would be unsafe and would tie the runtime to one deployment.

## Decision

Real credentials and private deployment identifiers remain in ignored local configuration or an external secret store. Repositories commit only blank examples. Integration destinations are explicit and have no implicit production fallback.

## Consequences

Deployments can choose and replace integration accounts without changing harness code. Test failures can be isolated from production systems, and accidental production delivery is less likely.
