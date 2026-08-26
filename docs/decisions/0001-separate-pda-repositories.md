# ADR 0001: Applications use separate repositories

- Status: Accepted
- Date: 2026-08-25

## Context

The shared runtime and the applications built on it change for different reasons. Long-lived Git branches were considered as a way to maintain specialized automation variants.

## Decision

Each automation is a separate application and normally a separate repository that depends on a tagged Automation Harness release. Branches remain temporary units of development work.

## Consequences

Runtime fixes can be versioned and adopted deliberately. Applications can coexist and release independently. Package and version management is required, but permanent branch divergence is avoided.
