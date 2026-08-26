# ADR 0002: Separate the shared runtime from application behavior

- Status: Accepted
- Date: 2026-08-25

## Context

Long-running automations share operational needs, but each application has different behavior, data, policies, and outputs. Mixing those concerns would make the runtime difficult to reuse.

## Decision

Automation Harness owns common runtime concerns such as lifecycle, scheduling, operational state, recovery, integrations, and health reporting. Applications own their workflows, decision logic, content, and interpretation of data.

## Consequences

Public contracts must remain small and be validated by applications outside this repository. Integration adapters should not import application-specific behavior or assume a particular use case.
