# Security Policy

## Scope

This repository is an autonomous-agent orchestration framework. It provides runtime primitives and safety controls; it is not a security certification or a guarantee of safe autonomous behavior.

## Security expectations

Treat model output, tool results, task payloads, and external events as untrusted input.

Before production use, implement:

- authentication and authorization for task submission and administrative actions
- tenant and workspace isolation
- least-privilege tool permissions
- approval gates for high-impact actions
- rate limits and execution budgets
- secret management outside source control
- audit logging and retention
- network egress controls
- prompt-injection defenses
- dependency and container scanning
- backup and recovery procedures

## Autonomous execution safeguards

The framework includes retry limits, payload/description bounds, HITL support, watchdog recovery, and structured audit events. These controls are guardrails, not a substitute for application-level authorization and policy.

Do not connect the swarm directly to destructive or irreversible tools without explicit authorization boundaries and human approval.

## Reporting

Do not disclose suspected vulnerabilities in a public issue. Use the repository organization's private security reporting channel and include reproduction steps, affected component, impact, and mitigation guidance where known.

Never include credentials, private prompts, customer data, or secrets in a report.
