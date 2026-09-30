# Runtime Guardrails

Autonomous systems need explicit limits so a transient failure, malformed task, or adversarial input cannot expand into unbounded work.

## Current defaults

| Guardrail | Default |
| --- | ---: |
| Maximum retries | 5 |
| Maximum task description | 20,000 characters |
| Maximum serialized payload | 1 MB |
| Maximum workflow steps | 100 |
| Maximum priority | 100 |
| Recommended worker concurrency ceiling | 32 |

These are library-level defaults. Embedding applications should expose them through their own configuration and enforce stricter values where risk requires it.

## Retry behavior

Retries should be finite and observable. Exponential backoff is useful for transient failures, but retries should not be used for deterministic validation, authorization, or policy failures.

## Human approval

Use HITL gates when a task can create an externally visible, financial, destructive, or otherwise high-impact side effect.

## Auditability

Every externally meaningful decision should carry a correlation ID. Applications can use the bundled audit helper to emit action, actor, decision, task, and correlation metadata without logging raw prompts or secrets.

## Evaluation

Measure task success separately from safety and operational metrics. At minimum track:

- task completion/failure rate
- retry rate
- queue latency
- execution latency
- schema compliance
- approval/rejection rate
- watchdog recoveries
- policy blocks
- tool-call failures
