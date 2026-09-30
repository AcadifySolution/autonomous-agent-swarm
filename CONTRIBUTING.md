# Contributing

## Development workflow

1. Create a focused branch.
2. Keep changes small and composable.
3. Add regression tests for behavior changes.
4. Run `ruff check .`, `black --check .`, and `pytest -q`.
5. Do not commit secrets, private prompts, customer data, or runtime databases.
6. Update architecture/API documentation when contracts change.

## Agent-runtime guidelines

- Keep orchestration, state, queueing, evaluation, and framework adapters separated.
- Treat tool/model outputs as untrusted.
- Preserve correlation IDs through asynchronous execution.
- Make retries bounded and observable.
- Prefer idempotent operations where practical.
- Use HITL gates before high-impact side effects.
- Add a regression test for new concurrency or recovery behavior.

## Pull requests

Include the problem, approach, tests run, safety impact, operational impact, and any migration requirements.
