"""Runtime guardrails shared by the swarm runtime."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SwarmLimits:
    """Upper bounds that prevent runaway autonomous execution."""

    max_task_retries: int = 5
    max_priority: int = 100
    max_payload_bytes: int = 1_000_000
    max_description_chars: int = 20_000
    max_workflow_steps: int = 100
    max_concurrent_workers: int = 32


DEFAULT_LIMITS = SwarmLimits()


def validate_task_limits(
    *,
    description: str,
    payload: dict,
    max_retries: int,
    priority: int,
    limits: SwarmLimits = DEFAULT_LIMITS,
) -> None:
    if not description.strip():
        raise ValueError("Task description must not be empty.")
    if len(description) > limits.max_description_chars:
        raise ValueError("Task description exceeds the configured size limit.")
    if max_retries < 0 or max_retries > limits.max_task_retries:
        raise ValueError(f"max_retries must be between 0 and {limits.max_task_retries}.")
    if priority < 0 or priority > limits.max_priority:
        raise ValueError(f"priority must be between 0 and {limits.max_priority}.")
    if len(str(payload).encode("utf-8")) > limits.max_payload_bytes:
        raise ValueError("Task payload exceeds the configured size limit.")
