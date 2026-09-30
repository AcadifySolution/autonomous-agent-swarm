"""Minimal audit event helpers for autonomous execution."""

import json
import logging
import time
import uuid
from typing import Any

logger = logging.getLogger("swarm.audit")


def audit_event(
    *,
    action: str,
    actor: str,
    correlation_id: str,
    task_id: str | None = None,
    decision: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Emit a structured, non-secret audit event."""
    record = {
        "audit_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "action": action,
        "actor": actor,
        "correlation_id": correlation_id,
        "task_id": task_id,
        "decision": decision,
        "details": details or {},
    }
    logger.info(json.dumps(record, sort_keys=True))
