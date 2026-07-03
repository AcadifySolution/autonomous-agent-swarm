import time
import uuid
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

class EventType(str, Enum):
    TASK_SUBMITTED = "TASK_SUBMITTED"
    TASK_ASSIGNED = "TASK_ASSIGNED"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    AGENT_STATE_CHANGED = "AGENT_STATE_CHANGED"
    EVALUATION_COMPLETED = "EVALUATION_COMPLETED"

class SwarmEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: float = Field(default_factory=time.time)
    sender_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
