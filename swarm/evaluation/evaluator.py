import uuid
import time
import logging
from typing import Dict, Any, Optional, Type
from pydantic import BaseModel
from swarm.core.queue import Task
from swarm.core.state import StateStore
from swarm.utils.sanitizer import OutputSanitizer

logger = logging.getLogger(__name__)

class WorkerEvaluator:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    def evaluate_task(
        self,
        task: Task,
        agent_id: str,
        raw_output: str,
        duration: float,
        schema_class: Optional[Type[BaseModel]] = None
    ) -> Dict[str, Any]:
        """
        Evaluate agent execution of a task.
        Computes latency, checks Pydantic schema compliance, and logs metrics.
        """
        eval_id = str(uuid.uuid4())
        compliance = True
        error_msg = None
        score = 5.0  # Default scale 1-5
        
        # Check Pydantic schema compliance if schema is provided
        if schema_class is not None:
            try:
                OutputSanitizer.sanitize_and_validate(raw_output, schema_class)
            except Exception as e:
                compliance = False
                score = 1.0  # Lowest score for compliance failure
                error_msg = str(e)
                logger.debug(f"Evaluation: Schema compliance failed: {error_msg}")

        # Compute additional simple heuristics (e.g. non-empty response)
        if compliance and not raw_output.strip():
            compliance = False
            score = 2.0
            error_msg = "Output is empty"

        metrics_payload = {
            "error_msg": error_msg,
            "raw_output_length": len(raw_output),
            "timestamp": time.time()
        }

        # Save evaluation record
        self.state_store.save_evaluation(
            eval_id=eval_id,
            task_id=task.task_id,
            agent_id=agent_id,
            latency=duration,
            compliance=compliance,
            score=score,
            metrics_payload=metrics_payload
        )
        
        return {
            "eval_id": eval_id,
            "task_id": task.task_id,
            "agent_id": agent_id,
            "latency": duration,
            "compliance": compliance,
            "score": score,
            "metrics_payload": metrics_payload
        }
