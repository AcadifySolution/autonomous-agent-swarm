import logging
import time
from typing import Dict, List, Any, Optional

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore

logger = logging.getLogger(__name__)

class HITLGate:
    def __init__(self, state_store: StateStore, queue: TaskQueue, broker: EventBroker):
        self.state_store = state_store
        self.queue = queue
        self.broker = broker

    def approve_task(self, task_id: str, approver_id: str = "operator") -> bool:
        """Approve a task blocked on PENDING_APPROVAL and enqueue it for execution."""
        task_record = self.state_store.get_task(task_id)
        if not task_record:
            logger.error(f"HITL Gate: Task {task_id} not found.")
            return False
            
        if task_record["status"] != "PENDING_APPROVAL":
            logger.warning(f"HITL Gate: Task {task_id} is in status '{task_record['status']}', expected 'PENDING_APPROVAL'.")
            return False

        logger.info(f"HITL Gate: Operator '{approver_id}' APPROVED task '{task_id}'. Enqueuing task...")
        
        # Reconstruct Task object
        task = Task(
            task_id=task_record["task_id"],
            name=task_record["name"],
            description=task_record["description"],
            worker_type=task_record["worker_type"],
            priority=task_record["priority"],
            payload=task_record["payload"],
            correlation_id=task_record["correlation_id"],
            max_retries=task_record["max_retries"],
            current_retry=task_record["current_retry"],
            parent_task_ids=task_record["parent_task_ids"],
            workflow_id=task_record["workflow_id"]
        )
        
        # Save state to database as PENDING (which ready checks or enqueuers need)
        self.state_store.save_task(task, status="PENDING")
        
        # Enqueue the task
        self.queue.enqueue(task)
        
        # Publish event
        self.broker.publish(SwarmEvent(
            event_type=EventType.AGENT_STATE_CHANGED,  # Or TASK_SUBMITTED
            sender_id=f"HITL-{approver_id}",
            correlation_id=task.correlation_id,
            payload={"task_id": task_id, "hitl_action": "APPROVED", "approver_id": approver_id}
        ))
        return True

    def reject_task(self, task_id: str, reason: str, rejecter_id: str = "operator") -> bool:
        """Reject a task blocked on PENDING_APPROVAL, stopping downstream step dependency execution."""
        task_record = self.state_store.get_task(task_id)
        if not task_record:
            logger.error(f"HITL Gate: Task {task_id} not found.")
            return False
            
        if task_record["status"] != "PENDING_APPROVAL":
            logger.warning(f"HITL Gate: Task {task_id} is in status '{task_record['status']}', expected 'PENDING_APPROVAL'.")
            return False

        logger.warning(f"HITL Gate: Operator '{rejecter_id}' REJECTED task '{task_id}'. Reason: {reason}")
        
        # Reconstruct Task object
        task = Task(
            task_id=task_record["task_id"],
            name=task_record["name"],
            description=task_record["description"],
            worker_type=task_record["worker_type"],
            priority=task_record["priority"],
            payload=task_record["payload"],
            correlation_id=task_record["correlation_id"],
            max_retries=task_record["max_retries"],
            current_retry=task_record["current_retry"],
            parent_task_ids=task_record["parent_task_ids"],
            workflow_id=task_record["workflow_id"]
        )
        
        # Save status as FAILED with rejection error
        error_msg = f"Rejected by human operator: {reason}"
        self.state_store.save_task(task, status="FAILED", error=error_msg)
        
        # Publish task failure event so workflow/subscribers clean up downstream DAG steps
        self.broker.publish(SwarmEvent(
            event_type=EventType.TASK_FAILED,
            sender_id=f"HITL-{rejecter_id}",
            correlation_id=task.correlation_id,
            payload={"task_id": task_id, "hitl_action": "REJECTED", "rejecter_id": rejecter_id, "error": error_msg}
        ))
        return True

    def get_pending_approvals(self) -> List[Dict[str, Any]]:
        """Retrieve list of tasks currently awaiting approval in the database."""
        # We query the SQLite state store directly by filtering tasks on PENDING_APPROVAL status.
        # Since state store handles connection pooling under mutex, we wrap this query.
        db = self.state_store
        with db._lock:
            conn = db._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE status = 'PENDING_APPROVAL'")
            rows = cursor.fetchall()
            
            results = []
            for r in rows:
                import json
                res = dict(r)
                res["payload"] = json.loads(res["payload"])
                res["parent_task_ids"] = json.loads(res["parent_task_ids"]) if res["parent_task_ids"] else []
                results.append(res)
            return results
