import uuid
import logging
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore

logger = logging.getLogger(__name__)

class WorkflowStep(BaseModel):
    name: str  # Unique name inside the workflow (e.g. "gather_data")
    description: str
    worker_type: str
    priority: int = 10
    payload: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)  # Names of steps that must finish first
    approval_required: bool = False
    max_retries: int = 3

class WorkflowCoordinator:
    def __init__(self, broker: EventBroker, queue: TaskQueue, state_store: StateStore):
        self.broker = broker
        self.queue = queue
        self.state_store = state_store
        
        # Subscribe to completion and failure events
        self.broker.subscribe(EventType.TASK_COMPLETED, self._on_task_completed)
        self.broker.subscribe(EventType.TASK_FAILED, self._on_task_failed)

    def submit_workflow(self, workflow_name: str, steps: List[WorkflowStep]) -> str:
        """
        Submit a new DAG workflow.
        Returns the generated workflow_id.
        """
        workflow_id = str(uuid.uuid4())
        logger.info(f"Submitting workflow '{workflow_name}' (ID: {workflow_id}) with {len(steps)} steps.")
        
        # Build a lookup map of steps to validate DAG constraints
        steps_map = {step.name: step for step in steps}
        self._validate_dag(steps_map)
        
        # Store all steps in the database as tasks
        for step in steps:
            # Map step names to deterministic task IDs
            task_id = self._get_task_id(workflow_id, step.name)
            parent_ids = [self._get_task_id(workflow_id, parent) for parent in step.depends_on]
            
            task = Task(
                task_id=task_id,
                name=f"{workflow_name} - {step.name}",
                description=step.description,
                worker_type=step.worker_type,
                priority=step.priority,
                payload=step.payload,
                correlation_id=workflow_id,
                max_retries=step.max_retries,
                current_retry=0,
                parent_task_ids=parent_ids,
                workflow_id=workflow_id
            )
            
            # Initial status depends on whether approval is required immediately
            initial_status = "PENDING_APPROVAL" if (step.approval_required and not step.depends_on) else "PENDING"
            self.state_store.save_task(task, status=initial_status, parent_task_ids=parent_ids, workflow_id=workflow_id)
            
        # Enqueue initial root steps (no dependencies)
        for step in steps:
            if not step.depends_on:
                task_id = self._get_task_id(workflow_id, step.name)
                task_record = self.state_store.get_task(task_id)
                if task_record and task_record["status"] == "PENDING":
                    logger.debug(f"Enqueuing root workflow step: {step.name} ({task_id})")
                    self._enqueue_step(task_id)
                    
        return workflow_id

    def _get_task_id(self, workflow_id: str, step_name: str) -> str:
        return f"wf-{workflow_id}-{step_name}"

    def _validate_dag(self, steps_map: Dict[str, WorkflowStep]) -> None:
        """Simple cycle detection (DFS) to validate that steps form a Directed Acyclic Graph."""
        visited: Dict[str, int] = {}  # 0=visiting, 1=visited
        
        def dfs(node_name: str):
            visited[node_name] = 0  # visiting
            step = steps_map[node_name]
            for parent in step.depends_on:
                if parent not in steps_map:
                    raise ValueError(f"Step '{node_name}' depends on non-existent step '{parent}'")
                if visited.get(parent) == 0:
                    raise ValueError(f"Cycle detected in workflow dependencies around step '{node_name}'")
                if parent not in visited:
                    dfs(parent)
            visited[node_name] = 1  # visited
            
        for name in steps_map:
            if name not in visited:
                dfs(name)

    def _enqueue_step(self, task_id: str) -> None:
        """Fetch step task from database and push it into the active execution queue."""
        task_record = self.state_store.get_task(task_id)
        if not task_record:
            return
            
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
        self.queue.enqueue(task)

    def _on_task_completed(self, event: SwarmEvent) -> None:
        """Triggered whenever a task completes. Checks if it unlocks downstream steps."""
        payload = event.payload
        task_id = payload.get("task_id")
        if not task_id:
            return
            
        task_record = self.state_store.get_task(task_id)
        if not task_record or not task_record.get("workflow_id"):
            return
            
        workflow_id = task_record["workflow_id"]
        logger.info(f"Workflow Coordinator: Step {task_id} completed. Checking downstream steps...")
        
        # Query all tasks for this workflow to evaluate downstream conditions
        wf_tasks = self.state_store.get_workflow_tasks(workflow_id)
        
        # Check all tasks that are currently PENDING
        for t in wf_tasks:
            if t["status"] in ("PENDING", "PENDING_APPROVAL"):
                # If this task has parent_task_ids, check if they are all COMPLETED
                parents = t["parent_task_ids"]
                if not parents:
                    continue  # Already enqueued/evaluated at root
                    
                # Find statuses of parent tasks
                parent_records = [self.state_store.get_task(pid) for pid in parents]
                all_completed = all(p is not None and p["status"] == "COMPLETED" for p in parent_records)
                
                if all_completed:
                    # All parent dependencies met!
                    if t["status"] == "PENDING_APPROVAL":
                        # Needs human approval, keep blocked
                        logger.info(f"Workflow Coordinator: Step {t['task_id']} ready but blocked on human approval.")
                    else:
                        # Unlocked! Enqueue it
                        logger.info(f"Workflow Coordinator: Unlocking step {t['task_id']}.")
                        self._enqueue_step(t["task_id"])

    def _on_task_failed(self, event: SwarmEvent) -> None:
        """Triggered if a task fails. Cascade-cancels downstream steps."""
        payload = event.payload
        task_id = payload.get("task_id")
        if not task_id:
            return
            
        task_record = self.state_store.get_task(task_id)
        if not task_record or not task_record.get("workflow_id"):
            return
            
        # Only trigger workflow cancellation if the task status in the DB is FAILED
        # (Meaning it has run out of retries and really failed, not just an intermediate retry failure)
        if task_record["status"] != "FAILED":
            return
            
        workflow_id = task_record["workflow_id"]
        logger.warning(f"Workflow Coordinator: Step {task_id} permanently failed. Cascade-cancelling workflow {workflow_id}...")
        
        # Cascade cancel downstream tasks
        self._cascade_cancel(workflow_id, task_id)
        
        # Publish WORKFLOW_FAILED
        self.broker.publish(SwarmEvent(
            event_type=EventType.TASK_FAILED,  # General error event type or custom workflow event
            sender_id="WorkflowCoordinator",
            correlation_id=workflow_id,
            payload={"workflow_id": workflow_id, "failed_task_id": task_id, "error": task_record.get("error")}
        ))

    def _cascade_cancel(self, workflow_id: str, failed_task_id: str) -> None:
        """Traverse the task list and mark any tasks that depend on the failed task as CANCELLED."""
        wf_tasks = self.state_store.get_workflow_tasks(workflow_id)
        
        # Queue of task IDs that are failed/cancelled
        cancelled_queue = [failed_task_id]
        
        while cancelled_queue:
            target_id = cancelled_queue.pop(0)
            for t in wf_tasks:
                if t["status"] in ("PENDING", "PENDING_APPROVAL") and target_id in t["parent_task_ids"]:
                    # Cancel this step
                    logger.warning(f"Workflow Coordinator: Cancelling downstream step {t['task_id']} due to dependency failure.")
                    # Build dummy task object to save state
                    dummy_task = Task(
                        task_id=t["task_id"],
                        name=t["name"],
                        description=t["description"],
                        worker_type=t["worker_type"],
                        priority=t["priority"],
                        payload=t["payload"],
                        correlation_id=t["correlation_id"]
                    )
                    self.state_store.save_task(dummy_task, status="CANCELLED", error=f"Cancelled due to dependency failure on {target_id}")
                    cancelled_queue.append(t["task_id"])
