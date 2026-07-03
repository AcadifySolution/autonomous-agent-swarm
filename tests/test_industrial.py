import time
import pytest
from pydantic import BaseModel

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore
from swarm.core.agent import SwarmAgent
from swarm.core.workflow import WorkflowCoordinator, WorkflowStep
from swarm.core.watchdog import SwarmWatchdog
from swarm.core.hitl import HITLGate

class OutputSchema(BaseModel):
    value: str

class AlwaysFailAgent(SwarmAgent):
    def execute_task(self, task: Task) -> str:
        raise RuntimeError("Transient execution failure simulation.")

class SimpleSuccessAgent(SwarmAgent):
    def execute_task(self, task: Task) -> str:
        return '{"value": "success"}'

def test_agent_retry_and_backoff_lifecycle():
    broker = EventBroker(max_workers=2)
    queue = TaskQueue()
    store = StateStore(":memory:")
    
    # Task with max_retries = 2
    task = Task(
        task_id="retry_task",
        name="Retry Task",
        description="will fail",
        worker_type="failed_worker",
        max_retries=2,
        current_retry=0
    )
    
    agent = AlwaysFailAgent(
        agent_id="agent_fail",
        name="Failing Agent",
        role="failed_worker",
        broker=broker,
        state_store=store,
        task_queue=queue,
        schema_class=OutputSchema
    )
    
    store.save_task(task, status="PENDING")
    queue.enqueue(task)
    
    # Track failed events
    failed_events = []
    def on_failed(event: SwarmEvent):
        failed_events.append(event)
    broker.subscribe(EventType.TASK_FAILED, on_failed)
    
    agent.start()
    
    # Wait for execution attempt 1 (fails, current_retry -> 1, backoff 2s)
    time.sleep(0.5)
    
    record1 = store.get_task("retry_task")
    assert record1["status"] == "PENDING"
    assert record1["current_retry"] == 1
    
    # Fake update available_at to now to skip the backoff wait in test
    with store._lock:
        conn = store._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET status = 'PENDING' WHERE task_id = 'retry_task'")
        conn.commit()
    
    # Update queue task available_at
    while queue.size() > 0:
        t = queue.dequeue()
        t.available_at = time.time() - 1.0  # Make active
        queue.enqueue(t)
        break
        
    # Wait for execution attempt 2 (fails, current_retry -> 2, backoff 4s)
    time.sleep(0.5)
    
    record2 = store.get_task("retry_task")
    assert record2["status"] == "PENDING"
    assert record2["current_retry"] == 2
    
    # Reset queue task available_at to execute attempt 3 (which will fail permanently)
    while queue.size() > 0:
        t = queue.dequeue()
        t.available_at = time.time() - 1.0  # Make active
        queue.enqueue(t)
        break
        
    time.sleep(0.5)
    
    # Task should now be in FAILED status (retries exhausted)
    record3 = store.get_task("retry_task")
    assert record3["status"] == "FAILED"
    assert record3["current_retry"] == 2
    assert len(failed_events) == 1
    
    agent.stop()
    broker.shutdown()

def test_workflow_dag_progression():
    broker = EventBroker(max_workers=2)
    queue = TaskQueue()
    store = StateStore(":memory:")
    
    coordinator = WorkflowCoordinator(broker=broker, queue=queue, state_store=store)
    
    # 3 Steps: step_a -> step_b -> step_c
    steps = [
        WorkflowStep(name="step_a", description="A", worker_type="worker_a", depends_on=[]),
        WorkflowStep(name="step_b", description="B", worker_type="worker_b", depends_on=["step_a"]),
        WorkflowStep(name="step_c", description="C", worker_type="worker_c", depends_on=["step_b"])
    ]
    
    workflow_id = coordinator.submit_workflow("Test Workflow", steps)
    
    # Initially, only step_a should be enqueued (no parents)
    assert queue.size() == 1
    task_a = queue.dequeue()
    assert task_a.task_id == f"wf-{workflow_id}-step_a"
    
    # Simulate completion of step_a
    store.save_task(task_a, status="COMPLETED", result={"value": "done"})
    broker.publish(SwarmEvent(
        event_type=EventType.TASK_COMPLETED,
        sender_id="worker_a",
        correlation_id=workflow_id,
        payload={"task_id": task_a.task_id, "result": {"value": "done"}}
    ))
    
    time.sleep(0.2)
    
    # Now step_b should be unlocked and enqueued
    assert queue.size() == 1
    task_b = queue.dequeue()
    assert task_b.task_id == f"wf-{workflow_id}-step_b"
    
    # Simulate completion of step_b
    store.save_task(task_b, status="COMPLETED", result={"value": "done"})
    broker.publish(SwarmEvent(
        event_type=EventType.TASK_COMPLETED,
        sender_id="worker_b",
        correlation_id=workflow_id,
        payload={"task_id": task_b.task_id, "result": {"value": "done"}}
    ))
    
    time.sleep(0.2)
    
    # Now step_c should be enqueued
    assert queue.size() == 1
    task_c = queue.dequeue()
    assert task_c.task_id == f"wf-{workflow_id}-step_c"
    broker.shutdown()

def test_watchdog_failover_recovery():
    broker = EventBroker(max_workers=2)
    queue = TaskQueue()
    store = StateStore(":memory:")
    
    # Initialize Watchdog check_interval=10s, threshold=0.1s
    watchdog = SwarmWatchdog(
        state_store=store,
        queue=queue,
        broker=broker,
        check_interval=10.0,
        crashed_threshold=0.1
    )
    
    # Create agent and assign a running task in DB
    store.save_agent("crashed_agent_01", "Crashed Agent", "worker", "BUSY")
    task = Task(task_id="orphaned_task", name="Orphaned", description="desc", worker_type="worker")
    store.save_task(task, status="RUNNING")
    
    # Inject heartbeat in past (0.5s ago) so it exceeds the 0.1s threshold
    with store._lock:
        conn = store._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE agents 
            SET last_heartbeat = ?, assigned_task_id = 'orphaned_task' 
            WHERE agent_id = 'crashed_agent_01'
        """, (time.time() - 0.5,))
        conn.commit()
        
    # Execute single watchdog check
    watchdog._check_agents()
    
    # Verify agent is marked CRASHED and task is reset and enqueued
    agent_record = store.get_agent("crashed_agent_01")
    assert agent_record["status"] == "CRASHED"
    assert agent_record["assigned_task_id"] is None
    
    task_record = store.get_task("orphaned_task")
    assert task_record["status"] == "PENDING"
    
    assert queue.size() == 1
    t = queue.dequeue()
    assert t.task_id == "orphaned_task"
    broker.shutdown()

def test_hitl_approval_gate():
    broker = EventBroker(max_workers=2)
    queue = TaskQueue()
    store = StateStore(":memory:")
    
    coordinator = WorkflowCoordinator(broker=broker, queue=queue, state_store=store)
    hitl = HITLGate(state_store=store, queue=queue, broker=broker)
    
    # Step requiring approval
    steps = [
        WorkflowStep(name="step_hitl", description="Needs human", worker_type="worker", approval_required=True)
    ]
    
    workflow_id = coordinator.submit_workflow("HITL Workflow", steps)
    
    # Initially status should be PENDING_APPROVAL and queue should be empty
    assert queue.size() == 0
    task_id = f"wf-{workflow_id}-step_hitl"
    record = store.get_task(task_id)
    assert record["status"] == "PENDING_APPROVAL"
    
    # Verify it is returned in pending approvals
    pending = hitl.get_pending_approvals()
    assert len(pending) == 1
    assert pending[0]["task_id"] == task_id
    
    # Approve task
    hitl.approve_task(task_id, approver_id="admin_user")
    
    # Verify status changed and it is in queue
    record_after = store.get_task(task_id)
    assert record_after["status"] == "PENDING"
    assert queue.size() == 1
    broker.shutdown()
