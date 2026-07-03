import pytest
from swarm.core.hitl import HITLGate
from swarm.core.broker import EventBroker
from swarm.core.queue import TaskQueue, Task
from swarm.core.state import StateStore

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_hitl.db"
    store = StateStore(str(db_file))
    yield store

@pytest.fixture
def hitl_gate(temp_db):
    broker = EventBroker()
    queue = TaskQueue()
    return HITLGate(temp_db, queue, broker)

def test_hitl_approve_task(hitl_gate):
    # Setup pending approval task
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="worker")
    hitl_gate.state_store.save_task(task, status="PENDING_APPROVAL")
    
    # Approve
    success = hitl_gate.approve_task("t1", approver_id="human_1")
    assert success is True
    
    # Check status changed to PENDING
    updated = hitl_gate.state_store.get_task("t1")
    assert updated["status"] == "PENDING"
    
    # Check it was enqueued
    assert hitl_gate.queue.qsize() == 1

def test_hitl_reject_task(hitl_gate):
    # Setup pending approval task
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="worker")
    hitl_gate.state_store.save_task(task, status="PENDING_APPROVAL")
    
    # Reject
    success = hitl_gate.reject_task("t1", reason="bad input", rejecter_id="human_1")
    assert success is True
    
    # Check status changed to FAILED
    updated = hitl_gate.state_store.get_task("t1")
    assert updated["status"] == "FAILED"
    assert "bad input" in updated["error"]
    
    # Check it was NOT enqueued
    assert hitl_gate.queue.qsize() == 0

def test_hitl_get_pending_approvals(hitl_gate):
    task1 = Task(task_id="t1", name="Task 1", description="desc", worker_type="worker")
    task2 = Task(task_id="t2", name="Task 2", description="desc", worker_type="worker")
    
    hitl_gate.state_store.save_task(task1, status="PENDING_APPROVAL")
    hitl_gate.state_store.save_task(task2, status="PENDING")  # Not pending approval
    
    pending = hitl_gate.get_pending_approvals()
    assert len(pending) == 1
    assert pending[0]["task_id"] == "t1"
