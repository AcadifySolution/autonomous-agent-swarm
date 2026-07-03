import pytest
from unittest.mock import MagicMock
from swarm.core.workflow import WorkflowCoordinator, WorkflowStep
from swarm.core.broker import EventBroker
from swarm.core.queue import TaskQueue, Task
from swarm.core.state import StateStore
from swarm.core.event import SwarmEvent, EventType

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_workflow.db"
    store = StateStore(str(db_file))
    yield store

@pytest.fixture
def coordinator(temp_db):
    broker = EventBroker()
    queue = TaskQueue()
    return WorkflowCoordinator(broker, queue, temp_db)

def test_workflow_submission(coordinator):
    steps = [
        WorkflowStep(name="step1", description="desc1", worker_type="worker"),
        WorkflowStep(name="step2", description="desc2", worker_type="worker", depends_on=["step1"])
    ]
    
    wf_id = coordinator.submit_workflow("test_wf", steps)
    
    # Check that root step is in queue
    assert coordinator.queue.qsize() == 1
    
    # Check tasks in DB
    tasks = coordinator.state_store.get_workflow_tasks(wf_id)
    assert len(tasks) == 2
    
    t1 = next(t for t in tasks if "step1" in t["task_id"])
    t2 = next(t for t in tasks if "step2" in t["task_id"])
    
    assert t1["status"] == "PENDING"
    assert t2["status"] == "PENDING"
    assert len(t2["parent_task_ids"]) == 1

def test_workflow_cycle_detection(coordinator):
    steps = [
        WorkflowStep(name="step1", description="desc1", worker_type="worker", depends_on=["step2"]),
        WorkflowStep(name="step2", description="desc2", worker_type="worker", depends_on=["step1"])
    ]
    with pytest.raises(ValueError, match="Cycle detected"):
        coordinator.submit_workflow("cycle_wf", steps)

def test_workflow_dependency_unlock(coordinator):
    steps = [
        WorkflowStep(name="step1", description="desc1", worker_type="worker"),
        WorkflowStep(name="step2", description="desc2", worker_type="worker", depends_on=["step1"])
    ]
    wf_id = coordinator.submit_workflow("unlock_wf", steps)
    t1_id = f"wf-{wf_id}-step1"
    
    # Fake completion of step1
    coordinator.state_store.save_task(Task(task_id=t1_id, name="n", description="d", worker_type="w", correlation_id="c"), status="COMPLETED")
    
    # Trigger event
    event = SwarmEvent(event_type=EventType.TASK_COMPLETED, sender_id="test", correlation_id=wf_id, payload={"task_id": t1_id})
    coordinator._on_task_completed(event)
    
    # Now queue should have step 2
    # step1 was enqueued at submission (1), step2 enqueued on unlock (2)
    assert coordinator.queue.qsize() == 2

def test_workflow_cascade_cancellation(coordinator):
    steps = [
        WorkflowStep(name="step1", description="desc1", worker_type="worker"),
        WorkflowStep(name="step2", description="desc2", worker_type="worker", depends_on=["step1"])
    ]
    wf_id = coordinator.submit_workflow("cancel_wf", steps)
    t1_id = f"wf-{wf_id}-step1"
    t2_id = f"wf-{wf_id}-step2"
    
    # Fake permanent failure of step1
    coordinator.state_store.save_task(Task(task_id=t1_id, name="n", description="d", worker_type="w", correlation_id="c"), status="FAILED", workflow_id=wf_id)
    
    # Trigger event
    event = SwarmEvent(event_type=EventType.TASK_FAILED, sender_id="test", correlation_id=wf_id, payload={"task_id": t1_id})
    coordinator._on_task_failed(event)
    
    # Check step2 is CANCELLED
    tasks = coordinator.state_store.get_workflow_tasks(wf_id)
    t2 = next(t for t in tasks if t["task_id"] == t2_id)
    assert t2["status"] == "CANCELLED"
