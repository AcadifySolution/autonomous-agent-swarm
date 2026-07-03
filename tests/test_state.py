import time
import os
import threading
import pytest
from swarm.core.queue import Task
from swarm.core.state import StateStore

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_swarm.db"
    store = StateStore(str(db_file))
    yield store
    if db_file.exists():
        try:
            os.remove(db_file)
        except OSError:
            pass

def test_state_agent_crud(temp_db):
    store = temp_db
    
    # Save agent
    store.save_agent("agent1", "Agent One", "researcher", "IDLE")
    agent = store.get_agent("agent1")
    
    assert agent is not None
    assert agent["name"] == "Agent One"
    assert agent["role"] == "researcher"
    assert agent["status"] == "IDLE"
    
    # Update agent
    store.save_agent("agent1", "Agent One", "researcher", "BUSY")
    agent_updated = store.get_agent("agent1")
    assert agent_updated["status"] == "BUSY"

def test_state_task_crud(temp_db):
    store = temp_db
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="researcher", payload={"input": "test"})
    
    # Save new task
    store.save_task(task, status="PENDING")
    saved = store.get_task("t1")
    
    assert saved is not None
    assert saved["name"] == "Task 1"
    assert saved["status"] == "PENDING"
    assert saved["payload"] == {"input": "test"}
    assert saved["result"] is None
    
    # Update task with result
    store.save_task(task, status="COMPLETED", result={"output": "success"})
    updated = store.get_task("t1")
    
    assert updated["status"] == "COMPLETED"
    assert updated["result"] == {"output": "success"}

def test_state_evaluation_logging(temp_db):
    store = temp_db
    
    # Prerequisite: agent and task
    store.save_agent("agent1", "Agent One", "researcher", "IDLE")
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="researcher")
    store.save_task(task, status="COMPLETED")
    
    # Save evaluation
    store.save_evaluation(
        eval_id="eval1",
        task_id="t1",
        agent_id="agent1",
        latency=1.5,
        compliance=True,
        score=4.5,
        metrics_payload={"details": "good output"}
    )
    
    evals = store.get_agent_evaluations("agent1")
    assert len(evals) == 1
    assert evals[0]["eval_id"] == "eval1"
    assert evals[0]["latency"] == 1.5
    assert evals[0]["compliance"] is True
    assert evals[0]["score"] == 4.5
    assert evals[0]["metrics_payload"] == {"details": "good output"}

def test_state_concurrent_writes(temp_db):
    store = temp_db
    num_threads = 10
    num_writes_per_thread = 20
    errors = []
    
    def writer_func(thread_idx: int):
        try:
            for i in range(num_writes_per_thread):
                agent_id = f"agent_{thread_idx}_{i}"
                store.save_agent(agent_id, f"Agent {thread_idx}", "worker", "BUSY")
                task = Task(task_id=f"task_{thread_idx}_{i}", name="Test Task", description="desc", worker_type="worker")
                store.save_task(task, status="COMPLETED", result={"val": i})
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer_func, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    assert len(errors) == 0, f"Encountered DB errors during concurrent writes: {errors}"
    
    # Check that agents and tasks were correctly stored
    all_agents = store.get_all_agents()
    assert len(all_agents) == num_threads * num_writes_per_thread
