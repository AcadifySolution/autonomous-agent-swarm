import pytest
import time
from swarm.core.watchdog import SwarmWatchdog
from swarm.core.broker import EventBroker
from swarm.core.queue import TaskQueue, Task
from swarm.core.state import StateStore

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_watchdog.db"
    store = StateStore(str(db_file))
    yield store

@pytest.fixture
def watchdog(temp_db):
    broker = EventBroker()
    queue = TaskQueue()
    # Very short threshold for testing
    return SwarmWatchdog(temp_db, queue, broker, check_interval=0.1, crashed_threshold=0.5)

def test_watchdog_detects_crashed_agent(watchdog):
    store = watchdog.state_store
    
    # Create an agent that is BUSY but hasn't heartbeat in 2 seconds
    store.save_agent("agent_crash", "A1", "role", "BUSY")
    
    # Manually update heartbeat to 2 seconds ago
    with store._lock:
        conn = store._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE agents SET last_heartbeat = ? WHERE agent_id = ?", (time.time() - 2.0, "agent_crash"))
        conn.commit()
        
    crashed = store.get_crashed_agents(0.5)
    assert len(crashed) == 1
    assert crashed[0]["agent_id"] == "agent_crash"
    
    # Run check logic
    watchdog._check_agents()
    
    # Verify status changed to CRASHED
    agent = store.get_agent("agent_crash")
    assert agent["status"] == "CRASHED"

def test_watchdog_recovers_task(watchdog):
    store = watchdog.state_store
    
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="worker")
    store.save_task(task, status="RUNNING")
    
    # Create an agent assigned to this task that crashed
    store.save_agent("agent_crash", "A1", "role", "BUSY", assigned_task_id="t1")
    
    # Manually update heartbeat to 2 seconds ago
    with store._lock:
        conn = store._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE agents SET last_heartbeat = ? WHERE agent_id = ?", (time.time() - 2.0, "agent_crash"))
        conn.commit()
        
    # Run check logic
    watchdog._check_agents()
    
    # Verify task was recovered and status reset to PENDING
    updated_task = store.get_task("t1")
    assert updated_task["status"] == "PENDING"
    
    # Verify task was re-enqueued
    assert watchdog.queue.qsize() == 1
