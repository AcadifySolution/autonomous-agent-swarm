import time
import pytest
from pydantic import BaseModel
from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore
from swarm.core.agent import SwarmAgent

class OutputSchema(BaseModel):
    summary: str
    word_count: int

# A simple concrete implementation of SwarmAgent for unit testing
class MockSwarmAgent(SwarmAgent):
    def __init__(self, output_to_return: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_to_return = output_to_return
        self.tasks_executed = []

    def execute_task(self, task: Task) -> str:
        self.tasks_executed.append(task)
        return self.output_to_return

def test_agent_background_execution_loop():
    # Setup
    broker = EventBroker(max_workers=2)
    queue = TaskQueue()
    store = StateStore(":memory:")
    
    agent = MockSwarmAgent(
        output_to_return='{"summary": "Task complete.", "word_count": 2}',
        agent_id="mock_agent_1",
        name="Mock Agent",
        role="worker",
        broker=broker,
        state_store=store,
        task_queue=queue,
        schema_class=OutputSchema
    )
    
    # Subscribe to TASK_COMPLETED
    completed_events = []
    def on_completed(event: SwarmEvent):
        completed_events.append(event)
    broker.subscribe(EventType.TASK_COMPLETED, on_completed)
    
    # Start Agent
    agent.start()
    
    # Check agent state in db is IDLE
    agent_info = store.get_agent("mock_agent_1")
    assert agent_info is not None
    assert agent_info["status"] == "IDLE"
    
    # Enqueue a task
    task = Task(
        task_id="task_1",
        name="Perform Work",
        description="do something",
        worker_type="worker",
        correlation_id="cor_1"
    )
    store.save_task(task, status="PENDING")
    queue.enqueue(task)
    
    # Wait for execution (up to 1s)
    start_time = time.time()
    while len(completed_events) < 1 and time.time() - start_time < 1.0:
        time.sleep(0.05)
        
    # Stop Agent
    agent.stop()
    broker.shutdown()
    
    # Verify execution took place
    assert len(agent.tasks_executed) == 1
    assert agent.tasks_executed[0].task_id == "task_1"
    
    # Verify completed event was published
    assert len(completed_events) == 1
    assert completed_events[0].payload["task_id"] == "task_1"
    assert completed_events[0].payload["result"] == {"summary": "Task complete.", "word_count": 2}
    
    # Verify task state in database is COMPLETED
    task_info = store.get_task("task_1")
    assert task_info is not None
    assert task_info["status"] == "COMPLETED"
    assert task_info["result"] == {"summary": "Task complete.", "word_count": 2}
    
    # Verify agent state is OFFLINE after stopping
    agent_info = store.get_agent("mock_agent_1")
    assert agent_info["status"] == "OFFLINE"
    
    # Verify evaluation was logged
    evals = store.get_agent_evaluations("mock_agent_1")
    assert len(evals) == 1
    assert evals[0]["compliance"] is True
    assert evals[0]["score"] == 5.0
