import pytest
from pydantic import BaseModel
from swarm.core.queue import Task
from swarm.core.state import StateStore
from swarm.evaluation.evaluator import WorkerEvaluator

class OutputSchema(BaseModel):
    summary: str
    word_count: int

def test_evaluator_successful_compliance():
    # Setup in-memory store
    store = StateStore(":memory:")
    evaluator = WorkerEvaluator(store)
    
    # Save dummy agent and task
    store.save_agent("agent_1", "Agent One", "summarizer", "IDLE")
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="summarizer")
    store.save_task(task, status="RUNNING")
    
    raw_output = '{"summary": "This is a brief summary.", "word_count": 5}'
    
    report = evaluator.evaluate_task(
        task=task,
        agent_id="agent_1",
        raw_output=raw_output,
        duration=1.25,
        schema_class=OutputSchema
    )
    
    assert report["task_id"] == "t1"
    assert report["agent_id"] == "agent_1"
    assert report["latency"] == 1.25
    assert report["compliance"] is True
    assert report["score"] == 5.0
    
    # Check database persistence
    evals = store.get_agent_evaluations("agent_1")
    assert len(evals) == 1
    assert evals[0]["eval_id"] == report["eval_id"]
    assert evals[0]["latency"] == 1.25
    assert evals[0]["compliance"] is True
    assert evals[0]["score"] == 5.0
    assert evals[0]["metrics_payload"]["raw_output_length"] == len(raw_output)

def test_evaluator_failed_compliance():
    store = StateStore(":memory:")
    evaluator = WorkerEvaluator(store)
    
    store.save_agent("agent_1", "Agent One", "summarizer", "IDLE")
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="summarizer")
    store.save_task(task, status="RUNNING")
    
    # Invalid JSON that doesn't comply with schema
    raw_output = '{"invalid_field": "test"}'
    
    report = evaluator.evaluate_task(
        task=task,
        agent_id="agent_1",
        raw_output=raw_output,
        duration=0.5,
        schema_class=OutputSchema
    )
    
    assert report["compliance"] is False
    assert report["score"] == 1.0
    assert "Schema validation failed" in report["metrics_payload"]["error_msg"]
    
    evals = store.get_agent_evaluations("agent_1")
    assert len(evals) == 1
    assert evals[0]["compliance"] is False
    assert evals[0]["score"] == 1.0
