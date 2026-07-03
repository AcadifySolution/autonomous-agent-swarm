import time
import threading
import pytest
from swarm.core.queue import Task, TaskQueue

def test_queue_priority_ordering():
    q = TaskQueue()
    
    t1 = Task(task_id="t1", name="Task 1", description="desc", worker_type="worker_a", priority=10)
    t2 = Task(task_id="t2", name="Task 2", description="desc", worker_type="worker_a", priority=5)
    t3 = Task(task_id="t3", name="Task 3", description="desc", worker_type="worker_a", priority=20)
    
    q.enqueue(t1)
    q.enqueue(t2)
    q.enqueue(t3)
    
    assert q.size() == 3
    
    # Lowest priority number should be popped first: t2 (5), t1 (10), t3 (20)
    p1 = q.dequeue()
    assert p1.task_id == "t2"
    
    p2 = q.dequeue()
    assert p2.task_id == "t1"
    
    p3 = q.dequeue()
    assert p3.task_id == "t3"
    
    assert q.size() == 0

def test_queue_worker_type_filtering():
    q = TaskQueue()
    
    t1 = Task(task_id="t1", name="Task 1", description="desc", worker_type="researcher", priority=10)
    t2 = Task(task_id="t2", name="Task 2", description="desc", worker_type="writer", priority=5)
    t3 = Task(task_id="t3", name="Task 3", description="desc", worker_type="researcher", priority=2)
    
    q.enqueue(t1)
    q.enqueue(t2)
    q.enqueue(t3)
    
    # Requesting task for writer should immediately return t2
    w_task = q.dequeue(worker_type="writer")
    assert w_task.task_id == "t2"
    
    # Requesting task for researcher should return t3 (priority 2) instead of t1 (priority 10)
    r_task = q.dequeue(worker_type="researcher")
    assert r_task.task_id == "t3"
    
    r_task2 = q.dequeue(worker_type="researcher")
    assert r_task2.task_id == "t1"

def test_queue_blocking_timeout():
    q = TaskQueue()
    
    start_time = time.time()
    task = q.dequeue(worker_type="researcher", timeout=0.5)
    duration = time.time() - start_time
    
    assert task is None
    assert 0.45 <= duration <= 0.60  # Check it blocked for roughly 0.5s

def test_queue_concurrent_enqueue_dequeue():
    q = TaskQueue()
    dequeued = []
    
    def consumer():
        task = q.dequeue(worker_type="researcher", timeout=2.0)
        if task:
            dequeued.append(task)
            
    t = threading.Thread(target=consumer)
    t.start()
    
    # Delay enqueuing to make the consumer block
    time.sleep(0.3)
    task = Task(task_id="t1", name="Task 1", description="desc", worker_type="researcher", priority=5)
    q.enqueue(task)
    
    t.join(timeout=2.0)
    assert len(dequeued) == 1
    assert dequeued[0].task_id == "t1"
