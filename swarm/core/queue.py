import heapq
import time
import threading
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class Task(BaseModel):
    task_id: str
    name: str
    description: str
    worker_type: str
    priority: int = 10  # Lower number means higher priority (e.g. 0 is highest)
    payload: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    
    # Industrial upgrades
    available_at: float = Field(default_factory=time.time)
    max_retries: int = 3
    current_retry: int = 0
    parent_task_ids: List[str] = Field(default_factory=list)
    workflow_id: Optional[str] = None

    # For heapq comparison, sort primarily by priority, then by created_at timestamp
    def __lt__(self, other: "Task") -> bool:
        if self.priority == other.priority:
            return self.created_at < other.created_at
        return self.priority < other.priority

class TaskQueue:
    def __init__(self):
        self._tasks: List[Task] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def enqueue(self, task: Task) -> None:
        """Add a task to the queue and notify waiting threads."""
        with self._lock:
            heapq.heappush(self._tasks, task)
            self._condition.notify_all()

    def dequeue(self, worker_type: Optional[str] = None, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Pop the highest priority available task from the queue.
        If worker_type is specified, pop the highest priority task matching that worker_type.
        Only returns tasks where task.available_at <= current_time.
        Blocks until a task is available, or the timeout expires.
        """
        start_time = time.time()
        with self._lock:
            while True:
                now = time.time()
                # Find the best task matching the worker_type and is currently active
                task_index = self._find_available_matching_task_index(worker_type, now)
                
                if task_index is not None:
                    # Remove item and re-heapify
                    if task_index == 0:
                        return heapq.heappop(self._tasks)
                    else:
                        task = self._tasks.pop(task_index)
                        heapq.heapify(self._tasks)
                        return task

                # No currently available matching task. Calculate how long to block.
                # Check if there are future delayed matching tasks.
                next_wakeup_delay = self._get_next_wakeup_delay(worker_type, now)
                
                # Determine how long we can afford to wait on the condition lock
                wait_time: Optional[float] = None
                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        return None
                    wait_time = min(remaining, next_wakeup_delay) if next_wakeup_delay is not None else remaining
                else:
                    wait_time = next_wakeup_delay

                # Wait on condition. If it returns due to timeout (and not a notify),
                # we loop back to check if the delayed task is now available.
                if wait_time is not None:
                    self._condition.wait(wait_time)
                else:
                    self._condition.wait()
                    
                # If we have a timeout and we've exceeded it, check one last time, else return None
                if timeout is not None and (time.time() - start_time) >= timeout:
                    # Final check before giving up
                    now = time.time()
                    task_index = self._find_available_matching_task_index(worker_type, now)
                    if task_index is not None:
                        if task_index == 0:
                            return heapq.heappop(self._tasks)
                        else:
                            task = self._tasks.pop(task_index)
                            heapq.heapify(self._tasks)
                            return task
                    return None

    def _find_available_matching_task_index(self, worker_type: Optional[str], current_time: float) -> Optional[int]:
        """Find the index of the highest priority task matching worker_type that is currently active."""
        if not self._tasks:
            return None

        # Gather all active matching tasks
        matching_indices = [
            i for i, t in enumerate(self._tasks) 
            if (worker_type is None or t.worker_type == worker_type) and t.available_at <= current_time
        ]
        if not matching_indices:
            return None

        # Find the one with highest priority (min heap comparison)
        best_index = matching_indices[0]
        for idx in matching_indices[1:]:
            if self._tasks[idx] < self._tasks[best_index]:
                best_index = idx
        return best_index

    def _get_next_wakeup_delay(self, worker_type: Optional[str], current_time: float) -> Optional[float]:
        """Get the time in seconds until the next delayed matching task becomes available."""
        delayed_tasks = [
            t.available_at for t in self._tasks 
            if (worker_type is None or t.worker_type == worker_type) and t.available_at > current_time
        ]
        if not delayed_tasks:
            return None
        return min(delayed_tasks) - current_time

    def size(self) -> int:
        """Return total number of tasks in the queue (including delayed ones)."""
        with self._lock:
            return len(self._tasks)

    def clear(self) -> None:
        """Clear the queue."""
        with self._lock:
            self._tasks.clear()
            self._condition.notify_all()
