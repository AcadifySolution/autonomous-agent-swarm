import time
import logging
import threading
from typing import Optional

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore

logger = logging.getLogger(__name__)

class SwarmWatchdog:
    def __init__(
        self,
        state_store: StateStore,
        queue: TaskQueue,
        broker: EventBroker,
        check_interval: float = 3.0,
        crashed_threshold: float = 10.0
    ):
        self.state_store = state_store
        self.queue = queue
        self.broker = broker
        self.check_interval = check_interval
        self.crashed_threshold = crashed_threshold
        
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the watchdog monitor loop in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Watchdog is already running.")
            return
            
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="SwarmWatchdog", daemon=True)
        self._thread.start()
        logger.info("SwarmWatchdog daemon started.")

    def stop(self) -> None:
        """Stop the watchdog monitor loop."""
        if self._thread is None:
            return
            
        self._stop_event.set()
        self._thread.join(timeout=5.0)
        logger.info("SwarmWatchdog daemon stopped.")

    def _run_loop(self) -> None:
        """Periodic scan loop for crashed agents."""
        while not self._stop_event.is_set():
            try:
                self._check_agents()
            except Exception as e:
                logger.error(f"Watchdog check failed: {e}", exc_info=True)
                
            # Sleep in increments checking the stop event
            self._stop_event.wait(timeout=self.check_interval)

    def _check_agents(self) -> None:
        """Scan db for crashed agents and recover tasks."""
        crashed = self.state_store.get_crashed_agents(self.crashed_threshold)
        for agent in crashed:
            agent_id = agent["agent_id"]
            assigned_task_id = agent["assigned_task_id"]
            
            logger.warning(f"Watchdog: Detected crashed agent '{agent['name']}' ({agent_id}). Last heartbeat was at {agent['last_heartbeat']}.")
            
            # Mark the agent as CRASHED
            self.state_store.save_agent(
                agent_id=agent_id,
                name=agent["name"],
                role=agent["role"],
                status="CRASHED",
                assigned_task_id=None
            )
            
            # Recover task if assigned
            if assigned_task_id:
                task_record = self.state_store.get_task(assigned_task_id)
                if task_record and task_record["status"] == "RUNNING":
                    logger.info(f"Watchdog: Recovering task '{assigned_task_id}' from crashed agent '{agent_id}'.")
                    
                    # Reconstruct task
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
                    
                    # Update status in db back to PENDING and clear error/result
                    self.state_store.save_task(task, status="PENDING")
                    
                    # Re-enqueue the task
                    self.queue.enqueue(task)
                    
                    # Publish recovery event
                    self.broker.publish(SwarmEvent(
                        event_type=EventType.TASK_ASSIGNED,  # Or a re-assigned event
                        sender_id="Watchdog",
                        correlation_id=task.correlation_id,
                        payload={"task_id": task.task_id, "recovered_from_agent": agent_id}
                    ))
