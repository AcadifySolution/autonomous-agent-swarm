import time
import logging
import threading
from typing import Dict, Any, Optional, Type
from abc import ABC, abstractmethod
from pydantic import BaseModel

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore
from swarm.evaluation.evaluator import WorkerEvaluator
from swarm.utils.sanitizer import OutputSanitizer

logger = logging.getLogger(__name__)

class SwarmAgent(ABC):
    def __init__(
        self,
        agent_id: str,
        name: str,
        role: str,
        broker: EventBroker,
        state_store: StateStore,
        task_queue: TaskQueue,
        schema_class: Optional[Type[BaseModel]] = None,
        heartbeat_interval: float = 2.0
    ):
        self.agent_id = agent_id
        self.name = name
        self.role = role
        self.broker = broker
        self.state_store = state_store
        self.task_queue = task_queue
        self.schema_class = schema_class
        self.heartbeat_interval = heartbeat_interval
        
        self.evaluator = WorkerEvaluator(state_store)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        
        # Live state tracking for heartbeats
        self.current_status = "OFFLINE"
        self.active_task_id: Optional[str] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the agent background loop and heartbeat daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning(f"Agent {self.name} is already running.")
            return
            
        self._stop_event.clear()
        self.current_status = "IDLE"
        self.active_task_id = None
        self.state_store.save_agent(self.agent_id, self.name, self.role, self.current_status)
        
        # Start Heartbeat Thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, 
            name=f"Heartbeat-{self.name}", 
            daemon=True
        )
        self._heartbeat_thread.start()
        
        # Start Execution Thread
        self._thread = threading.Thread(
            target=self._run_loop, 
            name=f"Agent-{self.name}", 
            daemon=True
        )
        self._thread.start()
        logger.info(f"Agent {self.name} started.")

    def stop(self) -> None:
        """Stop the background loops."""
        if self._thread is None:
            return
            
        self._stop_event.set()
        self._thread.join(timeout=5.0)
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5.0)
            
        self.current_status = "OFFLINE"
        self.active_task_id = None
        self.state_store.save_agent(self.agent_id, self.name, self.role, self.current_status)
        logger.info(f"Agent {self.name} stopped.")

    def _heartbeat_loop(self) -> None:
        """Daemon heartbeat check-in loop updating the database."""
        while not self._stop_event.is_set():
            try:
                self.state_store.record_heartbeat(
                    agent_id=self.agent_id,
                    status=self.current_status,
                    assigned_task_id=self.active_task_id
                )
            except Exception as e:
                logger.error(f"Heartbeat failure for agent {self.name}: {e}", exc_info=True)
                
            self._stop_event.wait(timeout=self.heartbeat_interval)

    def _run_loop(self) -> None:
        """Background execution loop polling tasks from the queue."""
        while not self._stop_event.is_set():
            task = self.task_queue.dequeue(worker_type=self.role, timeout=1.0)
            if task is None:
                continue

            logger.info(f"Agent {self.name} picked up task: {task.name} ({task.task_id})")
            
            # Update local state metadata for heartbeat visibility
            self.current_status = "BUSY"
            self.active_task_id = task.task_id
            
            # Sync database
            self.state_store.save_agent(self.agent_id, self.name, self.role, self.current_status, self.active_task_id)
            self.state_store.save_task(task, status="RUNNING")
            
            self.broker.publish(SwarmEvent(
                event_type=EventType.TASK_ASSIGNED,
                sender_id=self.agent_id,
                correlation_id=task.correlation_id,
                payload={"task_id": task.task_id, "agent_id": self.agent_id}
            ))

            start_time = time.time()
            raw_output = ""
            error_str = None
            success = False
            
            try:
                # Execute concrete subclass execution hook
                raw_output = self.execute_task(task)
                duration = time.time() - start_time
                success = True
            except Exception as e:
                duration = time.time() - start_time
                error_str = str(e)
                logger.error(f"Agent {self.name} failed task {task.task_id}: {error_str}", exc_info=True)

            if success:
                sanitized_result = {}
                try:
                    # Sanitize and validate against schema
                    if self.schema_class is not None:
                        sanitized_model = OutputSanitizer.sanitize_and_validate(raw_output, self.schema_class)
                        sanitized_result = sanitized_model.model_dump()
                    else:
                        sanitized_result = OutputSanitizer.sanitize_json(raw_output)
                    
                    eval_report = self.evaluator.evaluate_task(
                        task=task,
                        agent_id=self.agent_id,
                        raw_output=raw_output,
                        duration=duration,
                        schema_class=self.schema_class
                    )
                    
                    self.state_store.save_task(task, status="COMPLETED", result=sanitized_result)
                    self.broker.publish(SwarmEvent(
                        event_type=EventType.TASK_COMPLETED,
                        sender_id=self.agent_id,
                        correlation_id=task.correlation_id,
                        payload={"task_id": task.task_id, "result": sanitized_result, "evaluation": eval_report}
                    ))
                except Exception as eval_err:
                    error_str = f"Sanitization/Validation error: {eval_err}"
                    logger.warning(f"Agent {self.name} output validation failed: {error_str}")
                    
                    eval_report = self.evaluator.evaluate_task(
                        task=task,
                        agent_id=self.agent_id,
                        raw_output=raw_output,
                        duration=duration,
                        schema_class=self.schema_class
                    )
                    self._handle_task_failure(task, error_str, eval_report)
            else:
                eval_report = self.evaluator.evaluate_task(
                    task=task,
                    agent_id=self.agent_id,
                    raw_output=f"ERROR: {error_str}",
                    duration=duration,
                    schema_class=self.schema_class
                )
                self._handle_task_failure(task, error_str, eval_report)

            # Reset local state metadata back to idle
            self.current_status = "IDLE"
            self.active_task_id = None
            self.state_store.save_agent(self.agent_id, self.name, self.role, self.current_status, None)

    def _handle_task_failure(self, task: Task, error_msg: str, eval_report: dict) -> None:
        """Handle execution failures with automated retry-with-backoff scheduling or DLQ routing."""
        if task.current_retry < task.max_retries:
            task.current_retry += 1
            # Exponential backoff: 2s, 4s, 8s...
            backoff_delay = 2.0 ** task.current_retry
            task.available_at = time.time() + backoff_delay
            
            logger.info(f"Agent {self.name}: Task {task.task_id} failed. Retrying (attempt {task.current_retry}/{task.max_retries}) in {backoff_delay}s...")
            
            # Save intermediate status to database
            self.state_store.save_task(task, status="PENDING", error=error_msg, current_retry=task.current_retry)
            
            # Enqueue delayed task back to task queue
            self.task_queue.enqueue(task)
            
            # Publish retriable error event
            self.broker.publish(SwarmEvent(
                event_type=EventType.AGENT_STATE_CHANGED,
                sender_id=self.agent_id,
                correlation_id=task.correlation_id,
                payload={"task_id": task.task_id, "error": error_msg, "retry_attempt": task.current_retry, "next_execution_at": task.available_at}
            ))
        else:
            # Out of retries, mark as permanent FAILED (Dead Letter Queue route)
            logger.error(f"Agent {self.name}: Task {task.task_id} failed permanently after {task.max_retries} retries.")
            self.state_store.save_task(task, status="FAILED", error=error_msg)
            
            self.broker.publish(SwarmEvent(
                event_type=EventType.TASK_FAILED,
                sender_id=self.agent_id,
                correlation_id=task.correlation_id,
                payload={"task_id": task.task_id, "error": error_msg, "evaluation": eval_report}
            ))

    @abstractmethod
    def execute_task(self, task: Task) -> str:
        """Subclasses implement custom LLM reasoning routines here."""
        raise NotImplementedError("execute_task must be implemented by subclasses.")


class CrewAgentWrapper(SwarmAgent):
    def __init__(self, crew_agent, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crew_agent = crew_agent

    def execute_task(self, task: Task) -> str:
        """Execute a task using CrewAI orchestration."""
        from crewai import Task as CrewTask, Crew
        
        crew_task = CrewTask(
            description=task.description,
            expected_output="JSON formatted string complying with the required schema.",
            agent=self.crew_agent
        )
        
        crew = Crew(
            agents=[self.crew_agent],
            tasks=[crew_task],
            verbose=False
        )
        
        result = crew.kickoff(inputs=task.payload)
        if hasattr(result, "raw"):
            return result.raw
        return str(result)


class LangChainAgentWrapper(SwarmAgent):
    def __init__(self, chain_or_agent, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.chain_or_agent = chain_or_agent

    def execute_task(self, task: Task) -> str:
        """Execute a task using LangChain invoke."""
        inputs = {"input": task.description, **task.payload}
        
        response = self.chain_or_agent.invoke(inputs)
        
        if isinstance(response, dict):
            if "output" in response:
                return str(response["output"])
            import json
            return json.dumps(response)
        
        if hasattr(response, "content"):
            return str(response.content)
            
        return str(response)


class LlamaIndexAgentWrapper(SwarmAgent):
    def __init__(self, query_engine_or_agent, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_engine_or_agent = query_engine_or_agent

    def execute_task(self, task: Task) -> str:
        """Execute a query/task using LlamaIndex engine/agent."""
        query_str = f"{task.description}\nPayload context: {task.payload}"
        
        if hasattr(self.query_engine_or_agent, "query"):
            response = self.query_engine_or_agent.query(query_str)
        elif hasattr(self.query_engine_or_agent, "chat"):
            response = self.query_engine_or_agent.chat(query_str)
        else:
            raise AttributeError("Provided LlamaIndex object has neither 'query' nor 'chat' method.")
            
        return str(response)
