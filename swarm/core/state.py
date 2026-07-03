import json
import sqlite3
import threading
import time
from typing import Dict, Any, List, Optional
from swarm.core.queue import Task
from swarm.core.event import SwarmEvent

class StateStore:
    def __init__(self, db_path: str = "swarm.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get the shared SQLite connection."""
        return self._conn

    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create agents table with heartbeats and assigned task tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_active REAL NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    assigned_task_id TEXT
                )
            """)
            
            # Create tasks table supporting retries, DAG relationships, and workflows
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    worker_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    correlation_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    max_retries INTEGER DEFAULT 3,
                    current_retry INTEGER DEFAULT 0,
                    parent_task_ids TEXT, -- JSON array of parent task IDs
                    workflow_id TEXT
                )
            """)
            
            # Create evaluations table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS evaluations (
                    eval_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    latency REAL NOT NULL,
                    compliance INTEGER NOT NULL,
                    score REAL NOT NULL,
                    metrics_payload TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id),
                    FOREIGN KEY(agent_id) REFERENCES agents(agent_id)
                )
            """)
            
            # Create swarm_events audit table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS swarm_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    sender_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    correlation_id TEXT
                )
            """)
            
            conn.commit()

    def save_agent(self, agent_id: str, name: str, role: str, status: str, assigned_task_id: Optional[str] = None) -> None:
        """Persist or update an agent's state."""
        now = time.time()
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO agents (agent_id, name, role, status, last_active, last_heartbeat, assigned_task_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    name = excluded.name,
                    role = excluded.role,
                    status = excluded.status,
                    last_active = excluded.last_active,
                    last_heartbeat = excluded.last_heartbeat,
                    assigned_task_id = COALESCE(excluded.assigned_task_id, agents.assigned_task_id)
            """, (agent_id, name, role, status, now, now, assigned_task_id))
            conn.commit()

    def record_heartbeat(self, agent_id: str, status: Optional[str] = None, assigned_task_id: Optional[str] = None) -> None:
        """Record an agent heartbeat, updating status or currently assigned task."""
        now = time.time()
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if status is not None and assigned_task_id is not None:
                cursor.execute("""
                    UPDATE agents SET 
                        last_heartbeat = ?, 
                        status = ?, 
                        assigned_task_id = ?
                    WHERE agent_id = ?
                """, (now, status, assigned_task_id, agent_id))
            elif status is not None:
                cursor.execute("""
                    UPDATE agents SET 
                        last_heartbeat = ?, 
                        status = ?
                    WHERE agent_id = ?
                """, (now, status, agent_id))
            else:
                cursor.execute("""
                    UPDATE agents SET 
                        last_heartbeat = ?
                    WHERE agent_id = ?
                """, (now, agent_id))
                
            conn.commit()

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve agent state."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_agents(self) -> List[Dict[str, Any]]:
        """Retrieve states for all agents."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM agents")
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def get_crashed_agents(self, heartbeat_threshold_seconds: float) -> List[Dict[str, Any]]:
        """Find agents that haven't sent a heartbeat within the threshold and are not OFFLINE."""
        now = time.time()
        cutoff = now - heartbeat_threshold_seconds
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM agents 
                WHERE last_heartbeat < ? AND status != 'OFFLINE'
            """, (cutoff,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def save_task(
        self,
        task: Task,
        status: str,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        current_retry: Optional[int] = None,
        max_retries: Optional[int] = None,
        parent_task_ids: Optional[List[str]] = None,
        workflow_id: Optional[str] = None
    ) -> None:
        """Persist or update a task's state with full retry and workflow parameters."""
        now = time.time()
        payload_str = json.dumps(task.payload)
        result_str = json.dumps(result) if result is not None else None
        
        # Pull parent IDs and retries from task object or custom arguments
        parents_str = json.dumps(parent_task_ids) if parent_task_ids is not None else getattr(task, 'parent_task_ids', None)
        if isinstance(parents_str, list):
            parents_str = json.dumps(parents_str)
            
        m_retries = max_retries if max_retries is not None else getattr(task, 'max_retries', 3)
        c_retry = current_retry if current_retry is not None else getattr(task, 'current_retry', 0)
        wf_id = workflow_id if workflow_id is not None else getattr(task, 'workflow_id', None)
        
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO tasks (
                    task_id, name, description, worker_type, status, payload, 
                    result, error, correlation_id, created_at, updated_at, 
                    max_retries, current_retry, parent_task_ids, workflow_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status = excluded.status,
                    result = COALESCE(excluded.result, tasks.result),
                    error = COALESCE(excluded.error, tasks.error),
                    current_retry = COALESCE(excluded.current_retry, tasks.current_retry),
                    max_retries = COALESCE(excluded.max_retries, tasks.max_retries),
                    parent_task_ids = COALESCE(excluded.parent_task_ids, tasks.parent_task_ids),
                    workflow_id = COALESCE(excluded.workflow_id, tasks.workflow_id),
                    updated_at = excluded.updated_at
            """, (
                task.task_id, task.name, task.description, task.worker_type, status, payload_str,
                result_str, error, task.correlation_id, task.created_at, now,
                m_retries, c_retry, parents_str, wf_id
            ))
            conn.commit()

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve task state."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            if row:
                res = dict(row)
                res["payload"] = json.loads(res["payload"])
                res["result"] = json.loads(res["result"]) if res["result"] else None
                res["parent_task_ids"] = json.loads(res["parent_task_ids"]) if res["parent_task_ids"] else []
                return res
            return None

    def get_workflow_tasks(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Retrieve all tasks grouped under a specific workflow."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE workflow_id = ?", (workflow_id,))
            rows = cursor.fetchall()
            
            results = []
            for r in rows:
                res = dict(r)
                res["payload"] = json.loads(res["payload"])
                res["result"] = json.loads(res["result"]) if res["result"] else None
                res["parent_task_ids"] = json.loads(res["parent_task_ids"]) if res["parent_task_ids"] else []
                results.append(res)
            return results

    def save_evaluation(self, eval_id: str, task_id: str, agent_id: str, latency: float, compliance: bool, score: float, metrics_payload: Dict[str, Any]) -> None:
        """Persist an evaluation record."""
        metrics_str = json.dumps(metrics_payload)
        compliance_int = 1 if compliance else 0
        
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO evaluations (eval_id, task_id, agent_id, latency, compliance, score, metrics_payload, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (eval_id, task_id, agent_id, latency, compliance_int, score, metrics_str, time.time()))
            conn.commit()

    def get_agent_evaluations(self, agent_id: str) -> List[Dict[str, Any]]:
        """Retrieve evaluations for a specific agent."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM evaluations WHERE agent_id = ? ORDER BY timestamp DESC", (agent_id,))
            rows = cursor.fetchall()
            
            results = []
            for r in rows:
                d = dict(r)
                d["compliance"] = bool(d["compliance"])
                d["metrics_payload"] = json.loads(d["metrics_payload"])
                results.append(d)
            return results

    def save_event(self, event: SwarmEvent) -> None:
        """Persist an event record to the immutable audit table."""
        payload_str = json.dumps(event.payload)
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO swarm_events (event_id, event_type, timestamp, sender_id, payload, correlation_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (event.event_id, event.event_type.value, event.timestamp, event.sender_id, payload_str, event.correlation_id))
            conn.commit()

    def get_all_events(self, correlation_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve all audit events, optionally filtered by correlation_id."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            if correlation_id:
                cursor.execute("SELECT * FROM swarm_events WHERE correlation_id = ? ORDER BY timestamp ASC", (correlation_id,))
            else:
                cursor.execute("SELECT * FROM swarm_events ORDER BY timestamp ASC")
            rows = cursor.fetchall()
            
            results = []
            for r in rows:
                d = dict(r)
                d["payload"] = json.loads(d["payload"])
                results.append(d)
            return results

    def clear_all(self) -> None:
        """Clear all tables in the database (useful for testing)."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM evaluations")
            cursor.execute("DELETE FROM swarm_events")
            cursor.execute("DELETE FROM tasks")
            cursor.execute("DELETE FROM agents")
            conn.commit()

    def close(self) -> None:
        """Close the SQLite connection."""
        with self._lock:
            self._conn.close()
