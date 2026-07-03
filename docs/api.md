# API Reference Manual - Swarm Coordination Framework

This manual documents the primary classes, method interfaces, configuration parameters, and code snippets to build applications using the framework.

---

## 1. Core Classes & Setup

### `StateStore`
Thread-safe SQLite database manager for agent vital statistics and task execution logs.

```python
from swarm.core.state import StateStore

# Initialize file-backed SQL persistence
state_store = StateStore("production_swarm.db")

# In-memory option (ideal for test environments)
test_store = StateStore(":memory:")
```

### `EventBroker`
Thread-safe event dispatcher running callbacks asynchronously under a thread pool.

```python
from swarm.core.broker import EventBroker

# Initialize broker with 10 max concurrent worker threads
broker = EventBroker(max_workers=10)
```

### `TaskQueue`
Priority-based execution queue with scheduling delays and role filters.

```python
from swarm.core.queue import TaskQueue, Task

queue = TaskQueue()

# Enqueue a task
task = Task(
    task_id="task-100",
    name="Ingest logs",
    description="Scan syslogs",
    worker_type="ingestor",
    priority=1  # High priority
)
queue.enqueue(task)
```

---

## 2. Agent Wrapper Orchestration

Every agent wrapper inherits from `SwarmAgent` and handles heartbeats, state transitions, sanitization, and evaluation reports automatically.

### CrewAI Wrapper (`CrewAgentWrapper`)
Wraps CrewAI autonomous agents:

```python
from crewai import Agent as CrewAgent
from swarm.core.agent import CrewAgentWrapper

crew_agent = CrewAgent(
    role="Research Assistant",
    goal="Collect stats",
    backstory="Web search robot",
    llm=my_llm
)

research_worker = CrewAgentWrapper(
    crew_agent=crew_agent,
    agent_id="researcher-01",
    name="Crew Researcher",
    role="researcher",
    broker=broker,
    state_store=state_store,
    task_queue=queue,
    schema_class=MyPydanticSchema
)

# Start background polling loop
research_worker.start()
```

### LangChain Wrapper (`LangChainAgentWrapper`)
Wraps any LangChain Expression Language (LCEL) chain or agent executor:

```python
from langchain_core.prompts import PromptTemplate
from swarm.core.agent import LangChainAgentWrapper

chain = PromptTemplate.from_template("{input}") | my_llm

writer_worker = LangChainAgentWrapper(
    chain_or_agent=chain,
    agent_id="writer-01",
    name="LC Summary Writer",
    role="writer",
    broker=broker,
    state_store=state_store,
    task_queue=queue,
    schema_class=MySummarySchema
)

writer_worker.start()
```

### LlamaIndex Wrapper (`LlamaIndexAgentWrapper`)
Wraps LlamaIndex query engines or chat agents:

```python
from swarm.core.agent import LlamaIndexAgentWrapper

query_engine = index.as_query_engine()

search_worker = LlamaIndexAgentWrapper(
    query_engine_or_agent=query_engine,
    agent_id="searcher-01",
    name="LlamaIndex Searcher",
    role="searcher",
    broker=broker,
    state_store=state_store,
    task_queue=queue
)

search_worker.start()
```

---

## 3. Workflow Engine & DAGs

### `WorkflowStep` & `WorkflowCoordinator`
Coordinates Directed Acyclic Graph (DAG) task chains.

```python
from swarm.core.workflow import WorkflowCoordinator, WorkflowStep

coordinator = WorkflowCoordinator(broker=broker, queue=queue, state_store=state_store)

# Define DAG steps
steps = [
    WorkflowStep(
        name="fetch_news",
        description="Fetch latest industry news",
        worker_type="researcher",
        priority=10
    ),
    WorkflowStep(
        name="write_report",
        description="Write Markdown report summary",
        worker_type="writer",
        priority=5,
        depends_on=["fetch_news"]  # Depends on fetch_news completion
    )
]

# Submit DAG
workflow_id = coordinator.submit_workflow(
    workflow_name="Industry Analysis Workflow",
    steps=steps
)
```

---

## 4. Human-in-the-Loop (HITL) Gate

### `HITLGate`
Gives human administrators a checkpoint to inspect and approve/reject executions before passing downstream.

```python
from swarm.core.hitl import HITLGate
from swarm.core.workflow import WorkflowStep

hitl = HITLGate(state_store=state_store, queue=queue, broker=broker)

# Define step with approval required
review_step = WorkflowStep(
    name="operator_review",
    description="Validate summary content",
    worker_type="editor",
    approval_required=True,
    depends_on=["write_report"]
)

# Fetch pending approvals (e.g. to display on a dashboard)
pending = hitl.get_pending_approvals()

# Approve task (unlocks downstream dependency nodes)
hitl.approve_task("wf-xxxx-operator_review", approver_id="admin_1")

# Reject task with feedback
hitl.reject_task("wf-xxxx-operator_review", reason="Spelling errors in paragraph 2", rejecter_id="admin_1")
```
