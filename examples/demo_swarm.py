import os
import time
import logging
import uuid
from typing import List
from pydantic import BaseModel, Field

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s")
logger = logging.getLogger("SwarmDemo")

from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker
from swarm.core.queue import Task, TaskQueue
from swarm.core.state import StateStore
from swarm.core.agent import CrewAgentWrapper, LangChainAgentWrapper

# 1. Define Output Schemas
class ResearchOutput(BaseModel):
    topic: str
    key_findings: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)

class SummaryOutput(BaseModel):
    title: str
    summary: str
    action_items: List[str] = Field(default_factory=list)


def main():
    logger.info("Initializing Swarm Coordination Framework Demo...")
    
    # 2. Initialize Core Swarm Components
    db_path = "swarm_demo.db"
    # Clean up previous db if exists
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass
            
    state_store = StateStore(db_path)
    event_broker = EventBroker(max_workers=5)
    task_queue = TaskQueue()
    
    correlation_id = str(uuid.uuid4())
    
    # 3. Setup Orchestration Events (Task Routing Engine)
    # When Research completes, enqueue a Summarize task
    def handle_research_completed(event: SwarmEvent):
        payload = event.payload
        task_id = payload.get("task_id")
        if task_id != "task-research-01":
            return
            
        result = payload.get("result", {})
        logger.info(f"[Coordinator] Research Task {task_id} completed. Research results: {result}")
        
        # Build Writer Task description and context payload
        writer_desc = f"Summarize the findings on '{result.get('topic')}' and compile action items."
        writer_task = Task(
            task_id=f"task-writer-{uuid.uuid4().hex[:6]}",
            name="Summarization Task",
            description=writer_desc,
            worker_type="writer",
            priority=5,  # High priority
            payload={"research_data": result},
            correlation_id=correlation_id
        )
        
        logger.info(f"[Coordinator] Enqueuing new task: {writer_task.name} ({writer_task.task_id}) for 'writer' agent.")
        task_queue.enqueue(writer_task)
        state_store.save_task(writer_task, status="PENDING")
        
    event_broker.subscribe(EventType.TASK_COMPLETED, handle_research_completed)
    
    # Track when the last task completes to exit demo
    demo_finished = threading_event = time_to_exit = [False]
    def handle_writer_completed(event: SwarmEvent):
        payload = event.payload
        result = payload.get("result", {})
        task_id = payload.get("task_id")
        logger.info(f"[Coordinator] Final Writer Task {task_id} completed! Summary output: {result}")
        demo_finished[0] = True
        
    # We will hook into the same TASK_COMPLETED event but check the worker or result fields
    def global_completed_dispatcher(event: SwarmEvent):
        sender = event.sender_id
        if "writer" in sender.lower():
            handle_writer_completed(event)
            
    event_broker.subscribe(EventType.TASK_COMPLETED, global_completed_dispatcher)

    # 4. Instantiate Mock LLMs for CrewAI & LangChain (so it runs without API keys)
    # We import these inside to prevent import issues before pip completes
    from langchain_community.llms.fake import FakeListLLM
    from crewai import Agent as CrewAgent
    from langchain_core.prompts import PromptTemplate

    # A mock LLM responding with markdown JSON conforming to ResearchOutput
    research_mock_json = """
    ```json
    {
        "topic": "Event-Driven Agent Swarms",
        "key_findings": [
            "Decoupled event broker architecture allows independent scaling.",
            "Thread-safe SQLite layers prevent agent state corruption under race conditions."
        ],
        "sources": ["arXiv:2606.8821", "Google DeepMind Agent Swarm Technical Report"]
    }
    ```
    """
    
    # A mock LLM responding with markdown JSON conforming to SummaryOutput
    writer_mock_json = """
    ```json
    {
        "title": "Key Takeaways: Event-Driven Agent Swarms",
        "summary": "This summary evaluates the reliability of event-driven agent swarms, confirming that thread-safe persistence and priority-based task queues resolve operational race conditions.",
        "action_items": [
            "Utilize reentrant locks around SQLite write processes.",
            "Verify agent priority routing filters."
        ]
    }
    ```
    """
    
    from crewai.llm import LLM as CrewLLM

    fake_research_llm = CrewLLM(model="gpt-4o")
    fake_research_llm.call = lambda *args, **kwargs: research_mock_json
    
    fake_writer_llm = FakeListLLM(responses=[writer_mock_json])

    # 5. Create specialized agent instances
    # Research Agent (CrewAI wrapper)
    crew_research_agent = CrewAgent(
        role="Researcher",
        goal="Gather findings on event-driven swarm architectures.",
        backstory="Expert research bot designed to gather technical data.",
        llm=fake_research_llm,
        allow_delegation=False,
        verbose=False
    )
    
    research_worker = CrewAgentWrapper(
        crew_agent=crew_research_agent,
        agent_id="agent-researcher-01",
        name="CrewAI Research Agent",
        role="researcher",
        broker=event_broker,
        state_store=state_store,
        task_queue=task_queue,
        schema_class=ResearchOutput
    )

    # Writer Agent (LangChain wrapper using modern LCEL prompt | llm)
    prompt = PromptTemplate.from_template("{input}")
    langchain_writer_chain = prompt | fake_writer_llm
    
    writer_worker = LangChainAgentWrapper(
        chain_or_agent=langchain_writer_chain,
        agent_id="agent-writer-01",
        name="LangChain Writer Agent",
        role="writer",
        broker=event_broker,
        state_store=state_store,
        task_queue=task_queue,
        schema_class=SummaryOutput
    )

    # 6. Start Agent threads
    logger.info("Starting agent threads...")
    research_worker.start()
    writer_worker.start()
    
    # 7. Submit the Initial Task (Research Topic)
    initial_task = Task(
        task_id="task-research-01",
        name="Research Agent Swarms",
        description="Search for key papers on event-driven agent swarm coordinator frameworks.",
        worker_type="researcher",
        priority=10,  # Lower priority than writer task, but runs first since it's the only one
        payload={"query": "event-driven agent swarms"},
        correlation_id=correlation_id
    )
    
    logger.info(f"Enqueuing initial task: {initial_task.name} ({initial_task.task_id})")
    state_store.save_task(initial_task, status="PENDING")
    task_queue.enqueue(initial_task)
    
    # 8. Wait for completion
    logger.info("Waiting for demo pipeline execution to complete...")
    max_wait = 20.0
    start_time = time.time()
    while not demo_finished[0] and (time.time() - start_time) < max_wait:
        time.sleep(0.5)
        
    if not demo_finished[0]:
        logger.warning("Demo timed out before completing the pipeline.")
        
    # 9. Clean up and stop agents
    logger.info("Stopping agent threads...")
    research_worker.stop()
    writer_worker.stop()
    event_broker.shutdown()
    
    # 10. Display Database State Records
    logger.info("\n" + "="*50 + "\nDEMO EXECUTION REPORT FROM STATE PERSISTENCE DB\n" + "="*50)
    
    logger.info("\n--- AGENTS TABLE ---")
    agents = state_store.get_all_agents()
    for a in agents:
        logger.info(f"ID: {a['agent_id']} | Name: {a['name']} | Role: {a['role']} | Status: {a['status']}")
        
    logger.info("\n--- EVALUATIONS TABLE ---")
    evals = state_store.get_agent_evaluations("agent-researcher-01")
    for ev in evals:
        logger.info(f"Researcher Agent Eval -> Latency: {ev['latency']:.4f}s | Schema Compliant: {ev['compliance']} | Score: {ev['score']}")
        
    evals_writer = state_store.get_agent_evaluations("agent-writer-01")
    for ev in evals_writer:
        logger.info(f"Writer Agent Eval -> Latency: {ev['latency']:.4f}s | Schema Compliant: {ev['compliance']} | Score: {ev['score']}")
        
    logger.info("\n" + "="*50)
    
    # Cleanup database file
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass
    logger.info("Demo finished successfully.")

if __name__ == "__main__":
    main()
