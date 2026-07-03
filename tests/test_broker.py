import time
import pytest
from swarm.core.event import EventType, SwarmEvent
from swarm.core.broker import EventBroker

def test_broker_subscribe_and_publish():
    broker = EventBroker(max_workers=2)
    received_events = []
    
    def callback(event: SwarmEvent):
        received_events.append(event)
        
    broker.subscribe(EventType.TASK_SUBMITTED, callback)
    
    event = SwarmEvent(
        event_type=EventType.TASK_SUBMITTED,
        sender_id="test_sender",
        payload={"task_name": "test"}
    )
    
    broker.publish(event)
    
    # Wait for async delivery (up to 1s)
    start = time.time()
    while len(received_events) < 1 and time.time() - start < 1.0:
        time.sleep(0.05)
        
    assert len(received_events) == 1
    assert received_events[0].event_id == event.event_id
    assert received_events[0].payload["task_name"] == "test"
    broker.shutdown()

def test_broker_unsubscribe():
    broker = EventBroker(max_workers=2)
    received_events = []
    
    def callback(event: SwarmEvent):
        received_events.append(event)
        
    broker.subscribe(EventType.TASK_SUBMITTED, callback)
    broker.unsubscribe(EventType.TASK_SUBMITTED, callback)
    
    event = SwarmEvent(
        event_type=EventType.TASK_SUBMITTED,
        sender_id="test_sender"
    )
    broker.publish(event)
    
    time.sleep(0.2)
    assert len(received_events) == 0
    broker.shutdown()

def test_broker_error_handling_in_callback():
    broker = EventBroker(max_workers=2)
    executed = []
    
    def faulty_callback(event: SwarmEvent):
        raise RuntimeError("Callback failure simulation")
        
    def healthy_callback(event: SwarmEvent):
        executed.append(event)
        
    broker.subscribe(EventType.TASK_SUBMITTED, faulty_callback)
    broker.subscribe(EventType.TASK_SUBMITTED, healthy_callback)
    
    event = SwarmEvent(
        event_type=EventType.TASK_SUBMITTED,
        sender_id="test_sender"
    )
    broker.publish(event)
    
    start = time.time()
    while len(executed) < 1 and time.time() - start < 1.0:
        time.sleep(0.05)
        
    # The healthy callback should still execute despite the faulty one raising an error
    assert len(executed) == 1
    broker.shutdown()
