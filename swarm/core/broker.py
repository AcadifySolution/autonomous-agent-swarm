import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Set
from swarm.core.event import EventType, SwarmEvent

logger = logging.getLogger(__name__)

class EventBroker:
    def __init__(self, max_workers: int = 10):
        self._subscribers: Dict[EventType, Set[Callable[[SwarmEvent], None]]] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="SwarmBroker")

    def subscribe(self, event_type: EventType, callback: Callable[[SwarmEvent], None]) -> None:
        """Subscribe a callback to a specific event type."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = set()
            self._subscribers[event_type].add(callback)
            logger.debug(f"Callback registered for event type: {event_type}")

    def unsubscribe(self, event_type: EventType, callback: Callable[[SwarmEvent], None]) -> None:
        """Unsubscribe a callback from a specific event type."""
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type].discard(callback)
                logger.debug(f"Callback unregistered for event type: {event_type}")

    def publish(self, event: SwarmEvent) -> None:
        """Publish an event to all subscribed callbacks asynchronously."""
        with self._lock:
            # Copy the callbacks set to avoid issues if subscribers mutate during iteration
            callbacks = list(self._subscribers.get(event.event_type, []))
            
        if not callbacks:
            logger.debug(f"No subscribers for event: {event.event_type}")
            return

        for callback in callbacks:
            self._executor.submit(self._safe_dispatch, callback, event)

    def _safe_dispatch(self, callback: Callable[[SwarmEvent], None], event: SwarmEvent) -> None:
        """Execute a callback safely and catch any exceptions."""
        try:
            callback(event)
        except Exception as e:
            logger.error(f"Error in subscriber callback for event {event.event_type}: {e}", exc_info=True)

    def shutdown(self) -> None:
        """Shutdown the executor thread pool."""
        self._executor.shutdown(wait=True)
