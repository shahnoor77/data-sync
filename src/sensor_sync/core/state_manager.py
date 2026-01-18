"""
State management for tracking event lifecycle and acknowledgments
"""

import time
from typing import Dict, Any, Optional
from enum import Enum
import threading


class EventState(Enum):
    """Event lifecycle states"""
    CAPTURED = "CAPTURED"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    PUBLISHED = "PUBLISHED"
    MQTT_CONFIRMED = "MQTT_CONFIRMED"
    DELIVERED = "DELIVERED"
    WRITTEN = "WRITTEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRY = "RETRY"


class StateManager:
    """
    Manages event state and tracking
    Ensures exactly-once delivery semantics
    """
    
    def __init__(self, logger, metrics):
        """
        Initialize state manager
        
        Args:
            logger: Logger instance
            metrics: Metrics collector
        """
        self.logger = logger
        self.metrics = metrics
        
        # Event tracking
        self._events = {}  # event_id -> event_info
        self._lock = threading.Lock()
        
        # Pending acknowledgments
        self._pending_acks = {}  # mqtt_message_id -> event_id
        
        # Statistics
        self._stats = {
            'total_events': 0,
            'completed_events': 0,
            'failed_events': 0,
            'pending_events': 0
        }
    
    def register_event(
        self,
        event_id: str,
        event_data: Dict[str, Any],
        state: EventState = EventState.CAPTURED
    ):
        """Register a new event"""
        with self._lock:
            self._events[event_id] = {
                'event_id': event_id,
                'state': state,
                'data': event_data,
                'created_at': time.time(),
                'updated_at': time.time(),
                'mqtt_message_id': None,
                'retry_count': 0,
                'error': None
            }
            self._stats['total_events'] += 1
            self._stats['pending_events'] += 1
            
            self.metrics.set_gauge('pending_acks', self._stats['pending_events'])
    
    def update_state(
        self,
        event_id: str,
        state: EventState,
        error: Optional[str] = None,
        mqtt_message_id: Optional[int] = None
    ):
        """Update event state"""
        with self._lock:
            if event_id not in self._events:
                self.logger.warning(f"Unknown event: {event_id}")
                return
            
            event = self._events[event_id]
            event['state'] = state
            event['updated_at'] = time.time()
            
            if error:
                event['error'] = error
            
            if mqtt_message_id:
                event['mqtt_message_id'] = mqtt_message_id
                self._pending_acks[mqtt_message_id] = event_id
            
            # Update statistics
            if state == EventState.COMPLETED:
                self._stats['completed_events'] += 1
                self._stats['pending_events'] -= 1
                self.metrics.increment_counter('events_completed')
            elif state == EventState.FAILED:
                self._stats['failed_events'] += 1
                self._stats['pending_events'] -= 1
                self.metrics.increment_counter('events_failed')
            
            self.metrics.set_gauge('pending_acks', self._stats['pending_events'])
            
            # Log state change
            self.logger.log_event_lifecycle(
                event_id,
                state.value,
                data={
                    'mqtt_message_id': mqtt_message_id
                },
                error=error
            )
    
    def get_event_by_mqtt_id(self, mqtt_message_id: int) -> Optional[str]:
        """Get event ID by MQTT message ID"""
        with self._lock:
            return self._pending_acks.get(mqtt_message_id)
    
    def get_event_state(self, event_id: str) -> Optional[EventState]:
        """Get current state of an event"""
        with self._lock:
            event = self._events.get(event_id)
            return event['state'] if event else None
    
    def get_pending_events(self) -> list:
        """Get list of pending events"""
        with self._lock:
            return [
                event_id
                for event_id, event in self._events.items()
                if event['state'] not in [EventState.COMPLETED, EventState.FAILED]
            ]
    
    def cleanup_old_events(self, max_age_seconds: int = 3600):
        """Clean up old completed/failed events"""
        with self._lock:
            current_time = time.time()
            to_remove = []
            
            for event_id, event in self._events.items():
                if event['state'] in [EventState.COMPLETED, EventState.FAILED]:
                    age = current_time - event['updated_at']
                    if age > max_age_seconds:
                        to_remove.append(event_id)
            
            for event_id in to_remove:
                del self._events[event_id]
            
            if to_remove:
                self.logger.info(f"Cleaned up {len(to_remove)} old events")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get state manager statistics"""
        with self._lock:
            return self._stats.copy()