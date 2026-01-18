import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum


class DLQMessage(Enum):
    PUBLISH_FAILURE = "PUBLISH_FAILURE"
    PROCESSING_FAILURE = "PROCESSING_FAILURE"
    DATABASE_FAILURE = "DATABASE_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"


class DeadLetterQueue:
    """
    Dead Letter Queue for failed messages
    Implements professional error handling and retry logic
    """
    
    def __init__(
        self,
        storage_dir: str = "data/dlq",
        max_retries: int = 5,
        retention_days: int = 7,
        logger = None
    ):
        """Initialize DLQ"""
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_retries = max_retries
        self.retention_days = retention_days
        self.logger = logger
    
    def add_message(
        self,
        event_id: str,
        message_type: DLQMessage,
        payload: Dict[str, Any],
        error: str,
        retry_count: int = 0
    ):
        """Add failed message to DLQ"""
        try:
            dlq_entry = {
                'event_id': event_id,
                'message_type': message_type.value,
                'payload': payload,
                'error': error,
                'retry_count': retry_count,
                'timestamp': datetime.utcnow().isoformat(),
                'next_retry': self._calculate_next_retry(retry_count)
            }
            
            # Save to file
            event_file = self.storage_dir / f"{event_id}_{int(time.time())}.json"
            event_file.write_text(json.dumps(dlq_entry, indent=2))
            
            self.logger.warning(
                f"Message added to DLQ: {event_id}",
                event_id=event_id,
                message_type=message_type.value,
                error=error
            )
            
        except Exception as e:
            self.logger.error(f"Failed to add message to DLQ: {e}", exc_info=True)
    
    def get_pending_messages(self) -> List[Dict[str, Any]]:
        """Get messages ready for retry"""
        pending = []
        current_time = datetime.utcnow()
        
        for dlq_file in self.storage_dir.glob("*.json"):
            try:
                message = json.loads(dlq_file.read_text())
                next_retry = datetime.fromisoformat(message['next_retry'])
                
                if (message['retry_count'] < self.max_retries and 
                    next_retry <= current_time):
                    pending.append(message)
                    
            except Exception as e:
                self.logger.warning(f"Error reading DLQ file: {e}")
        
        return pending
    
    def mark_resolved(self, event_id: str):
        """Mark message as resolved"""
        for dlq_file in self.storage_dir.glob(f"{event_id}_*.json"):
            dlq_file.unlink()
            self.logger.info(f"Resolved DLQ message: {event_id}")
    
    def cleanup_old_messages(self):
        """Remove old DLQ messages"""
        cutoff_date = datetime.utcnow() - timedelta(days=self.retention_days)
        
        for dlq_file in self.storage_dir.glob("*.json"):
            try:
                message = json.loads(dlq_file.read_text())
                timestamp = datetime.fromisoformat(message['timestamp'])
                
                if timestamp < cutoff_date:
                    dlq_file.unlink()
                    
            except Exception as e:
                self.logger.warning(f"Error cleaning DLQ: {e}")
    
    def _calculate_next_retry(self, retry_count: int) -> str:
        """Calculate next retry time with exponential backoff"""
        # Exponential backoff: 5s, 25s, 125s, 625s, 3125s (52 min)
        delay = 5 * (5 ** retry_count)
        next_retry = datetime.utcnow() + timedelta(seconds=delay)
        return next_retry.isoformat()
