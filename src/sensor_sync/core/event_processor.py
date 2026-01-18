"""
Event processor with transformation, encryption, and MQTT publishing
"""

import uuid
import time
from datetime import datetime
from typing import Dict, Any, Optional
from ..utils.crypto import CryptoManager
from ..utils.logger import StructuredLogger


class EventProcessor:
    """
    Processes CDC events: transform, encrypt, validate
    """
    
    def __init__(
        self,
        crypto_manager: CryptoManager,
        sensitive_fields: list,
        logger: StructuredLogger,
        metrics
    ):
        """
        Initialize event processor
        
        Args:
            crypto_manager: Crypto manager for encryption/signing
            sensitive_fields: List of fields to encrypt
            logger: Logger instance
            metrics: Metrics collector
        """
        self.crypto = crypto_manager
        self.sensitive_fields = sensitive_fields
        self.logger = logger
        self.metrics = metrics
    
    def process_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a CDC event
        
        Args:
            raw_event: Raw event from Debezium
            
        Returns:
            Processed event ready for MQTT publishing, or None if should be skipped
        """
        start_time = time.time()
        
        try:
            # Extract event details
            payload = raw_event.get('payload', {})
            source = payload.get('source', {})
            
            # Generate event ID
            event_id = str(uuid.uuid4())
            
            # Log capture
            self.logger.log_event_lifecycle(
                event_id,
                'CAPTURED',
                data={
                    'table': source.get('table'),
                    'lsn': source.get('lsn'),
                    'timestamp': source.get('ts_ms')
                }
            )
            
            # Extract operation
            operation = payload.get('op')  # c=create, u=update, d=delete, r=read
            
            if not operation:
                self.logger.warning("Event has no operation, skipping", event_id=event_id)
                return None
            
            # Get table name
            table = source.get('table')
            if not table:
                self.logger.warning("Event has no table, skipping", event_id=event_id)
                return None
            
            # Extract data based on operation
            if operation in ['c', 'r', 'u']:  # Create, Read, Update
                data = payload.get('after', {})
            elif operation == 'd':  # Delete
                data = payload.get('before', {})
            else:
                self.logger.warning(f"Unknown operation: {operation}", event_id=event_id)
                return None
            
            if not data:
                self.logger.warning("Event has no data, skipping", event_id=event_id)
                return None
            
            # Log processing
            self.logger.log_event_lifecycle(
                event_id,
                'PROCESSING',
                data={
                    'operation': operation,
                    'table': table,
                    'record_count': 1
                }
            )
            
            # Transform event
            transformed_event = {
                'event_id': event_id,
                'operation': self._map_operation(operation),
                'table': table,
                'data': data,
                'source': {
                    'database': source.get('db'),
                    'schema': source.get('schema'),
                    'lsn': source.get('lsn'),
                    'timestamp': source.get('ts_ms')
                },
                'capture_timestamp': datetime.utcnow().isoformat(),
                'processed_timestamp': datetime.utcnow().isoformat()
            }
            
            # Encrypt sensitive fields
            if self.sensitive_fields:
                transformed_event['data'] = self.crypto.encrypt_sensitive_fields(
                    transformed_event['data'],
                    self.sensitive_fields
                )
            
            # Create signed message
            signed_event = self.crypto.create_signed_message(transformed_event)
            
            # Record metrics
            duration = time.time() - start_time
            self.metrics.record_latency('processing', duration)
            self.metrics.increment_counter('events_processed')
            
            # Log completion
            self.logger.log_event_lifecycle(
                event_id,
                'PROCESSED',
                data={
                    'processing_time_ms': duration * 1000,
                    'encrypted_fields': len(self.sensitive_fields)
                }
            )
            
            return signed_event
            
        except Exception as e:
            self.logger.error(
                f"Error processing event: {e}",
                event_id=event_id if 'event_id' in locals() else 'unknown',
                exc_info=True
            )
            self.metrics.increment_counter('events_failed')
            return None
    
    def _map_operation(self, op: str) -> str:
        """Map Debezium operation codes to readable names"""
        operation_map = {
            'c': 'INSERT',
            'r': 'READ',
            'u': 'UPDATE',
            'd': 'DELETE'
        }
        return operation_map.get(op, op)