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
        Process a CDC event with enhanced traceability and standardized envelope
        Future-Proofing: Wraps all outgoing data in versioned envelope
        """
        start_time = time.time()
        
        try:
            # Extract event details
            payload = raw_event.get('payload', {})
            source = payload.get('source', {})
            
            # Enhanced Traceability: Use trace_id from ChangeDetector if available
            trace_id = payload.get('trace_id')
            if not trace_id:
                # Fallback: Generate new trace_id if not provided by ChangeDetector
                trace_id = str(uuid.uuid4())
                self.logger.warning(
                    f"No trace_id from ChangeDetector, generated fallback: {trace_id}"
                )
            
            # Use trace_id as event_id for consistency
            event_id = trace_id
            
            # Enhanced Traceability: Set trace context for all subsequent logs
            self.logger.set_trace_context(trace_id)
            
            # Log capture with trace_id
            self.logger.log_event_lifecycle(
                trace_id,
                'CAPTURED',
                data={
                    'table': source.get('table'),
                    'timestamp': source.get('ts_ms'),
                    'detected_at': payload.get('detected_at'),
                    'detector_instance': payload.get('detector_instance')
                }
            )
            
            # Extract operation
            operation = payload.get('op')
            
            if not operation:
                self.logger.warning("Event has no operation, skipping", trace_id=trace_id)
                self.logger.clear_trace_context()
                return None
            
            # Get table name
            table = source.get('table')
            if not table:
                self.logger.warning("Event has no table, skipping", trace_id=trace_id)
                self.logger.clear_trace_context()
                return None
            
            # Extract data
            data = payload.get('after', {})
            
            if not data:
                self.logger.warning("Event has no data, skipping", trace_id=trace_id)
                self.logger.clear_trace_context()
                return None
            
            # Log processing with trace_id
            self.logger.log_event_lifecycle(
                trace_id,
                'PROCESSING',
                data={
                    'operation': operation,
                    'table': table,
                    'record_count': 1
                }
            )
            
            # Future-Proofing: Create standardized envelope with version field
            envelope_version = "1.0"  # Current envelope version
            
            # Transform event with standardized envelope structure
            transformed_event = {
                # Core identity fields
                'trace_id': trace_id,
                'event_id': event_id,
                'envelope_version': envelope_version,  # Future-Proofing: Version field
                
                # Event content
                'operation': self._map_operation(operation),
                'table': table,
                'data': data,
                
                # Source metadata
                'source': {
                    'database': source.get('db'),
                    'schema': source.get('schema'),
                    'lsn': source.get('lsn'),
                    'timestamp': source.get('ts_ms')
                },
                
                # Processing timestamps
                'capture_timestamp': datetime.utcnow().isoformat(),
                'processed_timestamp': datetime.utcnow().isoformat(),
                
                # Enhanced Traceability: Include detection metadata
                'detection_metadata': {
                    'detected_at': payload.get('detected_at'),
                    'detector_instance': payload.get('detector_instance'),
                    'processor_instance': 'event_processor_001'
                },
                
                # Future-Proofing: Envelope metadata for version compatibility
                'envelope_metadata': {
                    'version': envelope_version,
                    'schema_version': '1.0',
                    'format': 'json',
                    'created_by': 'event_processor',
                    'created_at': datetime.utcnow().isoformat()
                }
            }
            
            # Encrypt sensitive fields
            if self.sensitive_fields:
                transformed_event['data'] = self.crypto.encrypt_sensitive_fields(
                    transformed_event['data'],
                    self.sensitive_fields
                )
            
            # Future-Proofing: Create signed message with versioned envelope
            signed_event = self.crypto.create_signed_message(transformed_event)
            
            # Add envelope version to signed message for subscriber parsing
            signed_event['envelope_version'] = envelope_version
            
            # Record metrics
            duration = time.time() - start_time
            self.metrics.record_latency('processing', duration)
            self.metrics.increment_counter('events_processed')
            
            # Log completion with trace_id and envelope version
            self.logger.log_event_lifecycle(
                trace_id,
                'PROCESSED',
                data={
                    'processing_time_ms': duration * 1000,
                    'encrypted_fields': len(self.sensitive_fields),
                    'trace_id': trace_id,
                    'envelope_version': envelope_version
                }
            )
            
            # Clear trace context
            self.logger.clear_trace_context()
            
            return signed_event
            
        except Exception as e:
            trace_id = trace_id if 'trace_id' in locals() else 'unknown'
            self.logger.error(
                f"Error processing event: {e}",
                trace_id=trace_id,
                exc_info=True
            )
            self.metrics.increment_counter('events_failed')
            self.logger.clear_trace_context()
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