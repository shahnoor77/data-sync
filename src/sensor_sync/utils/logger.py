"""
Level 4: End-to-End Observability (Audit Trail)
Enhanced structured logging with JSON standard, tracing, and latency monitoring
"""

import logging
import json
import os
import time
from pathlib import Path
from datetime import datetime
import structlog
from pythonjsonlogger import jsonlogger
from typing import Dict, Any, Optional
from enum import Enum


class SystemFailure(Enum):
    """Level 4: Standardized SystemFailure enum for DLQ error mapping"""
    DATABASE_CONNECTION_FAILED = "DATABASE_CONNECTION_FAILED"
    DATABASE_TRANSACTION_FAILED = "DATABASE_TRANSACTION_FAILED"
    NETWORK_TIMEOUT = "NETWORK_TIMEOUT"
    NETWORK_CONNECTION_LOST = "NETWORK_CONNECTION_LOST"
    CRYPTO_SIGNATURE_INVALID = "CRYPTO_SIGNATURE_INVALID"
    CRYPTO_DECRYPTION_FAILED = "CRYPTO_DECRYPTION_FAILED"
    MQTT_PUBLISH_FAILED = "MQTT_PUBLISH_FAILED"
    MQTT_SUBSCRIBE_FAILED = "MQTT_SUBSCRIBE_FAILED"
    JSON_PARSE_ERROR = "JSON_PARSE_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class StructuredLogger:
    """
    Level 4: Enhanced Structured Logger with End-to-End Observability
    - JSON Standard: Every log entry is valid JSON
    - Tracing: Every log includes trace_id and instance_id
    - Latency Monitoring: End-to-End latency calculation
    - Error Mapping: Standardized SystemFailure enum
    """
    
    def __init__(self, name: str, log_dir: str = "logs", level: str = "INFO"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Level 4: Instance ID for tracing
        self.instance_id = os.getenv('INSTANCE_ID', 'unknown_instance')
        
        # Configure structlog for Level 4 JSON standard
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                self._add_level4_context,  # Level 4: Add trace_id and instance_id
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        
        # Setup Python logging
        self.logger = structlog.get_logger(name)
        
        # Level 4: JSON-only file handler
        log_file = self.log_dir / f"{name}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s'
        ))
        
        # Console handler (human-readable for development)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        
        # Add handlers
        python_logger = logging.getLogger(name)
        python_logger.setLevel(getattr(logging, level))
        python_logger.addHandler(file_handler)
        python_logger.addHandler(console_handler)
    
    def _add_level4_context(self, logger, method_name, event_dict):
        """
        Level 4: Add trace_id and instance_id to every log entry
        Ensures JSON standard compliance
        """
        # Level 4: Always include instance_id
        event_dict['instance_id'] = self.instance_id
        
        # Level 4: Include trace_id if available in context
        if 'trace_id' not in event_dict:
            event_dict['trace_id'] = getattr(self, '_current_trace_id', None)
        
        # Level 4: Ensure timestamp is always present
        if 'timestamp' not in event_dict:
            event_dict['timestamp'] = datetime.utcnow().isoformat()
        
        return event_dict
    
    def set_trace_context(self, trace_id: str):
        """Level 4: Set trace_id for current logging context"""
        self._current_trace_id = trace_id
    
    def clear_trace_context(self):
        """Level 4: Clear trace_id from logging context"""
        self._current_trace_id = None
    
    def info(self, msg: str, trace_id: str = None, **kwargs):
        """Level 4: Info log with mandatory JSON structure"""
        if trace_id:
            kwargs['trace_id'] = trace_id
        self.logger.info(msg, **kwargs)
    
    def error(self, msg: str, trace_id: str = None, system_failure: SystemFailure = None, 
              exc_info: bool = False, **kwargs):
        """Level 4: Error log with SystemFailure enum mapping"""
        if trace_id:
            kwargs['trace_id'] = trace_id
        if system_failure:
            kwargs['system_failure'] = system_failure.value
        self.logger.error(msg, exc_info=exc_info, **kwargs)
    
    def warning(self, msg: str, trace_id: str = None, **kwargs):
        """Level 4: Warning log with tracing"""
        if trace_id:
            kwargs['trace_id'] = trace_id
        self.logger.warning(msg, **kwargs)
    
    def debug(self, msg: str, trace_id: str = None, **kwargs):
        """Level 4: Debug log with tracing"""
        if trace_id:
            kwargs['trace_id'] = trace_id
        self.logger.debug(msg, **kwargs)
    
    def log_database_commit(self, trace_id: str, sensor_origin_time: float, 
                           record_count: int = 1, table: str = "sensor_readings"):
        """
        Level 4: Database commit log with End-to-End Latency calculation
        Latency = T_commit - T_sensor_origin
        """
        commit_time = time.time()
        end_to_end_latency = commit_time - sensor_origin_time
        
        self.logger.info(
            "database_commit_success",
            trace_id=trace_id,
            table=table,
            record_count=record_count,
            sensor_origin_time=sensor_origin_time,
            commit_time=commit_time,
            end_to_end_latency_seconds=end_to_end_latency,
            end_to_end_latency_ms=end_to_end_latency * 1000,
            instance_id=self.instance_id
        )
    
    def log_event_lifecycle(self, trace_id: str, stage: str, data: dict = None, 
                           error: str = None, system_failure: SystemFailure = None):
        """Level 4: Enhanced event lifecycle logging with SystemFailure mapping"""
        log_data = {
            'trace_id': trace_id,
            'stage': stage,
            'data': data or {},
            'instance_id': self.instance_id,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if error:
            log_data['error'] = error
        if system_failure:
            log_data['system_failure'] = system_failure.value
        
        if error or system_failure:
            self.logger.error("event_lifecycle_error", **log_data)
        else:
            self.logger.info("event_lifecycle", **log_data)
    
    def log_quantum_processing(self, quantum_size: int, valid_count: int, 
                              invalid_count: int, processing_time_ms: float):
        """Level 4: Quantum batch processing metrics"""
        self.logger.info(
            "quantum_processing_complete",
            quantum_size=quantum_size,
            valid_events=valid_count,
            invalid_events=invalid_count,
            processing_time_ms=processing_time_ms,
            throughput_events_per_second=quantum_size / (processing_time_ms / 1000) if processing_time_ms > 0 else 0,
            instance_id=self.instance_id
        )
    
    def log_system_health(self, component: str, status: str, metrics: Dict[str, Any] = None):
        """Level 4: System health monitoring"""
        self.logger.info(
            "system_health_check",
            component=component,
            status=status,
            metrics=metrics or {},
            instance_id=self.instance_id,
            timestamp=datetime.utcnow().isoformat()
        )
    
    def map_exception_to_system_failure(self, exception: Exception) -> SystemFailure:
        """
        Level 4: Map exceptions to standardized SystemFailure enum
        Used for consistent DLQ error categorization
        """
        exception_name = type(exception).__name__
        exception_message = str(exception).lower()
        
        # Database errors
        if 'mysql' in exception_name.lower() or 'database' in exception_message:
            if 'connection' in exception_message:
                return SystemFailure.DATABASE_CONNECTION_FAILED
            else:
                return SystemFailure.DATABASE_TRANSACTION_FAILED
        
        # Network errors
        elif 'timeout' in exception_message or 'timeout' in exception_name.lower():
            return SystemFailure.NETWORK_TIMEOUT
        elif 'connection' in exception_message and 'mqtt' not in exception_message:
            return SystemFailure.NETWORK_CONNECTION_LOST
        
        # Crypto errors
        elif 'signature' in exception_message or 'verify' in exception_message:
            return SystemFailure.CRYPTO_SIGNATURE_INVALID
        elif 'decrypt' in exception_message or 'crypto' in exception_message:
            return SystemFailure.CRYPTO_DECRYPTION_FAILED
        
        # MQTT errors
        elif 'mqtt' in exception_message or 'publish' in exception_message:
            return SystemFailure.MQTT_PUBLISH_FAILED
        elif 'subscribe' in exception_message:
            return SystemFailure.MQTT_SUBSCRIBE_FAILED
        
        # JSON errors
        elif 'json' in exception_name.lower():
            return SystemFailure.JSON_PARSE_ERROR
        
        # Validation errors
        elif 'validation' in exception_message or 'invalid' in exception_message:
            return SystemFailure.VALIDATION_ERROR
        
        # Resource errors
        elif 'memory' in exception_message or 'resource' in exception_message:
            return SystemFailure.RESOURCE_EXHAUSTED
        
        # Default
        else:
            return SystemFailure.UNKNOWN_ERROR
