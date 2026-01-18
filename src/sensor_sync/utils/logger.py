
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import threading


class StructuredLogger:
    """
    Structured logger for tracking event lifecycle
    Logs in both human-readable and JSON formats
    """
    
    def __init__(self, name: str, log_dir: str = "logs", level: str = "INFO"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers.clear()
        
        # File handler - Structured JSON logs
        json_handler = logging.FileHandler(
            self.log_dir / f"{name}_structured.jsonl"
        )
        json_handler.setFormatter(self.JSONFormatter())
        self.logger.addHandler(json_handler)
        
        # File handler - Human readable logs
        text_handler = logging.FileHandler(
            self.log_dir / f"{name}.log"
        )
        text_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        text_handler.setFormatter(text_formatter)
        self.logger.addHandler(text_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.ColoredFormatter())
        self.logger.addHandler(console_handler)
        
        # Thread-safe event tracking
        self._event_states = {}
        self._lock = threading.Lock()
    
    class JSONFormatter(logging.Formatter):
        """Format logs as JSON"""
        def format(self, record):
            log_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'thread': record.thread,
                'thread_name': record.threadName,
            }
            
            # Add extra fields
            if hasattr(record, 'extra'):
                log_data.update(record.extra)
            
            return json.dumps(log_data)
    
    class ColoredFormatter(logging.Formatter):
        """Add colors to console output"""
        COLORS = {
            'DEBUG': '\033[36m',     # Cyan
            'INFO': '\033[32m',      # Green
            'WARNING': '\033[33m',   # Yellow
            'ERROR': '\033[31m',     # Red
            'CRITICAL': '\033[35m',  # Magenta
        }
        RESET = '\033[0m'
        
        def format(self, record):
            color = self.COLORS.get(record.levelname, self.RESET)
            record.levelname = f"{color}{record.levelname}{self.RESET}"
            return super().format(record)
    
    def log_event_lifecycle(
        self,
        event_id: str,
        state: str,
        data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ):
        """
        Log event lifecycle state changes
        
        States: CAPTURED, PROCESSING, PUBLISHED, MQTT_CONFIRMED, 
                DELIVERED, WRITTEN, ACKNOWLEDGED, COMPLETED, FAILED
        """
        with self._lock:
            if event_id not in self._event_states:
                self._event_states[event_id] = {
                    'event_id': event_id,
                    'states': [],
                    'created_at': datetime.utcnow().isoformat()
                }
            
            state_entry = {
                'state': state,
                'timestamp': datetime.utcnow().isoformat(),
                'data': data or {},
                'error': error
            }
            
            self._event_states[event_id]['states'].append(state_entry)
            
            # Log the state change
            log_data = {
                'event_id': event_id,
                'state': state,
                'data': data or {},
            }
            
            if error:
                log_data['error'] = error
                self.logger.error(
                    f"Event {event_id} - {state}",
                    extra={'extra': log_data}
                )
            else:
                self.logger.info(
                    f"Event {event_id} - {state}",
                    extra={'extra': log_data}
                )
    
    def get_event_history(self, event_id: str) -> Optional[Dict]:
        """Get full lifecycle history for an event"""
        with self._lock:
            return self._event_states.get(event_id)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(message, extra={'extra': kwargs})
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(message, extra={'extra': kwargs})
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(message, extra={'extra': kwargs})
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(message, extra={'extra': kwargs})
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(message, extra={'extra': kwargs})
