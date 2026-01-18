import logging
import json
from pathlib import Path
from datetime import datetime
import structlog
from pythonjsonlogger import jsonlogger


class StructuredLogger:
    """Structured logging with JSON output"""
    
    def __init__(self, name: str, log_dir: str = "logs", level: str = "INFO"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure structlog
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
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        
        # Setup Python logging
        self.logger = structlog.get_logger(name)
        
        # File handler for JSON logs
        log_file = self.log_dir / f"{name}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(jsonlogger.JsonFormatter())
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        
        # Add handlers
        python_logger = logging.getLogger(name)
        python_logger.setLevel(getattr(logging, level))
        python_logger.addHandler(file_handler)
        python_logger.addHandler(console_handler)
    
    def info(self, msg: str, **kwargs):
        self.logger.info(msg, **kwargs)
    
    def error(self, msg: str, exc_info: bool = False, **kwargs):
        self.logger.error(msg, exc_info=exc_info, **kwargs)
    
    def warning(self, msg: str, **kwargs):
        self.logger.warning(msg, **kwargs)
    
    def debug(self, msg: str, **kwargs):
        self.logger.debug(msg, **kwargs)
    
    def log_event_lifecycle(self, event_id: str, stage: str, data: dict = None, error: str = None):
        """Log event lifecycle events"""
        self.logger.info(
            "event_lifecycle",
            event_id=event_id,
            stage=stage,
            data=data or {},
            error=error,
            timestamp=datetime.utcnow().isoformat()
        )
