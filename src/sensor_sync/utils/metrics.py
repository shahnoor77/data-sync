"""Metrics collection and monitoring"""

import time
from typing import Dict, Any
from collections import defaultdict
from datetime import datetime
import threading
from prometheus_client import Counter, Histogram, Gauge, start_http_server


class MetricsCollector:
    """
    Collects and exposes metrics for monitoring
    """
    
    def __init__(self, enable_prometheus: bool = True, port: int = 9090):
        """
        Initialize metrics collector
        
        Args:
            enable_prometheus: Enable Prometheus metrics endpoint
            port: Port for Prometheus metrics server
        """
        self.enable_prometheus = enable_prometheus
        self._lock = threading.Lock()
        
        # Internal metrics storage
        self._counters = defaultdict(int)
        self._histograms = defaultdict(list)
        self._gauges = defaultdict(float)
        
        # Prometheus metrics
        if enable_prometheus:
            self._setup_prometheus_metrics()
            try:
                start_http_server(port)
            except Exception:
                pass  # Port might already be in use
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics"""
        # Counters
        self.events_captured = Counter(
            'cdc_events_captured_total',
            'Total number of CDC events captured'
        )
        self.events_processed = Counter(
            'cdc_events_processed_total',
            'Total number of events processed successfully'
        )
        self.events_failed = Counter(
            'cdc_events_failed_total',
            'Total number of events that failed processing'
        )
        self.mqtt_messages_published = Counter(
            'mqtt_messages_published_total',
            'Total number of MQTT messages published'
        )
        self.mqtt_acks_received = Counter(
            'mqtt_acks_received_total',
            'Total number of MQTT acknowledgments received'
        )
        
        # Histograms
        self.processing_latency = Histogram(
            'cdc_processing_latency_seconds',
            'Time taken to process events',
            buckets=[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
        )
        self.end_to_end_latency = Histogram(
            'cdc_end_to_end_latency_seconds',
            'Total time from capture to acknowledgment',
            buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
        )
        
        # Gauges
        self.queue_size = Gauge(
            'cdc_queue_size',
            'Current size of event processing queue'
        )
        self.pending_acks = Gauge(
            'cdc_pending_acks',
            'Number of events waiting for acknowledgment'
        )
    
    def increment_counter(self, name: str, value: int = 1):
        """Increment a counter metric"""
        with self._lock:
            self._counters[name] += value
        
        # Update Prometheus if enabled
        if self.enable_prometheus:
            if name == 'events_captured':
                self.events_captured.inc(value)
            elif name == 'events_processed':
                self.events_processed.inc(value)
            elif name == 'events_failed':
                self.events_failed.inc(value)
            elif name == 'mqtt_published':
                self.mqtt_messages_published.inc(value)
            elif name == 'mqtt_acks':
                self.mqtt_acks_received.inc(value)
    
    def record_latency(self, name: str, duration_seconds: float):
        """Record a latency measurement"""
        with self._lock:
            self._histograms[name].append(duration_seconds)
        
        # Update Prometheus if enabled
        if self.enable_prometheus:
            if name == 'processing':
                self.processing_latency.observe(duration_seconds)
            elif name == 'end_to_end':
                self.end_to_end_latency.observe(duration_seconds)
    
    def set_gauge(self, name: str, value: float):
        """Set a gauge metric"""
        with self._lock:
            self._gauges[name] = value
        
        # Update Prometheus if enabled
        if self.enable_prometheus:
            if name == 'queue_size':
                self.queue_size.set(value)
            elif name == 'pending_acks':
                self.pending_acks.set(value)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        with self._lock:
            summary = {
                'timestamp': datetime.utcnow().isoformat(),
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {}
            }
            
            # Calculate histogram statistics
            for name, values in self._histograms.items():
                if values:
                    summary['histograms'][name] = {
                        'count': len(values),
                        'min': min(values),
                        'max': max(values),
                        'avg': sum(values) / len(values)
                    }
            
            return summary
    
    def reset_metrics(self):
        """Reset all metrics (useful for testing)"""
        with self._lock:
            self._counters.clear()
            self._histograms.clear()
            self._gauges.clear()


class Timer:
    """Context manager for timing operations"""
    
    def __init__(self, metrics: MetricsCollector, metric_name: str):
        self.metrics = metrics
        self.metric_name = metric_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.metrics.record_latency(self.metric_name, duration)