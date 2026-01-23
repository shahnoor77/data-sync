"""
EMQX Publisher for CDC events

"""

import json
import time
import threading
from typing import Dict, Any, List
import paho.mqtt.client as mqtt
from ..core.state_manager import StateManager, EventState
from ..core.dead_letter_queue import DeadLetterQueue


class EMQXPublisher:
    """
    Production-grade MQTT publisher with optimized batching
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        state_manager: StateManager,
        dlq: DeadLetterQueue,
        logger,
        metrics
    ):
        """Initialize EMQX publisher with enhanced tracking buffer"""
        self.config = config
        self.state_manager = state_manager
        self.dlq = dlq
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        self._lock = threading.Lock()
        
        # Ack-Window Expansion: Extended tracking buffer for high-throughput
        self._pending_publishes = {}  # message_id -> event_ids mapping
        self._inflight_tracking = {}  # event_id -> timestamp mapping for TTL
        self.max_inflight_ttl = 180  # Buffer Expansion: 180 seconds TTL (was 120)
        self.max_tracking_size = 50000  # Context Retention: 50K concurrent IDs (was 10K)
        
        # Instance ID for topic isolation
        self.instance_id = config.get('instance_id', 'publisher_001')
        
        # Batch publishing
        self.batch_enabled = self.config.get('batch_publish_enabled', True)
        self.batch_interval = self.config.get('batch_publish_interval_ms', 500) / 1000.0
        self.batch_size = self.config.get('batch_publish_size', 500)
        self.use_bulk_topic = self.config.get('batch_use_bulk_topic', True)
        self.event_batch = []
        self.last_publish_time = time.time()
        self.messages_processed = 0
        
        # Setup client
        self._setup_client()
    
    def _setup_client(self):
        """Setup EMQX MQTT client with TLS support for cloud serverless instance"""
        import os
        
        # Collision-Aware: Base client_id with revision counter for ID conflicts
        base_client_id = self.config.get('client_id_publisher', f"cloud_publisher_01")
        self.client_id_revision = 0
        self.client_id = f"{base_client_id}_rev{self.client_id_revision}"
        
        # Session Wipe: Track clean_session state for ID rejection recovery
        self.clean_session_override = False  # Temporary override for session wipe
        self.normal_clean_session = False    # Normal persistent session mode
        
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=self.normal_clean_session  # Start with persistent sessions
        )
        
        # TLS Configuration for EMQX Cloud
        ca_cert_path = os.getenv('CA_CERT_PATH', '')
        if os.path.exists(ca_cert_path):
            self.client.tls_set(ca_certs=ca_cert_path)
            self.logger.info(f"TLS enabled with CA certificate: {ca_cert_path}")
        else:
            self.logger.warning(f"CA certificate not found at {ca_cert_path}, proceeding without TLS")
        
        # Set max in-flight BEFORE connect
        self.client.max_inflight_messages_set(
            self.config.get('max_inflight_messages', 2000)
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.on_message = self._on_ack_message
        
        # Set authentication
        if self.config.get('username') and self.config.get('password'):
            self.client.username_pw_set(
                self.config['username'],
                self.config['password']
            )
        
        # Reconnect behavior
        self.client.reconnect_delay_set(
            min_delay=self.config.get('reconnect_min_interval', 1),
            max_delay=self.config.get('reconnect_max_interval', 30)
        )
        
        self.logger.info(
            f"EMQX Cloud publisher configured with TLS - client_id: {self.client_id}, "
            f"max_inflight: {self.config.get('max_inflight_messages', 2000)}, clean_session: {self.normal_clean_session}"
        )
    
    def connect(self):
        """Connect to EMQX broker using loop_start() for lifecycle management"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                broker_host = self.config['broker_host']
                broker_port = int(self.config['broker_port'])
                keepalive = int(self.config.get('keepalive', 60))  # Increase Keep-Alive: 60 seconds
                
                # Apply Session Wipe if needed for ID rejection recovery
                current_clean_session = self.clean_session_override or self.normal_clean_session
                self.client.clean_session = current_clean_session
                
                self.logger.info(
                    f"Connecting to EMQX: {broker_host}:{broker_port} "
                    f"(attempt {retry_count + 1}/{max_retries}, client_id: {self.client_id}, "
                    f"clean_session: {current_clean_session})"
                )
                
                # Use connect_async() + loop_start() instead of blocking connect()
                self.client.connect_async(
                    broker_host,
                    broker_port,
                    keepalive
                )
                
                # Start network loop in background thread
                self.client.loop_start()
                
                # Wait for connection with timeout
                timeout = 10
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    raise ConnectionError(f"Failed to connect to EMQX with client_id: {self.client_id}")
                
                # Reset Session Wipe override after successful connection
                if self.clean_session_override:
                    self.clean_session_override = False
                    self.logger.info("Session Wipe: Successfully connected, reverting to persistent sessions")
                
                self.logger.info(f"✓ Connected to EMQX with collision-aware client_id: {self.client_id}")
                return
                
            except Exception as e:
                retry_count += 1
                self.logger.warning(f"Connection attempt {retry_count} failed: {e}")
                
                # Stop loop if it was started
                if hasattr(self.client, '_thread') and self.client._thread:
                    self.client.loop_stop()
                
                if retry_count < max_retries:
                    # Give EMQX time to release the socket
                    time.sleep(1)
                    backoff = min(2 ** retry_count, 30)
                    time.sleep(backoff)
                else:
                    raise
    
    def disconnect(self):
        """Gracefully disconnect"""
        if self.event_batch:
            self._flush_batch()
        
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        
        self.logger.info(f"Publisher stopped. Total messages: {self.messages_processed}")
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback for connection with Topic Specificity"""
        if rc == 0:
            self.connected = True
            self.logger.info("✓ EMQX publisher connected")
            
            # Topic Specificity: Narrow ACK subscription to reduce cross-process noise
            ack_topic = f"sensors/ack/{self.instance_id}"
            client.subscribe(ack_topic, qos=self.config.get('qos_subscribe', 2))
            self.logger.info(f"✓ Topic Specificity: Subscribed to {ack_topic} (reduced noise)")
            
            self.metrics.set_gauge('mqtt_connected', 1)
        else:
            self.logger.error(f"Connection failed: {rc}")
            self.connected = False
            self.metrics.set_gauge('mqtt_connected', 0)
    
    def _on_disconnect(self, client, userdata, rc, properties=None):
        """
        Callback for disconnect with Session Wipe for ID rejection recovery
        RC 2 = ID Rejected, trigger Session Wipe and retry
        """
        self.connected = False
        self.metrics.set_gauge('mqtt_connected', 0)
        
        if rc == 2:  # ID Rejected - Collision detected
            self.client_id_revision += 1
            base_client_id = self.config.get('client_id_publisher', f"prod_publisher_01")
            new_client_id = f"{base_client_id}_rev{self.client_id_revision}"
            
            self.logger.warning(
                f"Session Wipe: ID Rejected (RC=2), triggering forceful reset: "
                f"{self.client_id} -> {new_client_id}"
            )
            
            # Update client_id and recreate client
            self.client_id = new_client_id
            self.client._client_id = new_client_id.encode('utf-8')
            
            # Session Wipe: Set clean_session=True for next attempt only
            self.clean_session_override = True
            self.logger.info("Session Wipe: Will use clean_session=True for next connection attempt")
            
        elif rc != 0:
            self.logger.warning(f"Unexpected disconnect: RC={rc}")
        else:
            self.logger.info("Clean disconnect")
    
    def _on_publish(self, client, userdata, mid, reason_codes=None, properties=None):
        """Callback for publish confirmation with enhanced tracking"""
        try:
            event_ids = self._pending_publishes.get(mid)
            current_time = time.time()
            
            if event_ids:
                if isinstance(event_ids, list):
                    for event_id in event_ids:
                        self.state_manager.update_state(
                            event_id,
                            EventState.MQTT_CONFIRMED
                        )
                        # Enhanced Tracking: Add to inflight tracking with timestamp
                        self._inflight_tracking[event_id] = current_time
                    self.metrics.increment_counter('mqtt_published', len(event_ids))
                else:
                    self.state_manager.update_state(
                        event_ids,
                        EventState.MQTT_CONFIRMED
                    )
                    # Enhanced Tracking: Add to inflight tracking with timestamp
                    self._inflight_tracking[event_ids] = current_time
                    self.metrics.increment_counter('mqtt_published')
                
                del self._pending_publishes[mid]
            
            # Enhanced Tracking: Cleanup expired inflight tracking
            self._cleanup_expired_tracking()
            
        except Exception as e:
            self.logger.error(f"Error in publish callback: {e}", exc_info=True)
    
    def _on_ack_message(self, client, userdata, msg):
        """Handle ACK messages from subscriber with enhanced tracking validation"""
        try:
            payload = msg.payload.decode('utf-8')
            ack_data = json.loads(payload)
            
            event_ids = ack_data.get('event_ids')  # Can be list
            success = ack_data.get('success', False)
            error = ack_data.get('error')
            
            if not event_ids:
                return
            
            # Handle both single and batch ACKs
            if isinstance(event_ids, list):
                for event_id in event_ids:
                    self._process_ack_for_event(event_id, success, error)
            else:
                self._process_ack_for_event(event_ids, success, error)
                
        except Exception as e:
            self.logger.error(f"Error processing ACK: {e}", exc_info=True)
    
    def _process_ack_for_event(self, event_id: str, success: bool, error: str = None):
        """Process ACK for individual event with Ack-Window Expansion validation"""
        # Ack-Window Expansion: Check if event is in our expanded tracking buffer
        if event_id not in self._inflight_tracking:
            # Context Retention: With 180s TTL and 50K buffer, this should be rare
            self.logger.debug(f"Ack-Window: ACK received for event not in expanded buffer: {event_id}")
            return
        
        # Filter Test Events: Check if event_id starts with 'evt_' or 'stress_'
        if event_id.startswith('evt_') or event_id.startswith('stress_'):
            self.logger.debug(f"Test ACK received for {event_id}")
            # Remove from tracking buffer
            self._inflight_tracking.pop(event_id, None)
            return
        
        if success:
            self.state_manager.update_state(event_id, EventState.ACKNOWLEDGED)
            self.metrics.increment_counter('mqtt_acks')
        else:
            self.state_manager.update_state(event_id, EventState.FAILED, error=error)
            try:
                self.dlq.add_message(
                    event_id=event_id,
                    message_type='ACK_FAILURE',
                    payload={'error': error},
                    error=f"NACK: {error}"
                )
            except Exception as dlq_error:
                self.logger.error(f"DLQ operation failed: {dlq_error}")
        
        # Remove from tracking buffer after processing
        self._inflight_tracking.pop(event_id, None)
    
    def _cleanup_expired_tracking(self):
        """Ack-Window Expansion: Cleanup expired inflight tracking entries"""
        current_time = time.time()
        expired_events = []
        
        # Find expired events (180s TTL)
        for event_id, timestamp in self._inflight_tracking.items():
            if current_time - timestamp > self.max_inflight_ttl:
                expired_events.append(event_id)
        
        # Remove expired events
        for event_id in expired_events:
            self._inflight_tracking.pop(event_id, None)
            self.logger.debug(f"Ack-Window: Expired event removed from buffer: {event_id}")
        
        # Context Retention: Prevent memory rotation during 1M message burst
        if len(self._inflight_tracking) > self.max_tracking_size:
            # Remove oldest entries to maintain size limit
            sorted_events = sorted(self._inflight_tracking.items(), key=lambda x: x[1])
            excess_count = len(self._inflight_tracking) - self.max_tracking_size
            
            for i in range(excess_count):
                event_id, _ = sorted_events[i]
                self._inflight_tracking.pop(event_id, None)
                self.logger.debug(f"Context Retention: Removed oldest event from buffer: {event_id}")
        
        if expired_events or len(self._inflight_tracking) > self.max_tracking_size:
            self.logger.debug(f"Ack-Window: Buffer size after cleanup: {len(self._inflight_tracking)}")
    
    def get_tracking_stats(self) -> Dict[str, int]:
        """Get tracking buffer statistics for monitoring"""
        return {
            'pending_publishes': len(self._pending_publishes),
            'inflight_tracking': len(self._inflight_tracking),
            'max_tracking_size': self.max_tracking_size,
            'max_inflight_ttl': self.max_inflight_ttl
        }
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """Add event to batch for publishing"""
        if not self.connected:
            self.logger.error("Not connected to EMQX")
            self.metrics.increment_counter('publish_failures')
            return False
        
        try:
            event_data = event.get('message', {})
            event_id = event_data.get('event_id')
            table = event_data.get('table')
            
            if not event_id or not table:
                return False
            
            if self.state_manager.is_duplicate(event_id):
                return True
            
            self.state_manager.update_state(event_id, EventState.PUBLISHED)
            
            if self.batch_enabled:
                with self._lock:
                    self.event_batch.append({
                        'event_id': event_id,
                        'table': table,
                        'message': event,
                        'timestamp': time.time()
                    })
                    
                    # Flush if batch full or interval exceeded
                    if len(self.event_batch) >= self.batch_size:
                        self._flush_batch()
                    elif time.time() - self.last_publish_time >= self.batch_interval:
                        self._flush_batch()
            else:
                return self._publish_single(event_id, table, event)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding to batch: {e}", exc_info=True)
            self.metrics.increment_counter('publish_failures')
            return False
    
    def _flush_batch(self):
        """Flush batched events as JSON array to bulk topic"""
        if not self.event_batch:
            return
        
        try:
            batch = self.event_batch.copy()
            self.event_batch = []
            self.last_publish_time = time.time()
            
            if self.use_bulk_topic and len(batch) > 1:
                self._publish_bulk_batch(batch)
            else:
                # Fall back to individual publishes
                for item in batch:
                    self._publish_single(item['event_id'], item['table'], item['message'])
            
            self.messages_processed += len(batch)
            
            # Health log every N messages with Ack-Window stats
            log_interval = self.config.get('log_every_n_messages', 10000)
            if self.messages_processed % log_interval == 0:
                tracking_stats = self.get_tracking_stats()
                queue_depth = len(self._pending_publishes)
                latency = (time.time() - self.last_publish_time) * 1000
                self.logger.info(
                    f"High-Performance Health: Processed {self.messages_processed:,} messages, "
                    f"pending ACKs: {queue_depth}, batch latency: {latency:.2f}ms, "
                    f"tracking buffer: {tracking_stats['inflight_tracking']}/{tracking_stats['max_tracking_size']}"
                )
            
        except Exception as e:
            self.logger.error(f"Error flushing batch: {e}", exc_info=True)
            with self._lock:
                self.event_batch.extend(batch)
    
    def _publish_bulk_batch(self, batch: List[Dict[str, Any]]):
        """Publish entire batch as single JSON array"""
        try:
            # Extract event IDs for tracking
            event_ids = [item['event_id'] for item in batch]
            
            # Build bulk payload
            bulk_payload = {
                'batch_id': f"batch_{int(time.time() * 1000)}",
                'event_count': len(batch),
                'events': [item['message'] for item in batch],
                'timestamp': time.time()
            }
            
            topic = self.config.get('topic_bulk', f"{self.config['topic_prefix']}/bulk")
            message = json.dumps(bulk_payload, separators=(',', ':'))
            
            result = self.client.publish(
                topic=topic,
                payload=message,
                qos=self.config.get('qos_publish', 2),
                retain=False
            )
            
            # Track all event IDs under this message ID
            self._pending_publishes[result.mid] = event_ids
            
            self.state_manager.update_state(
                event_ids[0],  # First event for reference
                EventState.PUBLISHED,
                mqtt_message_id=result.mid
            )
            
            self.logger.debug(
                f"Published bulk batch of {len(batch)} events to {topic}, "
                f"message_id: {result.mid}"
            )
            self.metrics.increment_counter('bulk_publishes')
            
        except Exception as e:
            self.logger.error(f"Error publishing bulk batch: {e}", exc_info=True)
            # Re-add for retry
            with self._lock:
                self.event_batch.extend(batch)
    
    def _publish_single(self, event_id: str, table: str, event: Dict[str, Any]) -> bool:
        """Publish a single event"""
        try:
            topic = f"{self.config['topic_prefix']}/{table}"
            message = json.dumps(event, separators=(',', ':'))
            
            result = self.client.publish(
                topic=topic,
                payload=message,
                qos=self.config.get('qos_publish', 2),
                retain=False
            )
            
            self._pending_publishes[result.mid] = event_id
            
            self.state_manager.update_state(
                event_id,
                EventState.PUBLISHED,
                mqtt_message_id=result.mid
            )
            
            self.metrics.increment_counter('publish_attempts')
            return True
            
        except Exception as e:
            self.logger.error(f"Error publishing event: {e}", exc_info=True)
            self.state_manager.update_state(event_id, EventState.FAILED, error=str(e))
            try:
                self.dlq.add_message(
                    event_id=event_id,
                    message_type='PUBLISH_FAILURE',
                    payload=event,
                    error=str(e)
                )
            except Exception as dlq_error:
                self.logger.error(f"DLQ operation failed: {dlq_error}")
            return False
    
    def is_connected(self) -> bool:
        """Check if connected"""
        return self.connected
    
    def get_pending_count(self) -> int:
        """Get pending ACKs"""
        return len(self._pending_publishes)
