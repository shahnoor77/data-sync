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
        """Initialize EMQX publisher"""
        self.config = config
        self.state_manager = state_manager
        self.dlq = dlq
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        self._lock = threading.Lock()
        self._pending_publishes = {}
        
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
        """Setup EMQX MQTT client with stable client ID"""
        # Use STABLE client ID for session recovery
        self.client_id = self.config.get('client_id_publisher', f"prod_publisher_01")
        
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,  # Changed to v311 for consistency with subscriber
            clean_session=False  # Session persistence for QoS reliability
        )
        
        # Set max in-flight BEFORE connect
        self.client.max_inflight_messages_set(
            self.config.get('max_inflight_messages', 2000)  # Increased to match subscriber
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
            f"EMQX publisher configured with stable client_id: {self.client_id}, "
            f"max_inflight: {self.config.get('max_inflight_messages', 2000)}, clean_session: False"
        )
    
    def connect(self):
        """Connect to EMQX broker with persistent session"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                broker_host = self.config['broker_host']
                broker_port = int(self.config['broker_port'])
                keepalive = int(self.config.get('keepalive', 60))
                
                self.logger.info(
                    f"Connecting to EMQX: {broker_host}:{broker_port} "
                    f"(attempt {retry_count + 1}/{max_retries})"
                )
                
                self.client.connect(
                    broker_host,
                    broker_port,
                    keepalive
                )
                
                self.client.loop_start()
                
                # Wait for connection
                timeout = 10
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    raise ConnectionError("Failed to connect to EMQX")
                
                self.logger.info("✓ Connected to EMQX with persistent session")
                return
                
            except Exception as e:
                retry_count += 1
                self.logger.warning(f"Connection attempt {retry_count} failed: {e}")
                
                if retry_count < max_retries:
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
        """Callback for connection"""
        if rc == 0:
            self.connected = True
            self.logger.info("✓ EMQX publisher connected")
            
            # Subscribe to ACK topic
            ack_topic = f"{self.config['ack_topic']}/#"
            client.subscribe(ack_topic, qos=self.config.get('qos_subscribe', 2))
            self.logger.info(f"✓ Subscribed to {ack_topic}")
            
            self.metrics.set_gauge('mqtt_connected', 1)
        else:
            self.logger.error(f"Connection failed: {rc}")
            self.connected = False
            self.metrics.set_gauge('mqtt_connected', 0)
    
    def _on_disconnect(self, client, userdata, rc, properties=None):
        """Callback for disconnect"""
        self.connected = False
        self.metrics.set_gauge('mqtt_connected', 0)
        
        if rc != 0:
            self.logger.warning(f"Unexpected disconnect: {rc}")
    
    def _on_publish(self, client, userdata, mid, reason_codes=None, properties=None):
        """Callback for publish confirmation"""
        try:
            event_ids = self._pending_publishes.get(mid)
            
            if event_ids:
                if isinstance(event_ids, list):
                    for event_id in event_ids:
                        self.state_manager.update_state(
                            event_id,
                            EventState.MQTT_CONFIRMED
                        )
                    self.metrics.increment_counter('mqtt_published', len(event_ids))
                else:
                    self.state_manager.update_state(
                        event_ids,
                        EventState.MQTT_CONFIRMED
                    )
                    self.metrics.increment_counter('mqtt_published')
                
                del self._pending_publishes[mid]
            
        except Exception as e:
            self.logger.error(f"Error in publish callback: {e}", exc_info=True)
    
    def _on_ack_message(self, client, userdata, msg):
        """Handle ACK messages from subscriber"""
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
                    # Filter Test Events: Check if event_id starts with 'evt_'
                    if event_id.startswith('evt_'):
                        self.logger.info(f"Test ACK received for {event_id}")
                        continue
                    
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
            else:
                # Filter Test Events: Check if event_id starts with 'evt_'
                if event_ids.startswith('evt_'):
                    self.logger.info(f"Test ACK received for {event_ids}")
                    return
                
                if success:
                    self.state_manager.update_state(event_ids, EventState.ACKNOWLEDGED)
                    self.metrics.increment_counter('mqtt_acks')
                else:
                    self.state_manager.update_state(event_ids, EventState.FAILED, error=error)
                    try:
                        self.dlq.add_message(
                            event_id=event_ids,
                            message_type='ACK_FAILURE',
                            payload={'error': error},
                            error=f"NACK: {error}"
                        )
                    except Exception as dlq_error:
                        self.logger.error(f"DLQ operation failed: {dlq_error}")
                
        except Exception as e:
            self.logger.error(f"Error processing ACK: {e}", exc_info=True)
    
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
            
            # Health log every N messages
            log_interval = self.config.get('log_every_n_messages', 10000)
            if self.messages_processed % log_interval == 0:
                queue_depth = len(self._pending_publishes)
                latency = (time.time() - self.last_publish_time) * 1000
                self.logger.info(
                    f"Health check: Processed {self.messages_processed} messages, "
                    f"pending ACKs: {queue_depth}, batch latency: {latency:.2f}ms"
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
