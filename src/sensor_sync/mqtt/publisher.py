"""
MQTT Publisher for CDC events
Publishes processed events with QoS 2 (exactly once)
"""

import json
import time
import threading
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt
from ..core.state_manager import StateManager, EventState


class MQTTPublisher:
    """
    Publishes CDC events to MQTT broker
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        state_manager: StateManager,
        logger,
        metrics
    ):
        """
        Initialize MQTT publisher
        
        Args:
            config: MQTT configuration
            state_manager: State manager for tracking
            logger: Logger instance
            metrics: Metrics collector
        """
        self.config = config
        self.state_manager = state_manager
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        self._lock = threading.Lock()
        
        # Setup client
        self._setup_client()
    
    def _setup_client(self):
        """Setup MQTT client"""
        self.client = mqtt.Client(
            client_id=f"cdc_publisher_{int(time.time())}",
            clean_session=False,
            protocol=mqtt.MQTTv311
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        # Set authentication if provided
        if self.config.get('username') and self.config.get('password'):
            self.client.username_pw_set(
                self.config['username'],
                self.config['password']
            )
        
        self.logger.info("MQTT publisher configured")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            broker_host = self.config['broker_host']
            broker_port = int(self.config['broker_port'])
            keepalive = int(self.config.get('keepalive', 60))
            self.logger.info(
                f"Connecting to MQTT broker: {broker_host}:{broker_port}"
            )
            
            self.client.connect(
                broker_host,
                broker_port,
                keepalive
            )
            
            # Start network loop in background
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise ConnectionError("Failed to connect to MQTT broker")
            
            self.logger.info("✓ Connected to MQTT broker")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        self.logger.info("Disconnected from MQTT broker")
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects"""
        if rc == 0:
            self.connected = True
            self.logger.info("✓ MQTT publisher connected successfully")
            
            # Subscribe to ACK topic
            ack_topic = f"{self.config['ack_topic']}/#"
            client.subscribe(ack_topic, qos=2)
            self.logger.info(f"✓ Subscribed to ACK topic: {ack_topic}")
            
            # Set message callback for ACKs
            client.on_message = self._on_ack_message
        else:
            self.logger.error(f"✗ MQTT connection failed with code: {rc}")
            self.connected = False
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback for when client disconnects"""
        self.connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected MQTT disconnect: {rc}")
        else:
            self.logger.info("MQTT client disconnected")
    
    def _on_publish(self, client, userdata, mid):
        """Callback for when message is published"""
        self.logger.debug(f"Message {mid} published successfully")
        
        # Update state to MQTT_CONFIRMED
        event_id = self.state_manager.get_event_by_mqtt_id(mid)
        if event_id:
            self.state_manager.update_state(
                event_id,
                EventState.MQTT_CONFIRMED
            )
        
        self.metrics.increment_counter('mqtt_published')
    
    def _on_ack_message(self, client, userdata, msg):
        """Callback for ACK messages from subscriber"""
        try:
            ack_data = json.loads(msg.payload.decode())
            
            event_id = ack_data.get('event_id')
            success = ack_data.get('success', False)
            error = ack_data.get('error')
            
            if not event_id:
                self.logger.warning("Received ACK without event_id")
                return
            
            if success:
                self.logger.info(
                    f"✓ Received ACK for event {event_id}",
                    event_id=event_id
                )
                self.state_manager.update_state(
                    event_id,
                    EventState.ACKNOWLEDGED
                )
                self.metrics.increment_counter('mqtt_acks')
            else:
                self.logger.error(
                    f"✗ Received NACK for event {event_id}: {error}",
                    event_id=event_id,
                    error=error
                )
                self.state_manager.update_state(
                    event_id,
                    EventState.FAILED,
                    error=error
                )
                
        except Exception as e:
            self.logger.error(f"Error processing ACK message: {e}", exc_info=True)
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """
        Publish event to MQTT
        
        Args:
            event: Signed event to publish
            
        Returns:
            True if published successfully
        """
        if not self.connected:
            self.logger.error("Cannot publish: Not connected to MQTT broker")
            return False
        
        try:
            # Extract event details
            event_data = event.get('message', {})
            event_id = event_data.get('event_id')
            table = event_data.get('table')
            
            if not event_id or not table:
                self.logger.error("Event missing required fields")
                return False
            
            # Update state to PUBLISHING
            self.state_manager.update_state(event_id, EventState.PUBLISHED)
            
            # Build topic
            topic = f"{self.config['topic_prefix']}/{table}"
            
            # Convert to JSON
            message = json.dumps(event)
            
            # Publish with QoS 2 (exactly once)
            result = self.client.publish(
                topic,
                message,
                qos=self.config.get('qos', 2),
                retain=False
            )
            
            # Store MQTT message ID
            self.state_manager.update_state(
                event_id,
                EventState.PUBLISHED,
                mqtt_message_id=result.mid
            )
            
            self.logger.info(
                f"✓ Published event to {topic}",
                event_id=event_id,
                mqtt_message_id=result.mid,
                topic=topic
            )
            
            # Wait for publish to complete
            result.wait_for_publish()
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Error publishing event: {e}",
                event_id=event_id if 'event_id' in locals() else 'unknown',
                exc_info=True
            )
            
            if 'event_id' in locals():
                self.state_manager.update_state(
                    event_id,
                    EventState.FAILED,
                    error=str(e)
                )
            
            return False