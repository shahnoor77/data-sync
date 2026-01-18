"""
MQTT Subscriber for CDC events
Receives events, writes to target database, sends ACK
"""

import json
import time
from typing import Dict, Any
import paho.mqtt.client as mqtt
from ..database.connector import DatabaseConnector, create_connector
from ..database.schema_manager import SchemaManager
from ..utils.crypto import CryptoManager


class MQTTSubscriber:
    """
    Subscribes to CDC events and writes to target database
    """
    
    def __init__(
        self,
        mqtt_config: Dict[str, Any],
        target_db_config: Dict[str, Any],
        crypto_manager: CryptoManager,
        logger,
        metrics
    ):
        """
        Initialize MQTT subscriber
        
        Args:
            mqtt_config: MQTT configuration
            target_db_config: Target database configuration
            crypto_manager: Crypto manager for decryption
            logger: Logger instance
            metrics: Metrics collector
        """
        self.mqtt_config = mqtt_config
        self.target_db_config = target_db_config
        self.crypto = crypto_manager
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        
        # Database connection
        self.target_db = None
        self.schema_manager = None
        
        # Connect to target database
        self._connect_to_database()
        
        # Setup MQTT client
        self._setup_client()
    
    def _connect_to_database(self):
        """Connect to target database"""
        try:
            self.logger.info(f"Connecting to target database: {self.target_db_config['type']}")
            
            self.target_db = create_connector(
                self.target_db_config['type'],
                self.target_db_config
            )
            
            # Create schema manager
            self.schema_manager = SchemaManager(
                self.target_db,
                self.target_db_config['type']
            )
            
            self.logger.info("✓ Connected to target database")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to target database: {e}")
            raise
    
    def _setup_client(self):
        """Setup MQTT client"""
        self.client = mqtt.Client(
            client_id=f"cdc_subscriber_{int(time.time())}",
            clean_session=False,
            protocol=mqtt.MQTTv311
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # Set authentication
        if self.mqtt_config.get('username') and self.mqtt_config.get('password'):
            self.client.username_pw_set(
                self.mqtt_config['username'],
                self.mqtt_config['password']
            )
        
        self.logger.info("MQTT subscriber configured")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            broker_host = self.mqtt_config['broker_host']
            broker_port = int(self.mqtt_config['broker_port'])
            keepalive = int(self.mqtt_config.get('keepalive', 60))
            self.logger.info(
                f"Connecting to MQTT broker: {broker_host}:{broker_port}"
            )
            
            self.client.connect(
                broker_host,
                broker_port,
                keepalive
            )
            
            # Start network loop
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
        
        if self.target_db:
            self.target_db.disconnect()
        
        self.logger.info("Disconnected from MQTT broker and database")
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects"""
        if rc == 0:
            self.connected = True
            self.logger.info("✓ MQTT subscriber connected successfully")
            
            # Subscribe to data topics
            data_topic = f"{self.mqtt_config['topic_prefix']}/#"
            client.subscribe(data_topic, qos=2)
            self.logger.info(f"✓ Subscribed to data topic: {data_topic}")
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
    
    def _on_message(self, client, userdata, msg):
        """Callback for when message is received"""
        start_time = time.time()
        event_id = None
        
        try:
            # Parse message
            signed_event = json.loads(msg.payload.decode())
            
            # Verify signature
            if not self.crypto.verify_signed_message(signed_event):
                self.logger.error("✗ Invalid signature - message rejected")
                return
            
            # Extract event
            event = signed_event.get('message', {})
            event_id = event.get('event_id')
            
            if not event_id:
                self.logger.error("Event missing event_id")
                return
            
            self.logger.log_event_lifecycle(
                event_id,
                'DELIVERED',
                data={'topic': msg.topic}
            )
            
            # Decrypt sensitive fields
            if 'data' in event:
                event['data'] = self.crypto.decrypt_sensitive_fields(event['data'])
            
            # Write to target database
            success, error = self._write_to_database(event)
            
            # Record latency
            duration = time.time() - start_time
            self.metrics.record_latency('end_to_end', duration)
            
            # Send ACK
            self._send_acknowledgment(event_id, success, error)
            
            if success:
                self.logger.info(
                    f"✓ Successfully processed event {event_id}",
                    event_id=event_id,
                    processing_time_ms=duration * 1000
                )
            else:
                self.logger.error(
                    f"✗ Failed to process event {event_id}: {error}",
                    event_id=event_id,
                    error=error
                )
                
        except Exception as e:
            self.logger.error(
                f"Error processing message: {e}",
                event_id=event_id if event_id else 'unknown',
                exc_info=True
            )
            
            if event_id:
                self._send_acknowledgment(event_id, False, str(e))
    
    def _write_to_database(self, event: Dict[str, Any]) -> tuple:
        """
        Write event to target database
        
        Returns:
            (success: bool, error: str or None)
        """
        try:
            table = event.get('table')
            operation = event.get('operation')
            data = event.get('data', {})
            event_id = event.get('event_id')
            
            if not table or not operation:
                return False, "Missing table or operation"
            
            # Add sync metadata
            data_with_metadata = data.copy()
            data_with_metadata['cdc_operation'] = operation
            data_with_metadata['synced_at'] = time.time()
            
            # Perform operation
            if operation == 'INSERT' or operation == 'READ':
                # Remove id if exists (let target generate)
                if 'id' in data_with_metadata:
                    del data_with_metadata['id']
                
                record_id = self.target_db.insert_record(table, data_with_metadata)
                
                self.logger.log_event_lifecycle(
                    event_id,
                    'WRITTEN',
                    data={
                        'table': table,
                        'operation': operation,
                        'record_id': str(record_id)
                    }
                )
                
            elif operation == 'UPDATE':
                key_field = 'id'
                key_value = data.get(key_field)
                
                if not key_value:
                    return False, f"Missing key field: {key_field}"
                
                update_data = {k: v for k, v in data_with_metadata.items() if k != key_field}
                
                self.target_db.update_record(table, update_data, key_field, key_value)
                
                self.logger.log_event_lifecycle(
                    event_id,
                    'WRITTEN',
                    data={
                        'table': table,
                        'operation': operation,
                        'key': f"{key_field}={key_value}"
                    }
                )
                
            elif operation == 'DELETE':
                key_field = 'id'
                key_value = data.get(key_field)
                
                if not key_value:
                    return False, f"Missing key field: {key_field}"
                
                self.target_db.delete_record(table, key_field, key_value)
                
                self.logger.log_event_lifecycle(
                    event_id,
                    'WRITTEN',
                    data={
                        'table': table,
                        'operation': operation,
                        'key': f"{key_field}={key_value}"
                    }
                )
            
            return True, None
            
        except Exception as e:
            self.logger.error(f"Database write error: {e}", exc_info=True)
            return False, str(e)
    
    def _send_acknowledgment(self, event_id: str, success: bool, error: str = None):
        """Send ACK message back to publisher"""
        try:
            ack_message = {
                'event_id': event_id,
                'success': success,
                'error': error,
                'timestamp': time.time(),
                'subscriber_id': f"subscriber_{int(time.time())}"
            }
            
            ack_topic = f"{self.mqtt_config['ack_topic']}/{event_id}"
            
            self.client.publish(
                ack_topic,
                json.dumps(ack_message),
                qos=2
            )
            
            self.logger.debug(f"Sent ACK for event {event_id}: {success}")
            
        except Exception as e:
            self.logger.error(f"Error sending ACK: {e}", exc_info=True)
    
    def run(self):
        """Run the subscriber (blocking call)"""
        self.logger.info("MQTT subscriber running...")
        self.client.loop_forever()