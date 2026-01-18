"""
EMQX Subscriber with optimized bulk inserts and message buffering
"""

import json
import time
import uuid
import threading
import queue
from typing import Dict, Any, List
import paho.mqtt.client as mqtt
from ..database.connector import create_connector
from ..database.schema_manager import SchemaManager
from ..utils.crypto import CryptoManager
from ..core.dead_letter_queue import DeadLetterQueue


class EMQXSubscriber:
    """
    High-throughput MQTT subscriber with bulk database operations
    """
    
    def __init__(
        self,
        mqtt_config: Dict[str, Any],
        target_db_config: Dict[str, Any],
        crypto_manager: CryptoManager,
        dlq: DeadLetterQueue,
        logger,
        metrics
    ):
        """Initialize EMQX subscriber"""
        self.mqtt_config = mqtt_config
        self.target_db_config = target_db_config
        self.crypto = crypto_manager
        self.dlq = dlq
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        self._lock = threading.Lock()
        
        # Database connection
        self.target_db = None
        self.schema_manager = None
        
        # Message buffering - INCREASED to 200k
        # Store raw payloads only (no parsing in MQTT callback)
        self.message_queue = queue.Queue(maxsize=200000)
        self.buffer_size = self.target_db_config.get('bulk_batch_size', 1000)
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        
        # Idempotency tracking
        self._processed_messages = {}
        self._message_retention = 300
        self.messages_processed = 0
        self.writer_thread_running = False
        
        # Connect to database
        self._connect_to_database()
        
        # Setup MQTT client
        self._setup_client()
    
    def _connect_to_database(self):
        """Connect to target database"""
        try:
            self.logger.info(f"Connecting to target: {self.target_db_config['type']}")
            
            self.target_db = create_connector(
                self.target_db_config['type'],
                self.target_db_config
            )
            
            self.schema_manager = SchemaManager(
                self.target_db,
                self.target_db_config['type']
            )
            
            self.logger.info("✓ Connected to target database")
            
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise
    
    def _setup_client(self):
        """Setup EMQX MQTT client with stable ID and increased keepalive"""
        self.client_id = self.mqtt_config.get('client_id_subscriber', "prod_subscriber_01")
        
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=mqtt.MQTTv5
        )
        
        # Set max in-flight
        self.client.max_inflight_messages_set(
            self.mqtt_config.get('max_inflight_messages', 200)
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # Authentication
        if self.mqtt_config.get('username') and self.mqtt_config.get('password'):
            self.client.username_pw_set(
                self.mqtt_config['username'],
                self.mqtt_config['password']
            )
        
        # Reconnect behavior
        self.client.reconnect_delay_set(
            min_delay=self.mqtt_config.get('reconnect_min_interval', 1),
            max_delay=self.mqtt_config.get('reconnect_max_interval', 30)
        )
        
        self.logger.info(
            f"EMQX subscriber configured with stable client_id: {self.client_id}, "
            f"keepalive: 120s"
        )
    
    def connect(self):
        """Connect to EMQX broker"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                broker_host = self.mqtt_config['broker_host']
                broker_port = int(self.mqtt_config['broker_port'])
                keepalive = 120  # Increased from 60
                
                self.logger.info(
                    f"Connecting to EMQX: {broker_host}:{broker_port} "
                    f"(attempt {retry_count + 1}/{max_retries})"
                )
                
                self.client.connect(
                    broker_host,
                    broker_port,
                    keepalive,
                    clean_start=False
                )
                
                self.client.loop_start()
                
                timeout = 10
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    raise ConnectionError("Failed to connect")
                
                # Start writer thread
                self._start_writer_thread()
                
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
        """Disconnect gracefully"""
        self.writer_thread_running = False
        self._flush_buffer()
        
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        
        if self.target_db:
            self.target_db.disconnect()
        
        self.logger.info(f"Subscriber stopped. Total messages: {self.messages_processed}")
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback for connection"""
        if rc == 0:
            self.connected = True
            self.logger.info("✓ EMQX subscriber connected")
            
            # Subscribe to both individual and bulk topics
            data_topic = f"{self.mqtt_config['topic_prefix']}/#"
            bulk_topic = self.mqtt_config.get('topic_bulk', 
                f"{self.mqtt_config['topic_prefix']}/bulk")
            
            client.subscribe(data_topic, qos=self.mqtt_config.get('qos_subscribe', 2))
            client.subscribe(bulk_topic, qos=self.mqtt_config.get('qos_subscribe', 2))
            
            self.logger.info(f"✓ Subscribed to {data_topic} and {bulk_topic}")
            
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
    
    def _on_message(self, client, userdata, msg):
        """
        Callback for incoming MQTT message
        ONLY puts raw payload in queue - NO parsing here
        """
        try:
            # Add raw payload to queue without blocking MQTT loop
            # Store topic info for later processing
            self.message_queue.put_nowait({
                'payload': msg.payload,  # Raw bytes
                'topic': msg.topic,
                'qos': msg.qos,
                'timestamp': time.time()
            })
            
        except queue.Full:
            self.logger.warning("Message queue full, dropping message")
            self.metrics.increment_counter('dropped_messages')
        except Exception as e:
            self.logger.error(f"Error queuing message: {e}", exc_info=True)
    
    def _start_writer_thread(self):
        """Start background database writer thread"""
        self.writer_thread_running = True
        writer_thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name="DatabaseWriter"
        )
        writer_thread.start()
        self.logger.info("Started database writer thread")
    
    def _writer_loop(self):
        """
        Background thread for database writes
        Handles ALL JSON parsing and decryption here (not in MQTT callback)
        """
        self.logger.info("Writer loop started")
        
        while self.writer_thread_running:
            try:
                # Get raw message from queue with timeout
                msg_data = self.message_queue.get(timeout=1.0)
                
                # PARSE AND DECRYPT HERE (not in _on_message)
                self._process_message(msg_data)
                
                # Check if buffer needs flushing
                with self.buffer_lock:
                    if len(self.message_buffer) >= self.buffer_size:
                        self._flush_buffer()
                
                self.message_queue.task_done()
                
            except queue.Empty:
                # Periodically flush buffer even if not full
                with self.buffer_lock:
                    if self.message_buffer:
                        self._flush_buffer()
                continue
            except Exception as e:
                self.logger.error(f"Error in writer loop: {e}", exc_info=True)
                time.sleep(1)
    
    def _process_message(self, msg_data: Dict[str, Any]):
        """
        Process individual message (JSON parsing and decryption)
        Called from writer thread, NOT from MQTT callback
        """
        try:
            # DECODE from bytes to string
            payload = msg_data['payload'].decode('utf-8')
            
            # Handle bulk batch
            if 'batch_id' in payload and msg_data['topic'].endswith('/bulk'):
                self._process_bulk_batch(payload)
            else:
                # Handle individual message - NOW parse JSON
                try:
                    signed_event = json.loads(payload)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON in message: {e}")
                    self.metrics.increment_counter('invalid_json')
                    return
                
                # Verify signature
                if not self.crypto.verify_signed_message(signed_event):
                    self.logger.error("Invalid signature rejected")
                    self.metrics.increment_counter('invalid_signatures')
                    return
                
                event = signed_event.get('message', {})
                event_id = event.get('event_id')
                
                if not event_id:
                    self.logger.warning("Event missing event_id")
                    return
                
                # Check for duplicates
                if self._is_duplicate(event_id):
                    self._send_acknowledgment(event_id, True, "Duplicate")
                    return
                
                # DECRYPT sensitive fields here
                if 'data' in event:
                    try:
                        event['data'] = self.crypto.decrypt_sensitive_fields(event['data'])
                    except Exception as e:
                        self.logger.error(f"Decryption error for {event_id}: {e}")
                        self._send_acknowledgment(event_id, False, f"Decryption error: {e}")
                        return
                
                # Add to buffer
                with self.buffer_lock:
                    self.message_buffer.append({
                        'event_id': event_id,
                        'event': event,
                        'timestamp': time.time()
                    })
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
    
    def _process_bulk_batch(self, bulk_payload_str: str):
        """Process bulk batch of events"""
        try:
            # Parse bulk JSON
            try:
                bulk_data = json.loads(bulk_payload_str)
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in bulk batch: {e}")
                self.metrics.increment_counter('invalid_json')
                return
            
            events = bulk_data.get('events', [])
            self.logger.debug(f"Processing bulk batch of {len(events)} events")
            
            for event_envelope in events:
                # Verify signature for each event
                if not self.crypto.verify_signed_message(event_envelope):
                    self.logger.warning("Invalid signature in bulk batch, skipping event")
                    self.metrics.increment_counter('invalid_signatures')
                    continue
                
                event = event_envelope.get('message', {})
                event_id = event.get('event_id')
                
                if not event_id:
                    self.logger.warning("Event in bulk missing event_id")
                    continue
                
                # Check for duplicates
                if self._is_duplicate(event_id):
                    self._send_acknowledgment(event_id, True, "Duplicate")
                    continue
                
                # DECRYPT sensitive fields
                if 'data' in event:
                    try:
                        event['data'] = self.crypto.decrypt_sensitive_fields(event['data'])
                    except Exception as e:
                        self.logger.error(f"Decryption error for {event_id}: {e}")
                        self._send_acknowledgment(event_id, False, f"Decryption error: {e}")
                        continue
                
                # Add to buffer
                with self.buffer_lock:
                    self.message_buffer.append({
                        'event_id': event_id,
                        'event': event,
                        'timestamp': time.time()
                    })
        
        except Exception as e:
            self.logger.error(f"Error processing bulk batch: {e}", exc_info=True)
    
    def _flush_buffer(self):
        """Flush buffered messages to database in true bulk operations"""
        if not self.message_buffer:
            return
        
        buffer = self.message_buffer.copy()
        self.message_buffer = []
        
        try:
            # Group by table
            tables = {}
            for item in buffer:
                event = item['event']
                table = event.get('table')
                if table not in tables:
                    tables[table] = []
                tables[table].append(item)
            
            # Bulk insert per table (single transaction)
            for table, items in tables.items():
                try:
                    self._bulk_insert_transaction(table, items)
                except Exception as e:
                    self.logger.error(f"Error bulk inserting {table}: {e}")
                    for item in items:
                        self._send_acknowledgment(
                            item['event_id'],
                            False,
                            f"DB error: {str(e)}"
                        )
                        self.dlq.add_message(
                            event_id=item['event_id'],
                            message_type='DATABASE_FAILURE',
                            payload=item['event'],
                            error=str(e)
                        )
            
            self.messages_processed += len(buffer)
            
            # Health log every N messages
            log_interval = self.mqtt_config.get('log_every_n_messages', 10000)
            if self.messages_processed % log_interval == 0:
                queue_depth = self.message_queue.qsize()
                self.logger.info(
                    f"Health check: Processed {self.messages_processed} messages, "
                    f"queue depth: {queue_depth}"
                )
            
        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}", exc_info=True)
    
    def _bulk_insert_transaction(self, table: str, items: List[Dict[str, Any]]):
        """
        Perform true bulk insert in single transaction
        Only sends ACKs after successful commit
        """
        if not items:
            return
        
        start_time = time.time()
        event_ids = [item['event_id'] for item in items]
        
        try:
            # Begin transaction
            if hasattr(self.target_db.connection, 'begin'):
                self.target_db.connection.begin()
            
            # Prepare bulk data
            insert_data = []
            update_data = []
            delete_data = []
            
            for item in items:
                event = item['event']
                data = event.get('data', {}).copy()
                operation = event.get('operation')
                
                # Add metadata
                data['cdc_operation'] = operation
                data['synced_at'] = time.time()
                
                if operation in ['INSERT', 'READ']:
                    if 'id' in data:
                        del data['id']
                    insert_data.append(data)
                elif operation == 'UPDATE':
                    update_data.append((data, item['event_id']))
                elif operation == 'DELETE':
                    delete_data.append(item['event_id'])
            
            # Execute bulk operations
            if insert_data:
                self._bulk_insert_records(table, insert_data)
            
            if update_data:
                self._bulk_update_records(table, update_data)
            
            if delete_data:
                self._bulk_delete_records(table, delete_data)
            
            # COMMIT TRANSACTION
            self.target_db.connection.commit()
            
            # ONLY AFTER COMMIT: Send ACKs
            for event_id in event_ids:
                self._send_acknowledgment(event_id, True)
                self._mark_processed(event_id)
            
            duration = time.time() - start_time
            self.metrics.record_latency('bulk_insert', duration)
            self.metrics.increment_counter('bulk_inserts')
            
            self.logger.debug(
                f"Bulk inserted {len(items)} events into {table} "
                f"({duration*1000:.2f}ms)"
            )
            
        except Exception as e:
            # ROLLBACK on error
            try:
                self.target_db.connection.rollback()
            except Exception:
                pass
            
            self.logger.error(
                f"Bulk insert failed, transaction rolled back: {e}",
                exc_info=True
            )
            raise
    
    def _bulk_insert_records(self, table: str, data_list: List[Dict[str, Any]]):
        """Bulk insert using database-specific method"""
        if not data_list:
            return
        
        db_type = self.target_db_config['type']
        
        if db_type == 'postgresql':
            self._bulk_insert_postgresql(table, data_list)
        elif db_type == 'mysql':
            self._bulk_insert_mysql(table, data_list)
        elif db_type == 'mongodb':
            self._bulk_insert_mongodb(table, data_list)
    
    def _bulk_insert_postgresql(self, table: str, data_list: List[Dict[str, Any]]):
        """PostgreSQL bulk insert using execute_values"""
        try:
            import psycopg2.extras
            
            if not data_list:
                return
            
            columns = list(data_list[0].keys())
            values = [tuple(row[col] for col in columns) for row in data_list]
            
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
            
            cursor = self.target_db.connection.cursor()
            psycopg2.extras.execute_values(
                cursor,
                query,
                values,
                page_size=len(values)
            )
            
        except Exception as e:
            self.logger.error(f"PostgreSQL bulk insert error: {e}")
            raise
    
    def _bulk_insert_mysql(self, table: str, data_list: List[Dict[str, Any]]):
        """MySQL bulk insert"""
        try:
            if not data_list:
                return
            
            columns = list(data_list[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            values_list = [tuple(row[col] for col in columns) for row in data_list]
            
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            cursor = self.target_db.connection.cursor()
            cursor.executemany(query, values_list)
            
        except Exception as e:
            self.logger.error(f"MySQL bulk insert error: {e}")
            raise
    
    def _bulk_insert_mongodb(self, table: str, data_list: List[Dict[str, Any]]):
        """MongoDB bulk insert using insert_many"""
        try:
            if not data_list:
                return
            
            collection = self.target_db.db[table]
            collection.insert_many(data_list)
            
        except Exception as e:
            self.logger.error(f"MongoDB bulk insert error: {e}")
            raise
    
    def _bulk_update_records(self, table: str, update_data: List[tuple]):
        """Bulk update records"""
        for data, event_id in update_data:
            key_field = 'id'
            key_value = data.get(key_field)
            if key_value:
                update_dict = {k: v for k, v in data.items() if k != key_field}
                self.target_db.update_record(table, update_dict, key_field, key_value)
    
    def _bulk_delete_records(self, table: str, delete_ids: List[str]):
        """Bulk delete records"""
        for event_id in delete_ids:
            self.target_db.delete_record(table, 'id', event_id)
    
    def _send_acknowledgment(self, event_id: str, success: bool, error: str = None):
        """Send ACK back to publisher"""
        try:
            ack_message = {
                'event_ids': event_id if isinstance(event_id, list) else event_id,
                'success': success,
                'error': error,
                'timestamp': time.time(),
                'subscriber_id': self.client_id
            }
            
            ack_topic = f"{self.mqtt_config['ack_topic']}/{event_id if isinstance(event_id, str) else 'batch'}"
            
            self.client.publish(
                ack_topic,
                json.dumps(ack_message),
                qos=2
            )
            
        except Exception as e:
            self.logger.error(f"Error sending ACK: {e}", exc_info=True)
    
    def _is_duplicate(self, message_id: str) -> bool:
        """Check for duplicates"""
        with self._lock:
            return message_id in self._processed_messages
    
    def _mark_processed(self, message_id: str):
        """Mark as processed"""
        with self._lock:
            self._processed_messages[message_id] = time.time()
            
            current_time = time.time()
            to_remove = [
                mid for mid, ts in self._processed_messages.items()
                if current_time - ts > self._message_retention
            ]
            
            for mid in to_remove:
                del self._processed_messages[mid]
    
    def run(self):
        """Run subscriber"""
        self.logger.info("EMQX subscriber running...")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt in run loop")
            self.disconnect()
        except Exception as e:
            self.logger.error(f"Error in run loop: {e}", exc_info=True)
            self.disconnect()
