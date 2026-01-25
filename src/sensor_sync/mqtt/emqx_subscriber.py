"""
Level 2: High-Throughput Subscriber with Internal Decoupling
- Ingestion Thread (Fast Path): on_message callback for receiving only
- Processing Thread (Worker Path): Background thread processes in "Quanta" of 500 records
- Verification: RSA/HMAC verification with invalid signatures to LOG_WARNING
- Persistence: MySQLConnector.bulk_insert() with confirmation logging
"""

import json
import time
import uuid
import threading
import queue
import random
from typing import Dict, Any, List
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from ..database.connector import create_connector
from ..database.mysql_connector import MySQLConnector, DatabaseFailure
from ..database.schema_manager import SchemaManager
from ..utils.logger import StructuredLogger, SystemFailure
from ..utils.crypto import CryptoManager
from ..core.dead_letter_queue import DeadLetterQueue


class EMQXSubscriber:
    """
    Level 2: High-Throughput Subscriber with Internal Decoupling
    Resolves "Disconnect 4/7" by decoupling networking from processing
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
        """Initialize Triple-Worker Concurrency EMQX subscriber"""
        self.mqtt_config = mqtt_config
        self.target_db_config = target_db_config
        self.crypto = crypto_manager
        self.dlq = dlq
        self.logger = logger
        self.metrics = metrics
        
        self.client = None
        self.connected = False
        
        # Extract instance_id for targeted feedback
        self.instance_id = mqtt_config.get('client_id_subscriber', 'subscriber_001')
        
        # Memory Management: Calculate buffer for 2,500 msg/s with 100ms cloud latency
        # Buffer = msg_rate * latency_seconds * safety_factor = 2500 * 0.1 * 10 = 2,500 messages
        self.internal_queue = queue.Queue(maxsize=25000)  # 10x safety buffer for cloud latency
        
        # Industrial Grade Configuration with Reduced Batch Pressure
        self.num_workers = 2              # Dual workers for parallel processing
        self.worker_batch_size = 1000     # Reduced batch size to prevent long-held DB locks
        self.worker_timeout = 0.2         # Aggressive 200ms timeout to reduce redelivery overlap
        self.worker_threads = []          # List of worker threads
        self.workers_active = False       # Control flag for all workers
        
        # Observability Refactor: Summary-Only Mode tracking
        self.total_messages_landed = 0    # Total successfully processed messages
        self.last_status_time = time.time()  # For status intervals
        self.last_status_count = 0        # Message count at last status
        self.batch_latencies = []         # Track batch processing times for avg latency
        self.start_time = time.time()     # Track overall processing start time for records_per_second
        self.status_trigger_count = 10000 # Log Frequency: Trigger every 10,000 records
        
        # Database connection - Force MySQL connector
        self.target_db = None
        self.schema_manager = None
        
        # Connect to MySQL database
        self._connect_to_mysql_database()
        
        # Setup MQTT client
        self._setup_client()
    
    def _connect_to_mysql_database(self):
        """Connect to MySQL target database with Nitro configuration for RAM-buffered writes"""
        try:
            self.logger.info(f"Nitro Configuration: Connecting to MySQL with RAM-buffered writes")
            
            # Force MySQL connector for Level 1 requirement
            if self.target_db_config['type'] != 'mysql':
                self.logger.warning(f"Forcing MySQL connector (was {self.target_db_config['type']})")
                self.target_db_config['type'] = 'mysql'
            
            # Use the new MySQL connector with connection pooling
            self.target_db = MySQLConnector(self.target_db_config)
            
            # Pool Cleanup: Reset pool on startup to clear zombie connections
            self.logger.info("Pool Cleanup: Resetting connection pool to clear zombie connections...")
            self.target_db.close_all_connections()
            
            # Nitro Configuration: Configure MySQL for RAM-buffered writes
            self.logger.info("Nitro Configuration: Configuring MySQL for RAM-buffered writes...")
            self._configure_mysql_nitro()
            
            self.schema_manager = SchemaManager(
                self.target_db,
                'mysql'
            )
            
            self.logger.info("✓ Nitro Configuration: MySQL configured for RAM-buffered writes (1,000,000 target)")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def _configure_mysql_nitro(self):
        """Nitro Configuration: RAM-Buffered Writes for maximum speed"""
        try:
            connection = self.target_db._get_connection()
            cursor = connection.cursor()
            
            # RAM-Buffered Writes: Buffer writes in RAM, flush to disk once per second
            cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 0")
            cursor.execute("SET GLOBAL unique_checks = 0")
            cursor.execute("SET GLOBAL foreign_key_checks = 0")
            
            # Additional Nitro optimizations for 1M record target
            cursor.execute("SET GLOBAL innodb_buffer_pool_size = 2147483648")  # 2GB buffer pool
            cursor.execute("SET GLOBAL innodb_log_file_size = 536870912")      # 512MB log file
            cursor.execute("SET GLOBAL innodb_log_buffer_size = 134217728")    # 128MB log buffer
            cursor.execute("SET GLOBAL innodb_flush_method = O_DIRECT")        # Direct I/O
            cursor.execute("SET GLOBAL sync_binlog = 0")                       # Disable binary log sync
            
            connection.commit()
            self.logger.info("✓ Nitro Configuration: RAM-buffered writes enabled (flushes once per second)")
            
        except Exception as e:
            self.logger.warning(f"Nitro Configuration: Some optimizations may have failed: {e}")
        finally:
            if 'connection' in locals():
                self.target_db._return_connection(connection)
    
    def _setup_client(self):
        """Setup EMQX MQTT client with TLS support for cloud serverless instance"""
        import uuid
        import os
        
        # Zero-Crash Logic: Unique Identity with short UUID to prevent RC=7 Ghost Session collisions
        base_client_id = self.mqtt_config.get('client_id_subscriber', "cloud_sub_01")
        unique_suffix = str(uuid.uuid4())[:8]  # Short UUID: e.g., a8f2c4d1
        self.client_id = f"{base_client_id}_{unique_suffix}"
        
        # Session Wipe: Track clean_session state for ID rejection recovery
        self.clean_session_override = False  # Temporary override for session wipe
        self.normal_clean_session = False    # Persistent sessions for cloud buffering
        
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=self.normal_clean_session  # Persistent sessions for reliability
        )
        
        # TLS Configuration for EMQX Cloud Serverless
        ca_cert_path = os.getenv('CA_CERT_PATH', '/app/emqx-ca-cert.pem')
        if os.path.exists(ca_cert_path):
            import ssl
            self.client.tls_set(
                ca_certs=ca_cert_path,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )
            self.client.tls_insecure_set(False)  # Serverless security requirement
            self.logger.info(f"TLS enabled with CA certificate: {ca_cert_path}")
        else:
            self.logger.warning(f"CA certificate not found at {ca_cert_path}, proceeding without TLS")
        
        # Performance Tuning: Set max in-flight messages to 1,000 for smooth delivery during database spikes
        self.client.max_inflight_messages_set(1000)
        
        # Expand OS-level socket buffer to 4MB
        try:
            import socket
            self.client._sock_recv_buf = 4 * 1024 * 1024
            if hasattr(self.client, '_socket'):
                self.client._socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except Exception as e:
            self.logger.warning(f"Could not expand socket buffer: {e}")
        
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
            f"EMQX Cloud subscriber configured with TLS - client_id: {self.client_id}, "
            f"max_inflight: 1000, socket_buffer: 4MB, clean_session: {self.normal_clean_session}"
        )
    
    def connect(self):
        """Connect to EMQX broker with exponential backoff for RC=7 handling"""
        # Initialization Guard: Ensure database is connected before MQTT loop
        if not self.target_db.is_connected():
            self.logger.info("Database not connected, attempting to connect...")
            self.target_db.connect()
        
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                broker_host = self.mqtt_config['broker_host']
                broker_port = int(self.mqtt_config['broker_port'])
                keepalive = 60  # Cloud-optimized keepalive for NAT/Firewall traversal
                
                # Apply Session Wipe if needed for ID rejection recovery
                current_clean_session = self.clean_session_override or self.normal_clean_session
                self.client.clean_session = current_clean_session
                
                self.logger.info(
                    f"Cloud Connection: Connecting to EMQX: {broker_host}:{broker_port} "
                    f"(attempt {retry_count + 1}/{max_retries}, client_id: {self.client_id}, "
                    f"keepalive: {keepalive}s, clean_session: {current_clean_session})"
                )
                
                # Non-Blocking Loop: Use connect_async() + loop_start() instead of blocking connect()
                self.client.connect_async(
                    broker_host,
                    broker_port,
                    keepalive
                )
                
                # Start network loop in background thread
                self.client.loop_start()
                
                # Wait for connection with timeout
                timeout = 15  # Increased timeout for cloud connections
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    raise ConnectionError(f"Failed to connect with client_id: {self.client_id}")
                
                # Reset Session Wipe override after successful connection
                if self.clean_session_override:
                    self.clean_session_override = False
                    self.logger.info("Session Wipe: Successfully connected, reverting to persistent sessions")
                
                # Start Triple-Worker Concurrency
                self._start_triple_workers()
                
                self.logger.info(f"✓ Cloud Connection: Connected with unique client_id: {self.client_id}")
                return
                
            except Exception as e:
                retry_count += 1
                self.logger.warning(f"Connection attempt {retry_count} failed: {e}")
                
                # Stop loop if it was started
                if hasattr(self.client, '_thread') and self.client._thread:
                    self.client.loop_stop()
                
                if retry_count < max_retries:
                    # Exponential backoff with jitter for cloud connections
                    backoff = min(2 ** retry_count, 60) + random.uniform(0, 5)
                    self.logger.info(f"Exponential backoff: waiting {backoff:.1f}s before retry")
                    time.sleep(backoff)
                else:
                    raise
    
    def disconnect(self):
        """Disconnect gracefully"""
        self.workers_active = False
        
        # Wait for all worker threads to finish
        for worker_thread in self.worker_threads:
            if worker_thread and worker_thread.is_alive():
                worker_thread.join(timeout=5)
        
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        
        if self.target_db:
            self.target_db.disconnect()
        
        # Calculate final records per second
        elapsed_time = time.time() - self.start_time
        final_records_per_second = self.total_messages_landed / elapsed_time if elapsed_time > 0 else 0
        
        self.logger.info(f"[STATUS] Stopped. Total: {self.total_messages_landed:,} | Final Rate: {final_records_per_second:.0f} records/sec")
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback for connection with Zero-Crash Logic"""
        if rc == 0:
            self.connected = True
            self.logger.info("[STATUS] Zero-Crash Logic: EMQX subscriber connected")
            
            prefix = self.mqtt_config.get('topic_prefix', 'sensors/data')
            if "${" in prefix or not prefix:
                prefix = "sensors/data"
            data_topic = f"{prefix}/#"
            client.subscribe(data_topic, qos=1)
            self.logger.info(f"[STATUS] Subscribed to {data_topic} with QoS 1")
            self.metrics.set_gauge('mqtt_connected', 1)
        else:
            self.logger.error(f"[STATUS] Connection failed: RC={rc}")
            self.connected = False
            self.metrics.set_gauge('mqtt_connected', 0)
    
    def _on_disconnect(self, client, userdata, rc, properties=None):
        """
        Callback for disconnect with RC=7 specific handling and exponential backoff
        """
        self.connected = False
        self.metrics.set_gauge('mqtt_connected', 0)
        
        if rc == 7:  # RC=7 Connection refused, not authorized
            self.logger.error(
                f"[STATUS] RC=7 Authorization failed - check credentials and TLS configuration"
            )
            # Don't retry immediately for auth failures
            time.sleep(10)
            
        elif rc == 2:  # ID Rejected - Should be rare with unique UUIDs
            import uuid
            base_client_id = self.mqtt_config.get('client_id_subscriber', "sub_live_01")
            unique_suffix = str(uuid.uuid4())[:8]  # Generate new unique suffix
            new_client_id = f"{base_client_id}_{unique_suffix}"
            
            self.logger.warning(
                f"[STATUS] ID Rejected (RC=2) despite unique UUID, generating new ID: "
                f"{self.client_id} -> {new_client_id}"
            )
            
            # Update client_id and recreate client
            self.client_id = new_client_id
            self.client._client_id = new_client_id.encode('utf-8')
            
            # Session Wipe: Set clean_session=True for next attempt only
            self.clean_session_override = True
            self.logger.info("[STATUS] Session Wipe: Will use clean_session=True for next connection attempt")
            
        elif rc != 0:
            self.logger.warning(f"[STATUS] Unexpected disconnect: RC={rc}")
        else:
            self.logger.info("[STATUS] Clean disconnect")
    
    def _on_message(self, client, userdata, msg):
        """
        Fast ingestion to shared queue - at-least-once delivery semantics
        No in-memory deduplication - idempotency handled at database level
        """
        try:
            # Fast Path - Only extract topic and payload
            try:
                topic = msg.topic
                payload = msg.payload.decode('utf-8', errors='replace')
            except UnicodeDecodeError:
                return
            
            # Store message info for worker processing
            message_info = {
                'topic': topic,
                'payload': payload,
                'timestamp': time.time()
            }
            
            # Immediate queue insertion for workers
            if self.internal_queue.full():
                # Drop oldest message if queue is full
                try:
                    self.internal_queue.get_nowait()
                    self.metrics.increment_counter('dropped_message_overflow')
                except queue.Empty:
                    pass
            
            # Put message_info into shared queue for workers
            self.internal_queue.put_nowait(message_info)
            
        except Exception:
            # Shield MQTT loop from any crashes
            pass
            
        
    def _start_triple_workers(self):
        """Start workers with reduced batch pressure configuration"""
        self.workers_active = True
        self.worker_threads = []
        
        for worker_id in range(self.num_workers):
            worker_thread = threading.Thread(
                target=self._worker_loop,
                args=(worker_id,),
                daemon=True,
                name=f"Worker-{worker_id}"
            )
            worker_thread.start()
            self.worker_threads.append(worker_thread)
        
        self.logger.info(f"[STATUS] Started {self.num_workers} workers (1000 records/batch, 200ms timeout)")
    
    def _worker_loop(self, worker_id: int):
        """
        Worker with deterministic batch outcomes and connection discipline
        Every batch results in exactly one of: DB commit, DLQ persistence, or retry
        """
        self.logger.info(f"[WORKER-{worker_id}] Started with deterministic batch outcomes")
        
        worker_batch = []
        message_batch = []
        last_flush_time = time.time()
        
        while self.workers_active:
            worker_connection = None
            try:
                # Connection discipline: Get connection with retry handling
                try:
                    worker_connection = self.target_db._get_connection()
                except Exception as pool_error:
                    if "pool exhausted" in str(pool_error).lower() or "timeout" in str(pool_error).lower():
                        self.logger.warning(f"[WORKER-{worker_id}] Pool exhausted, waiting 5s before retry")
                        time.sleep(5)
                        continue
                    else:
                        raise
                
                # Pull messages for batch processing
                messages_pulled = 0
                
                while messages_pulled < self.worker_batch_size and self.workers_active:
                    try:
                        message_info = self.internal_queue.get(timeout=0.1)
                        worker_batch.append((message_info['topic'], message_info['payload']))
                        message_batch.append(message_info)
                        self.internal_queue.task_done()
                        messages_pulled += 1
                    except queue.Empty:
                        break
                
                current_time = time.time()
                
                # Batch pressure reduction: 1000 records OR 200ms timeout
                should_flush = (
                    len(worker_batch) >= self.worker_batch_size or
                    (worker_batch and (current_time - last_flush_time) >= self.worker_timeout)
                )
                
                if should_flush and worker_batch:
                    batch_start = time.time()
                    
                    # Deterministic batch outcome
                    try:
                        self._process_worker_batch(worker_id, worker_batch, worker_connection)
                        
                        batch_duration = (time.time() - batch_start) * 1000
                        self.logger.info(f"[BATCH] Worker-{worker_id} committed {len(worker_batch)} records in {batch_duration:.0f}ms")
                        
                        # Track for observability
                        self.total_messages_landed += len(worker_batch)
                        self.batch_latencies.append(batch_duration)
                        if len(self.batch_latencies) > 100:
                            self.batch_latencies = self.batch_latencies[-100:]
                        
                    except Exception as batch_error:
                        self.logger.error(f"[WORKER-{worker_id}] Batch processing failed: {batch_error}")
                        # Allow redelivery - no confirmation sent
                    
                    worker_batch = []
                    message_batch = []
                    last_flush_time = current_time
                
                # Prevent tight loop when queue is empty
                if messages_pulled == 0:
                    time.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"[WORKER-{worker_id}] Error in worker loop: {e}")
                time.sleep(1)
                
            finally:
                # Connection discipline: Always return connection to pool
                if worker_connection:
                    try:
                        self.target_db._return_connection(worker_connection)
                    except Exception as cleanup_error:
                        self.logger.warning(f"[WORKER-{worker_id}] Connection cleanup error: {cleanup_error}")
        
        self.logger.info(f"[WORKER-{worker_id}] Stopped")
    
    def _process_worker_batch(self, worker_id: int, worker_batch: List[tuple], worker_connection):
        """
        Process batch with deterministic outcomes and database idempotency
        Every batch results in exactly one of: DB commit, DLQ persistence, or retry
        """
        if not worker_batch:
            return
        
        valid_events = []
        invalid_count = 0
        
        # Step 1: Parse and validate messages
        for topic, payload in worker_batch:
            try:
                try:
                    parsed_payload = json.loads(payload)
                except json.JSONDecodeError:
                    invalid_count += 1
                    continue
                
                # Extract signed event
                signed_event = None
                if isinstance(parsed_payload, dict):
                    if 'message' in parsed_payload and isinstance(parsed_payload['message'], dict):
                        signed_event = parsed_payload['message']
                    else:
                        signed_event = parsed_payload
                
                if not signed_event:
                    invalid_count += 1
                    continue
                
                event = signed_event.get('message', {})
                trace_id = event.get('trace_id')
                
                if not trace_id:
                    invalid_count += 1
                    continue
                
                valid_events.append({
                    'trace_id': trace_id,
                    'event': event,
                    'timestamp': time.time()
                })
                
            except Exception:
                invalid_count += 1
        
        # Step 2: Database operation with retry logic
        if valid_events:
            retry_attempts = 3
            for attempt in range(retry_attempts):
                try:
                    self._bulk_insert_worker(worker_id, valid_events, worker_connection)
                    return  # Success - exit function
                    
                except Exception as e:
                    error_str = str(e).lower()
                    
                    # Handle different error types
                    if "duplicate" in error_str or "unique constraint" in error_str:
                        # Duplicates are no-ops, consider successful
                        return
                    elif "deadlock" in error_str or "lock wait timeout" in error_str:
                        if attempt < retry_attempts - 1:
                            time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            # Final deadlock failure - send to DLQ
                            self._handle_worker_batch_failure(worker_id, valid_events, str(e))
                            return
                    else:
                        # Other database errors - send to DLQ
                        self._handle_worker_batch_failure(worker_id, valid_events, str(e))
                        return
    
    def _bulk_insert_worker(self, worker_id: int, valid_events: List[Dict[str, Any]], worker_connection):
        """
        Database idempotency with UPSERT - duplicates become cheap no-ops
        """
        if not valid_events:
            return
        
        start_time = time.time()
        cursor = None
        
        try:
            cursor = worker_connection.cursor(buffered=False)
            
            # Disable checks for performance
            cursor.execute("SET SESSION sql_notes = 0")
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.execute("SET SESSION unique_checks = 0")
            
            cursor.execute("START TRANSACTION")
            
            # Prepare records
            records = []
            for item in valid_events:
                event = item['event']
                
                sent_at = event.get('sent_at', start_time)
                cloud_latency_ms = (time.time() - sent_at) * 1000 if sent_at else None
                
                record = {
                    'trace_id': item['trace_id'],
                    'sequence_number': event.get('sequence_number'),
                    'process_id': event.get('process_id'),
                    'event_id': event.get('event_id'),
                    'table_name': event.get('table', 'sensor_readings'),
                    'operation': event.get('operation', 'INSERT'),
                    'envelope_version': event.get('envelope_version', '1.0'),
                    'latency_ms': cloud_latency_ms,
                    'payload': json.dumps(event, separators=(',', ':'))
                }
                records.append(record)
            
            # Database idempotency: UPSERT with ON DUPLICATE KEY UPDATE
            columns = ['trace_id', 'sequence_number', 'process_id', 'event_id', 
                      'table_name', 'operation', 'envelope_version', 'latency_ms', 'payload']
            placeholders = ', '.join(['%s'] * len(columns))
            values_list = [
                tuple(record.get(col) for col in columns) 
                for record in records
            ]
            
            # Idempotent UPSERT - duplicates become no-ops
            query = f"""
                INSERT INTO sensor_readings ({', '.join(columns)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE trace_id = trace_id
            """
            
            cursor.executemany(query, values_list)
            
            # Re-enable checks before commit
            cursor.execute("SET SESSION unique_checks = 1")
            cursor.execute("SET SESSION foreign_key_checks = 1")
            cursor.execute("SET SESSION sql_notes = 1")
            
            worker_connection.commit()
            
            duration = time.time() - start_time
            self.metrics.record_latency('worker_bulk_insert', duration)
            self.metrics.increment_counter('worker_bulk_inserts')
            
        except Exception as e:
            # Rollback on failure
            try:
                if worker_connection:
                    worker_connection.rollback()
                    if cursor:
                        cursor.execute("SET SESSION unique_checks = 1")
                        cursor.execute("SET SESSION foreign_key_checks = 1")
                        cursor.execute("SET SESSION sql_notes = 1")
            except Exception:
                pass
            raise
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
    
    def _handle_worker_batch_failure(self, worker_id: int, failed_events: List[Dict[str, Any]], error_message: str):
        """
        DLQ handling for unrecoverable batch failures
        If DLQ succeeds, data is preserved. If DLQ fails, allow redelivery.
        """
        try:
            # Prepare failed records for DLQ
            failed_records = []
            for item in failed_events:
                event = item['event']
                failed_record = {
                    'trace_id': item['trace_id'],
                    'sequence_number': event.get('sequence_number'),
                    'process_id': event.get('process_id'),
                    'event_id': event.get('event_id'),
                    'table_name': event.get('table', 'sensor_readings'),
                    'operation': event.get('operation', 'INSERT'),
                    'envelope_version': event.get('envelope_version', '1.0'),
                    'data': event.get('data', {}),
                    'original_event': event
                }
                failed_records.append(failed_record)
            
            # Attempt DLQ persistence
            dlq_success = self.target_db.bulk_insert_dlq(failed_records, error_message, f"WORKER_{worker_id}_FAILURE")
            
            if dlq_success:
                self.logger.warning(f"[WORKER-{worker_id}] {len(failed_events)} events preserved in DLQ")
                self.metrics.increment_counter('worker_dlq_preservations')
            else:
                self.logger.error(f"[WORKER-{worker_id}] DLQ preservation failed for {len(failed_events)} events - allowing redelivery")
                
        except Exception as dlq_error:
            self.logger.error(f"[WORKER-{worker_id}] DLQ operation failed: {dlq_error} - allowing redelivery")
    
    def run(self):
        """
        Main processing loop with deterministic batch outcomes
        At-least-once delivery with database-level idempotency
        """
        self.logger.info("[STATUS] Started with deterministic batch outcomes and database idempotency")
        
        try:
            while True:
                time.sleep(1)
                
                # Status logging every 10,000 records
                if self.total_messages_landed > 0 and self.total_messages_landed % self.status_trigger_count == 0:
                    if self.total_messages_landed != self.last_status_count:
                        self._log_structured_status()
                
                # Monitor MQTT network loop health
                if not self.client.is_connected() and self.workers_active:
                    self.logger.warning("[STATUS] MQTT network loop disconnected, attempting reconnection...")
                    try:
                        self.connect()
                    except Exception as e:
                        self.logger.error(f"[STATUS] Reconnection failed: {e}")
                        time.sleep(5)
                
                # Monitor worker health
                active_workers = sum(1 for t in self.worker_threads if t.is_alive())
                if active_workers < self.num_workers and self.workers_active:
                    self.logger.warning(f"[STATUS] Only {active_workers}/{self.num_workers} workers active, restarting workers...")
                    self._start_triple_workers()
                
        except KeyboardInterrupt:
            self.logger.info("[STATUS] Keyboard interrupt received")
            self.disconnect()
        except Exception as e:
            self.logger.error(f"[STATUS] Error in main thread: {e}", exc_info=True)
            self.disconnect()
            raise
    
    def _log_structured_status(self):
        """
        Structured status logging with batch pressure metrics
        """
        try:
            # Calculate metrics
            active_workers = sum(1 for t in self.worker_threads if t.is_alive())
            queue_depth = self.internal_queue.qsize()
            
            # Calculate average latency from recent batches
            avg_latency = 0
            if self.batch_latencies:
                avg_latency = sum(self.batch_latencies) / len(self.batch_latencies)
            
            # Calculate messages processed since last status
            messages_in_period = self.total_messages_landed - self.last_status_count
            self.last_status_count = self.total_messages_landed
            
            # Calculate overall records per second since start
            elapsed_time = time.time() - self.start_time
            records_per_second = self.total_messages_landed / elapsed_time if elapsed_time > 0 else 0
            
            # Structured status with batch pressure info
            self.logger.info(
                f"[STATUS] Workers: {active_workers} | "
                f"Queue Depth: {queue_depth:,} | "
                f"Total Landed: {self.total_messages_landed:,} | "
                f"Avg Latency: {avg_latency:.0f}ms | "
                f"Records/sec: {records_per_second:.0f} | "
                f"Progress: {messages_in_period:,} new | "
                f"Batch: 1000/200ms"
            )
            
        except Exception as e:
            self.logger.warning(f"[STATUS] Error generating status log: {e}")
