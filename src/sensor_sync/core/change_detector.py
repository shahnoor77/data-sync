import time
import threading
import uuid
from typing import Dict, Any, Callable, Optional, List
from datetime import datetime
import queue
from enum import Enum
import psycopg2
import psycopg2.extras


class OperationType(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SNAPSHOT = "SNAPSHOT"


class ChangeDetector:
    """
    PostgreSQL WAL-based change detector (replaces Debezium)
    Polls transaction log for changes
    """
    
    def __init__(
        self,
        db_config: Dict[str, Any],
        monitored_tables: List[str],
        poll_interval: int = 5,
        batch_size: int = 100,
        event_handler: Optional[Callable] = None,
        logger = None
    ):
        """Initialize change detector"""
        self.db_config = db_config
        self.monitored_tables = monitored_tables
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.event_handler = event_handler
        self.logger = logger
        
        self.connection = None
        self.running = False
        self.event_queue = queue.Queue(maxsize=5000)
        
        # Track last LSN (Log Sequence Number)
        self.last_lsn = None
        self.offset_file = "data/offsets/change_detector.offset"
        
        # Cache for timestamp columns
        self._timestamp_columns = {}  # table -> column_name
        
        self._load_offset()
        self._init_database()
    
    def _init_database(self):
        """Initialize database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            # IMPORTANT: Set autocommit=True to avoid transaction issues
            self.connection.autocommit = True
            self.logger.info("✓ Connected to source database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def _load_offset(self):
        """Load last processed LSN from file"""
        try:
            from pathlib import Path
            offset_path = Path(self.offset_file)
            if offset_path.exists():
                self.last_lsn = offset_path.read_text().strip()
                self.logger.info(f"Loaded last LSN: {self.last_lsn}")
        except Exception as e:
            self.logger.warning(f"Could not load offset: {e}")
    
    def _save_offset(self, lsn: str):
        """Save current LSN to file"""
        try:
            from pathlib import Path
            Path(self.offset_file).parent.mkdir(parents=True, exist_ok=True)
            Path(self.offset_file).write_text(lsn)
        except Exception as e:
            self.logger.error(f"Failed to save offset: {e}")
    
    def start(self):
        """Start change detection"""
        self.running = True
        
        # Start polling thread
        poll_thread = threading.Thread(
            target=self._poll_changes,
            daemon=True,
            name="ChangeDetectorPoller"
        )
        poll_thread.start()
        
        # Start event processor thread
        process_thread = threading.Thread(
            target=self._process_events,
            daemon=True,
            name="ChangeDetectorProcessor"
        )
        process_thread.start()
        
        self.logger.info("✓ Change detector started")
    
    def _poll_changes(self):
        """Poll for database changes"""
        self.logger.info("Change polling thread started")
        
        while self.running:
            try:
                # Query changes from all monitored tables
                changes = self._query_changes()
                
                # Add changes to queue
                for change in changes:
                    try:
                        self.event_queue.put(change, timeout=1.0)
                    except queue.Full:
                        self.logger.warning("Event queue is full, dropping event")
                
                # Update LSN if we got changes
                if changes:
                    self.last_lsn = changes[-1]['current_lsn']
                    self._save_offset(self.last_lsn)
                
                time.sleep(self.poll_interval)
                
            except Exception as e:
                self.logger.error(f"Error in polling thread: {e}", exc_info=True)
                # Add delay to prevent log spamming
                time.sleep(self.poll_interval)
    
    def _query_changes(self) -> List[Dict[str, Any]]:
        """
        Query for recent changes from all monitored tables
        Enhanced with trace_id generation at the moment of database change detection
        """
        changes = []
        
        for table in self.monitored_tables:
            try:
                # Create new cursor for each table query
                cursor = self.connection.cursor()
                
                # Check if 'updated_at' column exists, otherwise use 'created_at'
                timestamp_column = self._get_timestamp_column(cursor, table)
                
                if not timestamp_column:
                    self.logger.warning(
                        f"Cannot find timestamp column in {table}, skipping"
                    )
                    cursor.close()
                    continue
                
                # Build query with the correct timestamp column
                query = f"""
                    SELECT 
                        '{table}'::text as table_name,
                        row_to_json(t)::jsonb as after,
                        'c'::char as operation,
                        EXTRACT(EPOCH FROM {timestamp_column})::bigint * 1000 as ts_ms,
                        pg_current_wal_lsn()::text as current_lsn
                    FROM {table} t
                    WHERE {timestamp_column} > now() - interval '{self.poll_interval} seconds'
                    ORDER BY {timestamp_column} DESC
                    LIMIT {self.batch_size}
                """
                
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                
                # Process results with trace_id generation
                for row in results:
                    # Enhanced Traceability: Generate trace_id at the moment of database change detection
                    trace_id = str(uuid.uuid4())
                    detection_timestamp = time.time_ns() / 1_000_000_000  # High-precision timestamp
                    
                    event = {
                        'payload': {
                            'source': {
                                'table': row['table_name'],
                                'ts_ms': row['ts_ms'],
                                'lsn': row['current_lsn'],
                                'db': self.db_config['database'],
                                'schema': 'public'
                            },
                            'op': row['operation'],  # Debezium-compatible
                            'after': dict(row['after']) if row['after'] else None,
                            # Enhanced Traceability: Add trace_id and detection metadata
                            'trace_id': trace_id,
                            'detected_at': detection_timestamp,
                            'detector_instance': 'change_detector_001'
                        }
                    }
                    changes.append(event)
                    
                    # Log trace_id generation for debugging
                    if self.logger:
                        self.logger.debug(
                            f"Enhanced Traceability: Generated trace_id {trace_id} "
                            f"for {row['table_name']} change detection"
                        )
                
            except psycopg2.Error as e:
                # Log the error but don't crash
                self.logger.warning(f"Error querying {table}: {str(e).strip()}")
                
                # Rollback any aborted transaction on this connection
                try:
                    self.connection.rollback()
                except Exception as rollback_error:
                    self.logger.error(f"Error rolling back transaction: {rollback_error}")
                
                # Continue with next table
                continue
                
            except Exception as e:
                self.logger.error(f"Unexpected error querying {table}: {e}", exc_info=True)
                
                # Try to rollback
                try:
                    self.connection.rollback()
                except:
                    pass
                
                continue
        
        return changes
    
    def _get_timestamp_column(self, cursor, table: str) -> Optional[str]:
        """
        Detect which timestamp column exists in the table
        Tries 'updated_at' first, then 'created_at', then 'last_modified'
        
        Args:
            cursor: Database cursor
            table: Table name (e.g., 'public.sensor_readings')
            
        Returns:
            Column name if found, None otherwise
        """
        # Check cache first
        if table in self._timestamp_columns:
            return self._timestamp_columns[table]
        
        try:
            # Split schema and table name
            parts = table.split('.')
            if len(parts) == 2:
                schema, table_name = parts
            else:
                schema = 'public'
                table_name = parts[0]
            
            # Query information_schema for columns
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name IN ('updated_at', 'created_at', 'last_modified')
                ORDER BY CASE column_name
                    WHEN 'updated_at' THEN 1
                    WHEN 'created_at' THEN 2
                    WHEN 'last_modified' THEN 3
                END
                LIMIT 1
            """
            
            cursor.execute(query, (schema, table_name))
            result = cursor.fetchone()
            
            if result:
                # result is a RealDictRow, get the column_name key
                column_name = result['column_name'] if isinstance(result, dict) else result[0]
                self.logger.debug(f"Using timestamp column '{column_name}' for {table}")
                
                # Cache the result
                self._timestamp_columns[table] = column_name
                return column_name
            
            self.logger.warning(
                f"No timestamp column found for {table} "
                f"(looked for: updated_at, created_at, last_modified)"
            )
            return None
            
        except Exception as e:
            self.logger.warning(f"Error detecting timestamp column for {table}: {e}")
            return None
    
    def _process_events(self):
        """Process events from queue"""
        self.logger.info("Event processor thread started")
        
        while self.running:
            try:
                event = self.event_queue.get(timeout=1.0)
                
                if self.event_handler:
                    try:
                        self.event_handler(event)
                    except Exception as e:
                        self.logger.error(f"Error in event handler: {e}", exc_info=True)
                
                self.event_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing event: {e}", exc_info=True)
                # Add delay to prevent spamming
                time.sleep(1)
    
    def stop(self):
        """Stop change detection"""
        self.logger.info("Stopping change detector...")
        self.running = False
        
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        
        self.logger.info("✓ Change detector stopped")
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.event_queue.qsize()
