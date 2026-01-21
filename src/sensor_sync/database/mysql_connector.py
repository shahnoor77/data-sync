"""
MySQL Connector with Connection Pooling and Transactional Unit of Work
Level 1: Standardized MySQL Sink with Flexible Persistence
"""

import mysql.connector
import mysql.connector.pooling
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import time
import uuid
import json
from .connector import DatabaseConnector


class DatabaseFailure(Exception):
    """Custom exception for database failures with trace_ids"""
    
    def __init__(self, message: str, trace_ids: List[str] = None):
        super().__init__(message)
        self.trace_ids = trace_ids or []


class MySQLConnector(DatabaseConnector):
    """
    Production MySQL connector with connection pooling and transactional bulk operations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MySQL connector with connection pooling
        
        Args:
            config: Database configuration dict
        """
        self.config = config
        self._connection_pool = None
        self._connection = None
        
        # Pool configuration - Pool Expansion: Increased to 20 with pre-ping for zombie detection
        self.pool_name = f"mysql_pool_{uuid.uuid4().hex[:8]}"
        self.pool_size = config.get('pool_size', 20)  # Pool Expansion: Increased to 20
        
        # Auto-migration flag
        self.auto_migrate = config.get('auto_migrate', True)
        
        # Initialize connection pool
        self._create_connection_pool()
        
        # Perform auto-migration if enabled
        if self.auto_migrate:
            self._ensure_schema_ready()
    
    def _create_connection_pool(self):
        """Create MySQL connection pool with zombie detection via enhanced health checks"""
        try:
            pool_config = {
                'pool_name': self.pool_name,
                'pool_size': self.pool_size,
                'pool_reset_session': True,
                'host': self.config['host'],
                'port': self.config['port'],
                'database': self.config['database'],
                'user': self.config['user'],
                'password': self.config['password'],
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_unicode_ci',
                'autocommit': False,  # Explicit transaction control
                'raise_on_warnings': True,
                'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO',
                'time_zone': '+00:00',  # UTC
                # Increase Timeout: 30 seconds for large batch operations
                'connection_timeout': 30,
                # Performance optimizations
                'use_unicode': True,
                'buffered': True,
                'get_warnings': False  # Disable metadata lookups for warnings
            }
            
            self._connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
            print(f"✓ MySQL connection pool created: {self.pool_size} connections with enhanced health checks")
            
        except MySQLError as e:
            raise DatabaseFailure(f"Failed to create connection pool: {e}")
    
    def close_all_connections(self):
        """
        Pool Reset on Startup: Clear any zombie connections from previous runs
        This ensures a clean slate when the subscriber starts up
        """
        try:
            if self._connection_pool:
                # Close current connection if exists
                if self._connection and self._connection.is_connected():
                    self._connection.close()
                    self._connection = None
                
                # Force close all connections in the pool by recreating it
                old_pool_name = self.pool_name
                self.pool_name = f"mysql_pool_{uuid.uuid4().hex[:8]}"
                
                # Create new pool (old one will be garbage collected)
                self._create_connection_pool()
                
                print(f"✓ Pool Reset: Cleared zombie connections, new pool: {self.pool_name}")
                
        except Exception as e:
            print(f"Warning: Pool reset failed: {e}")
            # Continue anyway - create new pool
            self._create_connection_pool()
    
    def _ensure_schema_ready(self):
        """Auto-migration: Ensure Target Indexing for high-performance inserts"""
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Create schema-agnostic sensor_readings table with Target Indexing
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                trace_id VARCHAR(36) NOT NULL,
                sequence_number BIGINT NOT NULL,
                process_id INT,
                event_id VARCHAR(255),
                table_name VARCHAR(100),
                operation VARCHAR(20) DEFAULT 'INSERT',
                payload JSON NOT NULL,
                envelope_version VARCHAR(10) DEFAULT '1.0',
                latency_ms FLOAT,
                received_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
                created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
                
                UNIQUE INDEX idx_trace_id_unique (trace_id),
                INDEX idx_sequence_number (sequence_number),
                INDEX idx_process_id (process_id),
                INDEX idx_received_at (received_at),
                INDEX idx_envelope_version (envelope_version),
                INDEX idx_latency_ms (latency_ms),
                INDEX idx_table_name (table_name),
                INDEX idx_operation (operation)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ROW_FORMAT=COMPRESSED
            """
            
            cursor.execute(create_table_sql)
            
            # Create system_dlq table for Universal Error Room
            create_dlq_sql = """
            CREATE TABLE IF NOT EXISTS system_dlq (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                trace_id VARCHAR(36),
                sequence_number BIGINT,
                process_id INT,
                batch_id VARCHAR(36),
                error_type VARCHAR(100),
                error_message TEXT,
                failed_payload JSON,
                original_table VARCHAR(100),
                retry_count INT DEFAULT 0,
                failed_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
                
                INDEX idx_trace_id (trace_id),
                INDEX idx_batch_id (batch_id),
                INDEX idx_error_type (error_type),
                INDEX idx_failed_at (failed_at),
                INDEX idx_retry_count (retry_count)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ROW_FORMAT=COMPRESSED
            """
            
            cursor.execute(create_dlq_sql)
            
            # Target Indexing: Ensure trace_id index exists for performance
            try:
                cursor.execute("""
                    ALTER TABLE sensor_readings 
                    ADD UNIQUE INDEX idx_trace_id_performance (trace_id)
                """)
                self.logger.info("✓ Target Indexing: Added performance index on trace_id")
            except Exception:
                # Index already exists
                pass
            
            connection.commit()
            print("✓ Auto-migration: Target Indexing schema ready (optimized for 1M+ inserts)")
            
        except MySQLError as e:
            # Handle table already exists gracefully
            if e.errno == 1050:  # Table already exists
                print("✓ Auto-migration: Target Indexing schema already exists")
            else:
                raise DatabaseFailure(f"Auto-migration failed: {e}")
        finally:
            if 'connection' in locals():
                self._return_connection(connection)
    
    def _get_connection(self):
        """
        Get connection from pool with enhanced health check for zombie detection
        Implements manual ping validation since pool_pre_ping is not supported
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                connection = self._connection_pool.get_connection()
                
                # Pool Expansion: Manual zombie detection via ping
                try:
                    connection.ping(reconnect=False, attempts=1, delay=0)
                    return connection
                except MySQLError:
                    # Connection is dead (zombie), discard it and get a fresh one
                    try:
                        connection.close()
                    except:
                        pass  # Ignore errors when closing dead connection
                    
                    if attempt < max_retries - 1:
                        continue  # Try again with a new connection
                    else:
                        # Last attempt, try to get fresh connection with reconnect
                        connection = self._connection_pool.get_connection()
                        connection.ping(reconnect=True, attempts=1, delay=0)
                        return connection
                        
            except MySQLError as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1)  # Brief pause before retry
                    continue
                else:
                    raise DatabaseFailure(f"Failed to get healthy connection from pool after {max_retries} attempts: {e}")
        
        raise DatabaseFailure("Failed to get connection from pool")
    
    def _return_connection(self, connection):
        """
        Return connection to pool with proper cleanup
        Ensures connection is always returned even if it's in a bad state
        """
        if connection:
            try:
                # Always close the connection to return it to pool
                # The pool will handle whether it's reusable or needs to be discarded
                connection.close()
            except Exception:
                # Ignore errors during connection return
                # The pool will handle cleanup of bad connections
                pass
    
    @property
    def connection(self):
        """Standardized connection handle - gets from pool"""
        if not self._connection or not self._connection.is_connected():
            self._connection = self._get_connection()
        return self._connection
    
    def connect(self):
        """Establish MySQL connection (pool already created)"""
        # Connection pool is already created in __init__
        # This method ensures we have an active connection
        self._connection = self._get_connection()
        print("✓ MySQL connector ready with connection pooling")
    
    def disconnect(self):
        """Close all connections in pool"""
        if self._connection:
            self._return_connection(self._connection)
            self._connection = None
        
        # Note: Connection pool cleanup is handled by garbage collection
        print("✓ MySQL connector disconnected")
    
    def is_connected(self) -> bool:
        """Check if connection pool is available"""
        try:
            if not self._connection_pool:
                return False
            
            # Test with a quick connection
            test_conn = self._get_connection()
            test_conn.ping(reconnect=True, attempts=1, delay=0)
            self._return_connection(test_conn)
            return True
            
        except Exception:
            return False
    
    def bulk_insert_dlq(self, failed_records: List[Dict[str, Any]], error_message: str, error_type: str = "BULK_INSERT_FAILURE") -> bool:
        """
        Universal Error Room: Pure INSERT Speed for DLQ operations with context manager pattern
        Preserves 1M messages for audit instead of NACKing
        
        Args:
            failed_records: List of failed record dictionaries
            error_message: Error message from the failure
            error_type: Type of error for categorization
            
        Returns:
            True if DLQ insert successful
        """
        if not failed_records:
            return True
        
        connection = None
        cursor = None
        batch_id = str(uuid.uuid4())
        
        try:
            # Context Manager Pattern: Get connection with health check
            connection = self._get_connection()
            cursor = connection.cursor(buffered=False)  # Disable buffering for speed
            
            # Pure INSERT Speed: Disable metadata lookups for DLQ
            cursor.execute("SET SESSION sql_notes = 0")
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.execute("SET SESSION unique_checks = 0")
            
            # START TRANSACTION for DLQ
            cursor.execute("START TRANSACTION")
            
            # Prepare DLQ records
            dlq_records = []
            for record in failed_records:
                dlq_record = {
                    'trace_id': record.get('trace_id'),
                    'sequence_number': record.get('sequence_number'),
                    'process_id': record.get('process_id'),
                    'batch_id': batch_id,
                    'error_type': error_type,
                    'error_message': error_message[:1000],  # Truncate long messages
                    'failed_payload': json.dumps(record, separators=(',', ':')),  # Compact JSON
                    'original_table': record.get('table_name', 'sensor_readings'),
                    'retry_count': 0
                }
                dlq_records.append(dlq_record)
            
            # Pure INSERT Speed: Bulk insert into system_dlq
            columns = list(dlq_records[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            values_list = [tuple(record[col] for col in columns) for record in dlq_records]
            
            query = f"""
                INSERT INTO system_dlq ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            cursor.executemany(query, values_list)
            
            # Re-enable checks before commit
            cursor.execute("SET SESSION unique_checks = 1")
            cursor.execute("SET SESSION foreign_key_checks = 1")
            cursor.execute("SET SESSION sql_notes = 1")
            
            connection.commit()
            
            print(f"✓ Universal Error Room: {len(failed_records)} records saved to system_dlq (batch_id: {batch_id})")
            return True
            
        except MySQLError as e:
            # ROLLBACK on failure with proper cleanup
            try:
                if connection:
                    connection.rollback()
                    # Re-enable checks after rollback
                    if cursor:
                        cursor.execute("SET SESSION unique_checks = 1")
                        cursor.execute("SET SESSION foreign_key_checks = 1")
                        cursor.execute("SET SESSION sql_notes = 1")
            except Exception:
                # Ignore cleanup errors - connection will be discarded
                pass
            raise DatabaseFailure(f"DLQ insert failed: {e}")
            
        finally:
            # Automated Release: Always clean up resources
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass  # Ignore cursor close errors
            if connection:
                self._return_connection(connection)
    def bulk_insert(self, table: str, records: List[Dict[str, Any]]) -> bool:
        """
        Pure INSERT Speed: Optimized bulk insert with context manager pattern
        Uses try...finally to ensure connection is always returned to pool
        
        Args:
            table: Target table name (typically 'sensor_readings')
            records: List of record dictionaries with schema-agnostic structure
            
        Returns:
            True if successful
            
        Raises:
            DatabaseFailure: With failed trace_ids on failure
        """
        if not records:
            return True
        
        connection = None
        cursor = None
        trace_ids = [record.get('trace_id', 'unknown') for record in records]
        
        try:
            # Context Manager Pattern: Get connection with health check
            connection = self._get_connection()
            cursor = connection.cursor(buffered=False)  # Disable buffering for speed
            
            # Pure INSERT Speed: Disable metadata lookups
            cursor.execute("SET SESSION sql_notes = 0")  # Disable notes
            cursor.execute("SET SESSION foreign_key_checks = 0")  # Skip FK checks for speed
            cursor.execute("SET SESSION unique_checks = 0")  # Skip unique checks during insert
            
            # START TRANSACTION (Level 1 requirement)
            cursor.execute("START TRANSACTION")
            
            # Transform records to schema-agnostic format with minimal processing
            schema_agnostic_records = []
            for record in records:
                # Extract core fields for indexing
                core_fields = {
                    'trace_id': record.get('trace_id'),
                    'sequence_number': record.get('sequence_number'),
                    'process_id': record.get('process_id'),
                    'event_id': record.get('event_id'),
                    'table_name': record.get('table_name', table),
                    'operation': record.get('operation', 'INSERT'),
                    'envelope_version': record.get('envelope_version', '1.0'),
                    'latency_ms': record.get('latency_ms'),
                    'payload': json.dumps(record, separators=(',', ':'))  # Compact JSON
                }
                schema_agnostic_records.append(core_fields)
            
            # Pure INSERT Speed: Optimized bulk insert without duplicate key handling
            columns = ['trace_id', 'sequence_number', 'process_id', 'event_id', 
                      'table_name', 'operation', 'envelope_version', 'latency_ms', 'payload']
            placeholders = ', '.join(['%s'] * len(columns))
            values_list = [
                tuple(record.get(col) for col in columns) 
                for record in schema_agnostic_records
            ]
            
            # Pure INSERT Speed: Simple INSERT without ON DUPLICATE KEY UPDATE
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            # Pure INSERT Speed: cursor.executemany() for maximum throughput
            cursor.executemany(query, values_list)
            
            # Re-enable checks before commit
            cursor.execute("SET SESSION unique_checks = 1")
            cursor.execute("SET SESSION foreign_key_checks = 1")
            cursor.execute("SET SESSION sql_notes = 1")
            
            # COMMIT on success (Level 1 requirement)
            connection.commit()
            
            return True
            
        except MySQLError as e:
            # ROLLBACK on failure (Level 1 requirement)
            try:
                if connection:
                    connection.rollback()
                    # Re-enable checks after rollback
                    if cursor:
                        cursor.execute("SET SESSION unique_checks = 1")
                        cursor.execute("SET SESSION foreign_key_checks = 1")
                        cursor.execute("SET SESSION sql_notes = 1")
            except Exception:
                # Ignore cleanup errors - connection will be discarded
                pass
            
            # Raise DatabaseFailure with trace_ids (Level 1 requirement)
            raise DatabaseFailure(
                f"Pure INSERT Speed bulk insert failed for table {table}: {e}",
                trace_ids=trace_ids
            )
            
        finally:
            # Automated Release: Always clean up resources
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass  # Ignore cursor close errors
            if connection:
                self._return_connection(connection)
    
    def execute_batch(self, operations: List[Dict[str, Any]]) -> bool:
        """
        Execute batch operations with transactional guarantees
        Implements the standardized interface for high-throughput processing
        """
        if not operations:
            return True
        
        # Group operations by table and type
        tables = {}
        for op in operations:
            table = op.get('table', 'sensor_readings')  # Default table
            if table not in tables:
                tables[table] = {'inserts': [], 'updates': [], 'deletes': []}
            
            op_type = op.get('operation', 'INSERT')
            if op_type in ['INSERT', 'READ']:
                # Ensure trace_id is present
                data = op.get('data', {}).copy()
                if 'trace_id' not in data and 'event_id' in op:
                    data['trace_id'] = op['event_id']
                tables[table]['inserts'].append(data)
            elif op_type == 'UPDATE':
                tables[table]['updates'].append(op)
            elif op_type == 'DELETE':
                tables[table]['deletes'].append(op)
        
        # Execute operations per table
        for table, ops in tables.items():
            if ops['inserts']:
                self.bulk_insert(table, ops['inserts'])
            
            # Handle updates and deletes individually for now
            # TODO: Implement bulk update/delete if needed
            for update_op in ops['updates']:
                self.update_record(table, update_op['data'], 'trace_id', update_op.get('trace_id'))
            
            for delete_op in ops['deletes']:
                self.delete_record(table, 'trace_id', delete_op.get('trace_id'))
        
        return True
    
    def insert_event(self, table: str, event: Dict[str, Any]) -> Any:
        """Insert a single event (standardized method)"""
        return self.insert_record(table, event)
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a MySQL query"""
        connection = self._get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            return cursor
        finally:
            self._return_connection(connection)
    
    def insert_record(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert single record into MySQL table"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            columns = list(data.keys())
            placeholders = ['%s'] * len(columns)
            values = [data[col] for col in columns]
            
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            cursor.execute(query, values)
            record_id = cursor.lastrowid
            connection.commit()
            
            return record_id
            
        except MySQLError as e:
            if connection:
                connection.rollback()
            raise DatabaseFailure(f"Insert failed: {e}")
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                self._return_connection(connection)
    
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update record in MySQL table"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
            values = list(data.values()) + [key_value]
            
            query = f"""
                UPDATE {table}
                SET {set_clause}
                WHERE {key_field} = %s
            """
            
            cursor.execute(query, values)
            connection.commit()
            
        except MySQLError as e:
            if connection:
                connection.rollback()
            raise DatabaseFailure(f"Update failed: {e}")
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                self._return_connection(connection)
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from MySQL table"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            query = f"DELETE FROM {table} WHERE {key_field} = %s"
            cursor.execute(query, (key_value,))
            connection.commit()
            
        except MySQLError as e:
            if connection:
                connection.rollback()
            raise DatabaseFailure(f"Delete failed: {e}")
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                self._return_connection(connection)
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in MySQL"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            query = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            """
            
            cursor.execute(query, (self.config['database'], table))
            count = cursor.fetchone()[0]
            return count > 0
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                self._return_connection(connection)
    
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema from MySQL"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                ORDER BY ordinal_position
            """
            
            cursor.execute(query, (self.config['database'], table))
            return cursor.fetchall()
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                self._return_connection(connection)