"""
Database connectors for different database systems
Supports PostgreSQL, MySQL, and MongoDB
"""

import psycopg2
import psycopg2.extras
import mysql.connector
from pymongo import MongoClient
from typing import Dict, Any, List, Optional, Tuple
from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    """Abstract base class for database connectors"""
    
    @property
    @abstractmethod
    def connection(self):
        """Abstract property for database connection handle"""
        pass
    
    @abstractmethod
    def connect(self):
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if database is connected"""
        pass
    
    @abstractmethod
    def execute_batch(self, operations: List[Dict[str, Any]]) -> bool:
        """Execute batch operations for high-throughput inserts"""
        pass
    
    @abstractmethod
    def insert_event(self, table: str, event: Dict[str, Any]) -> Any:
        """Insert a single event (standardized method)"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a query"""
        pass
    
    @abstractmethod
    def insert_record(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert a record"""
        pass
    
    @abstractmethod
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update a record"""
        pass
    
    @abstractmethod
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete a record"""
        pass
    
    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """Check if table exists"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema"""
        pass


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL connector
        
        Args:
            config: Database configuration dict
        """
        self.config = config
        self._connection = None
    
    @property
    def connection(self):
        """Standardized connection handle"""
        return self._connection
    
    def connect(self):
        """Establish PostgreSQL connection"""
        self._connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        self._connection.autocommit = False
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def is_connected(self) -> bool:
        """Check if PostgreSQL is connected"""
        try:
            return self._connection is not None and self._connection.closed == 0
        except Exception:
            return False
    
    def execute_batch(self, operations: List[Dict[str, Any]]) -> bool:
        """Execute batch operations for high-throughput inserts"""
        try:
            cursor = self._connection.cursor()
            
            # Group operations by table and type
            tables = {}
            for op in operations:
                table = op.get('table')
                if table not in tables:
                    tables[table] = {'inserts': [], 'updates': [], 'deletes': []}
                
                op_type = op.get('operation', 'INSERT')
                if op_type in ['INSERT', 'READ']:
                    tables[table]['inserts'].append(op.get('data', {}))
                elif op_type == 'UPDATE':
                    tables[table]['updates'].append(op)
                elif op_type == 'DELETE':
                    tables[table]['deletes'].append(op)
            
            # Execute batch inserts using execute_values
            for table, ops in tables.items():
                if ops['inserts']:
                    self._batch_insert_postgresql(cursor, table, ops['inserts'])
                if ops['updates']:
                    for update_op in ops['updates']:
                        self.update_record(table, update_op['data'], 'id', update_op.get('id'))
                if ops['deletes']:
                    for delete_op in ops['deletes']:
                        self.delete_record(table, 'id', delete_op.get('id'))
            
            self._connection.commit()
            return True
            
        except Exception as e:
            self._connection.rollback()
            raise Exception(f"PostgreSQL batch operation failed: {e}")
    
    def _batch_insert_postgresql(self, cursor, table: str, data_list: List[Dict[str, Any]]):
        """PostgreSQL-specific batch insert using execute_values"""
        if not data_list:
            return
        
        columns = list(data_list[0].keys())
        values = [tuple(row[col] for col in columns) for row in data_list]
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        psycopg2.extras.execute_values(cursor, query, values, page_size=len(values))
    
    def insert_event(self, table: str, event: Dict[str, Any]) -> Any:
        """Insert a single event (standardized method)"""
        return self.insert_record(table, event)
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a PostgreSQL query"""
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        return cursor
    
    def insert_record(self, table: str, data: Dict[str, Any]) -> Any:
        """
        Insert record into PostgreSQL table
        
        Returns:
            Inserted record ID
        """
        # Build INSERT statement
        columns = list(data.keys())
        placeholders = ['%s'] * len(columns)
        values = [data[col] for col in columns]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING id
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, values)
        record_id = cursor.fetchone()[0]
        self._connection.commit()
        
        return record_id
    
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update record in PostgreSQL table"""
        # Build UPDATE statement
        set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
        values = list(data.values()) + [key_value]
        
        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {key_field} = %s
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, values)
        self._connection.commit()
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from PostgreSQL table"""
        query = f"DELETE FROM {table} WHERE {key_field} = %s"
        
        cursor = self._connection.cursor()
        cursor.execute(query, (key_value,))
        self._connection.commit()
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in PostgreSQL"""
        # Split schema and table name
        parts = table.split('.')
        if len(parts) == 2:
            schema, table_name = parts
        else:
            schema = 'public'
            table_name = parts[0]
        
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            )
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, (schema, table_name))
        return cursor.fetchone()[0]
    
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema from PostgreSQL"""
        # Split schema and table name
        parts = table.split('.')
        if len(parts) == 2:
            schema, table_name = parts
        else:
            schema = 'public'
            table_name = parts[0]
        
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
        
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, (schema, table_name))
        return cursor.fetchall()


class MySQLConnector(DatabaseConnector):
    """MySQL database connector"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize MySQL connector"""
        self.config = config
        self._connection = None
    
    @property
    def connection(self):
        """Standardized connection handle"""
        return self._connection
    
    def connect(self):
        """Establish MySQL connection"""
        self._connection = mysql.connector.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        self._connection.autocommit = False
    
    def disconnect(self):
        """Close MySQL connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def is_connected(self) -> bool:
        """Check if MySQL is connected"""
        try:
            return self._connection is not None and self._connection.is_connected()
        except Exception:
            return False
    
    def execute_batch(self, operations: List[Dict[str, Any]]) -> bool:
        """Execute batch operations for high-throughput inserts"""
        try:
            cursor = self._connection.cursor()
            
            # Group operations by table and type
            tables = {}
            for op in operations:
                table = op.get('table')
                if table not in tables:
                    tables[table] = {'inserts': [], 'updates': [], 'deletes': []}
                
                op_type = op.get('operation', 'INSERT')
                if op_type in ['INSERT', 'READ']:
                    tables[table]['inserts'].append(op.get('data', {}))
                elif op_type == 'UPDATE':
                    tables[table]['updates'].append(op)
                elif op_type == 'DELETE':
                    tables[table]['deletes'].append(op)
            
            # Execute batch operations
            for table, ops in tables.items():
                if ops['inserts']:
                    self._batch_insert_mysql(cursor, table, ops['inserts'])
                if ops['updates']:
                    for update_op in ops['updates']:
                        self.update_record(table, update_op['data'], 'id', update_op.get('id'))
                if ops['deletes']:
                    for delete_op in ops['deletes']:
                        self.delete_record(table, 'id', delete_op.get('id'))
            
            self._connection.commit()
            return True
            
        except Exception as e:
            self._connection.rollback()
            raise Exception(f"MySQL batch operation failed: {e}")
    
    def _batch_insert_mysql(self, cursor, table: str, data_list: List[Dict[str, Any]]):
        """MySQL-specific batch insert using executemany"""
        if not data_list:
            return
        
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        values_list = [tuple(row[col] for col in columns) for row in data_list]
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(query, values_list)
    
    def insert_event(self, table: str, event: Dict[str, Any]) -> Any:
        """Insert a single event (standardized method)"""
        return self.insert_record(table, event)
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a MySQL query"""
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        return cursor
    
    def insert_record(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert record into MySQL table"""
        columns = list(data.keys())
        placeholders = ['%s'] * len(columns)
        values = [data[col] for col in columns]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, values)
        record_id = cursor.lastrowid
        self._connection.commit()
        
        return record_id
    
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update record in MySQL table"""
        set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
        values = list(data.values()) + [key_value]
        
        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {key_field} = %s
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, values)
        self._connection.commit()
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from MySQL table"""
        query = f"DELETE FROM {table} WHERE {key_field} = %s"
        
        cursor = self._connection.cursor()
        cursor.execute(query, (key_value,))
        self._connection.commit()
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in MySQL"""
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        """
        
        cursor = self._connection.cursor()
        cursor.execute(query, (self.config['database'], table))
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema from MySQL"""
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
        
        cursor = self._connection.cursor(dictionary=True)
        cursor.execute(query, (self.config['database'], table))
        return cursor.fetchall()


class MongoDBConnector(DatabaseConnector):
    """MongoDB database connector"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize MongoDB connector"""
        self.config = config
        self._client = None
        self._db = None
    
    @property
    def connection(self):
        """Standardized connection handle - returns the database object"""
        return self._db
    
    def connect(self):
        """Establish MongoDB connection"""
        connection_string = f"mongodb://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}"
        self._client = MongoClient(connection_string)
        self._db = self._client[self.config['database']]
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
    
    def is_connected(self) -> bool:
        """Check if MongoDB is connected"""
        try:
            if self._client is None or self._db is None:
                return False
            # Ping the database to check connection
            self._client.admin.command('ping')
            return True
        except Exception:
            return False
    
    def execute_batch(self, operations: List[Dict[str, Any]]) -> bool:
        """Execute batch operations for high-throughput inserts"""
        try:
            # Group operations by table and type
            tables = {}
            for op in operations:
                table = op.get('table')
                if table not in tables:
                    tables[table] = {'inserts': [], 'updates': [], 'deletes': []}
                
                op_type = op.get('operation', 'INSERT')
                if op_type in ['INSERT', 'READ']:
                    tables[table]['inserts'].append(op.get('data', {}))
                elif op_type == 'UPDATE':
                    tables[table]['updates'].append(op)
                elif op_type == 'DELETE':
                    tables[table]['deletes'].append(op)
            
            # Execute batch operations
            for table, ops in tables.items():
                collection = self._db[table]
                
                if ops['inserts']:
                    collection.insert_many(ops['inserts'])
                
                if ops['updates']:
                    for update_op in ops['updates']:
                        self.update_record(table, update_op['data'], 'id', update_op.get('id'))
                
                if ops['deletes']:
                    for delete_op in ops['deletes']:
                        self.delete_record(table, 'id', delete_op.get('id'))
            
            return True
            
        except Exception as e:
            raise Exception(f"MongoDB batch operation failed: {e}")
    
    def insert_event(self, table: str, event: Dict[str, Any]) -> Any:
        """Insert a single event (standardized method)"""
        return self.insert_record(table, event)
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a MongoDB query (not directly applicable)"""
        raise NotImplementedError("MongoDB doesn't use SQL queries")
    
    def insert_record(self, table: str, data: Dict[str, Any]) -> Any:
        """
        Insert record into MongoDB collection
        
        Args:
            table: Collection name
            data: Document to insert
            
        Returns:
            Inserted document ID
        """
        collection = self._db[table]
        result = collection.insert_one(data)
        return result.inserted_id
    
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update record in MongoDB collection"""
        collection = self._db[table]
        collection.update_one(
            {key_field: key_value},
            {'$set': data}
        )
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from MongoDB collection"""
        collection = self._db[table]
        collection.delete_one({key_field: key_value})
    
    def table_exists(self, table: str) -> bool:
        """Check if collection exists in MongoDB"""
        return table in self._db.list_collection_names()
    
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Get collection schema from MongoDB
        Note: MongoDB is schemaless, so we infer from a sample document
        """
        collection = self._db[table]
        sample = collection.find_one()
        
        if not sample:
            return []
        
        schema = []
        for field, value in sample.items():
            schema.append({
                'column_name': field,
                'data_type': type(value).__name__,
                'is_nullable': 'YES',
                'column_default': None
            })
        
        return schema


def create_connector(db_type: str, config: Dict[str, Any]) -> DatabaseConnector:
    """
    Factory function to create appropriate database connector
    
    Args:
        db_type: Type of database ('postgresql', 'mysql', 'mongodb')
        config: Database configuration
        
    Returns:
        DatabaseConnector instance
    """
    connectors = {
        'postgresql': PostgreSQLConnector,
        'mysql': MySQLConnector,
        'mongodb': MongoDBConnector
    }
    
    if db_type not in connectors:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    connector = connectors[db_type](config)
    connector.connect()
    
    return connector
