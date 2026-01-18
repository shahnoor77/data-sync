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
    
    @abstractmethod
    def connect(self):
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection"""
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
        self.connection = None
    
    def connect(self):
        """Establish PostgreSQL connection"""
        self.connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        self.connection.autocommit = False
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a PostgreSQL query"""
        cursor = self.connection.cursor()
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
        
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        record_id = cursor.fetchone()[0]
        self.connection.commit()
        
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
        
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        self.connection.commit()
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from PostgreSQL table"""
        query = f"DELETE FROM {table} WHERE {key_field} = %s"
        
        cursor = self.connection.cursor()
        cursor.execute(query, (key_value,))
        self.connection.commit()
    
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
        
        cursor = self.connection.cursor()
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
        
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, (schema, table_name))
        return cursor.fetchall()


class MySQLConnector(DatabaseConnector):
    """MySQL database connector"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize MySQL connector"""
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish MySQL connection"""
        self.connection = mysql.connector.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        self.connection.autocommit = False
    
    def disconnect(self):
        """Close MySQL connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a MySQL query"""
        cursor = self.connection.cursor()
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
        
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        record_id = cursor.lastrowid
        self.connection.commit()
        
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
        
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        self.connection.commit()
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from MySQL table"""
        query = f"DELETE FROM {table} WHERE {key_field} = %s"
        
        cursor = self.connection.cursor()
        cursor.execute(query, (key_value,))
        self.connection.commit()
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in MySQL"""
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        """
        
        cursor = self.connection.cursor()
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
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, (self.config['database'], table))
        return cursor.fetchall()


class MongoDBConnector(DatabaseConnector):
    """MongoDB database connector"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize MongoDB connector"""
        self.config = config
        self.client = None
        self.db = None
    
    def connect(self):
        """Establish MongoDB connection"""
        connection_string = f"mongodb://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}"
        self.client = MongoClient(connection_string)
        self.db = self.client[self.config['database']]
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
    
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
        collection = self.db[table]
        result = collection.insert_one(data)
        return result.inserted_id
    
    def update_record(self, table: str, data: Dict[str, Any], key_field: str, key_value: Any):
        """Update record in MongoDB collection"""
        collection = self.db[table]
        collection.update_one(
            {key_field: key_value},
            {'$set': data}
        )
    
    def delete_record(self, table: str, key_field: str, key_value: Any):
        """Delete record from MongoDB collection"""
        collection = self.db[table]
        collection.delete_one({key_field: key_value})
    
    def table_exists(self, table: str) -> bool:
        """Check if collection exists in MongoDB"""
        return table in self.db.list_collection_names()
    
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Get collection schema from MongoDB
        Note: MongoDB is schemaless, so we infer from a sample document
        """
        collection = self.db[table]
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
