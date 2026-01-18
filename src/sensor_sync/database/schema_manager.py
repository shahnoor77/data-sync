"""
Automatic schema management and table creation
Creates target tables automatically based on source schema
"""

from typing import Dict, Any, List
from .connector import DatabaseConnector, create_connector


class SchemaManager:
    """
    Manages automatic schema creation in target database
    """
    
    def __init__(self, target_connector: DatabaseConnector, target_type: str):
        """
        Initialize schema manager
        
        Args:
            target_connector: Target database connector
            target_type: Type of target database
        """
        self.target = target_connector
        self.target_type = target_type
        self._created_tables = set()
    
    def ensure_table_exists(self, table_name: str, source_schema: List[Dict[str, Any]]):
        """
        Ensure table exists in target database, create if necessary
        
        Args:
            table_name: Name of the table
            source_schema: Schema from source database
        """
        # Check if we've already created this table
        if table_name in self._created_tables:
            return
        
        # Check if table exists
        if self.target.table_exists(table_name):
            self._created_tables.add(table_name)
            return
        
        # Create table based on target database type
        if self.target_type == 'mongodb':
            # MongoDB creates collections automatically
            self._created_tables.add(table_name)
        elif self.target_type == 'postgresql':
            self._create_postgresql_table(table_name, source_schema)
        elif self.target_type == 'mysql':
            self._create_mysql_table(table_name, source_schema)
    
    def _create_postgresql_table(self, table_name: str, source_schema: List[Dict[str, Any]]):
        """Create PostgreSQL table from source schema"""
        columns = []
        
        for col in source_schema:
            col_name = col['column_name']
            data_type = self._map_type_to_postgresql(col['data_type'])
            nullable = 'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'
            
            # Skip id if it's auto-generated
            if col_name == 'id':
                columns.append(f"{col_name} SERIAL PRIMARY KEY")
            else:
                columns.append(f"{col_name} {data_type} {nullable}")
        
        # Add metadata columns
        columns.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("cdc_operation VARCHAR(10)")
        
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
        """
        
        self.target.execute_query(create_query)
        self.target.connection.commit()
        self._created_tables.add(table_name)
    
    def _create_mysql_table(self, table_name: str, source_schema: List[Dict[str, Any]]):
        """Create MySQL table from source schema"""
        columns = []
        
        for col in source_schema:
            col_name = col['column_name']
            data_type = self._map_type_to_mysql(col['data_type'])
            nullable = 'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'
            
            # Skip id if it's auto-generated
            if col_name == 'id':
                columns.append(f"{col_name} INT AUTO_INCREMENT PRIMARY KEY")
            else:
                columns.append(f"{col_name} {data_type} {nullable}")
        
        # Add metadata columns
        columns.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("cdc_operation VARCHAR(10)")
        
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            ) ENGINE=InnoDB
        """
        
        self.target.execute_query(create_query)
        self.target.connection.commit()
        self._created_tables.add(table_name)
    
    def _map_type_to_postgresql(self, source_type: str) -> str:
        """Map source data type to PostgreSQL type"""
        type_mapping = {
            'int': 'INTEGER',
            'integer': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'decimal': 'NUMERIC',
            'numeric': 'NUMERIC',
            'real': 'REAL',
            'double': 'DOUBLE PRECISION',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR',
            'text': 'TEXT',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'boolean': 'BOOLEAN',
            'json': 'JSONB',
            'jsonb': 'JSONB'
        }
        
        return type_mapping.get(source_type.lower(), 'TEXT')
    
    def _map_type_to_mysql(self, source_type: str) -> str:
        """Map source data type to MySQL type"""
        type_mapping = {
            'int': 'INT',
            'integer': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'decimal': 'DECIMAL(10,2)',
            'numeric': 'DECIMAL(10,2)',
            'real': 'FLOAT',
            'double': 'DOUBLE',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR',
            'text': 'TEXT',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'boolean': 'BOOLEAN',
            'json': 'JSON',
            'jsonb': 'JSON'
        }
        
        return type_mapping.get(source_type.lower(), 'TEXT')