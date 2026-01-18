"""
Configuration loader and validator
Loads from YAML files and environment variables
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class Settings:
    """
    Application settings manager
    Combines configuration from YAML and environment variables
    """
    
    def __init__(self, config_file: str = "config/config.yaml"):
        """
        Initialize settings
        
        Args:
            config_file: Path to YAML configuration file
        """
        # Load environment variables
        load_dotenv()
        
        # Load YAML configuration
        self.config_file = Path(config_file)
        self.config = self._load_config()
        
        # Resolve environment variables in config
        self.config = self._resolve_env_vars(self.config)
        
        # Validate configuration
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_file}"
            )
        
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        return config or {}
    
    def _resolve_env_vars(self, config: Any) -> Any:
        """
        Recursively resolve environment variables in config
        Replaces ${VAR_NAME} with environment variable value
        """
        if isinstance(config, dict):
            return {
                key: self._resolve_env_vars(value)
                for key, value in config.items()
            }
        elif isinstance(config, list):
            return [self._resolve_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Check if string contains environment variable
            if config.startswith('${') and config.endswith('}'):
                env_var = config[2:-1]
                value = os.getenv(env_var)
                if value is None:
                    raise ValueError(
                        f"Environment variable not set: {env_var}"
                    )
                return value
            return config
        else:
            return config
    
    def _validate_config(self):
        """Validate required configuration fields"""
        required_sections = [
            'application',
            'debezium',
            'mqtt',
            'source_database',
            'target_database',
            'security'
        ]
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(
                    f"Required configuration section missing: {section}"
                )
        
        # Validate database types
        valid_db_types = ['postgresql', 'mysql', 'mongodb']
        source_type = self.config['source_database']['type']
        target_type = self.config['target_database']['type']
        
        if source_type not in valid_db_types:
            raise ValueError(
                f"Invalid source database type: {source_type}. "
                f"Must be one of {valid_db_types}"
            )
        
        if target_type not in valid_db_types:
            raise ValueError(
                f"Invalid target database type: {target_type}. "
                f"Must be one of {valid_db_types}"
            )
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Args:
            key_path: Path to config value (e.g., 'mqtt.broker_host')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_debezium_properties(self) -> Dict[str, str]:
        """
        Get Debezium connector properties
        Returns properties formatted for Java configuration
        """
        debezium_config = self.config['debezium']
        source_db = self.config['source_database']
        
        # Base properties
        properties = {
            'connector.class': debezium_config['connector_class'],
            'offset.storage': debezium_config['offset_storage'],
            'offset.storage.file.filename': debezium_config['offset_storage_file_filename'],
            'offset.flush.interval.ms': str(debezium_config['offset_flush_interval_ms']),
            'database.hostname': debezium_config['database_hostname'],
            'database.port': str(debezium_config['database_port']),
            'database.user': debezium_config['database_user'],
            'database.password': debezium_config['database_password'],
            'database.dbname': debezium_config['database_dbname'],
            'database.server.name': debezium_config['database_server_name'],
            'snapshot.mode': debezium_config['snapshot_mode'],
        }
        
        # PostgreSQL specific properties
        if source_db['type'] == 'postgresql':
            properties.update({
                'plugin.name': debezium_config.get('plugin_name', 'pgoutput'),
                'slot.name': debezium_config.get('slot_name', 'debezium'),
                'publication.name': debezium_config.get('publication_name', 'dbz_publication'),
                'schema.history.internal': debezium_config['schema_history_internal'],
                'schema.history.internal.file.filename': debezium_config['schema_history_internal_file_filename'],
            })
        
        # MySQL specific properties
        elif source_db['type'] == 'mysql':
            properties.update({
                'database.server.id': '1',
                'database.include.list': debezium_config['database_dbname'],
                'schema.history.internal': debezium_config['schema_history_internal'],
                'schema.history.internal.file.filename': debezium_config['schema_history_internal_file_filename'],
            })
        
        # Add table include list if specified
        if 'table_include_list' in debezium_config:
            properties['table.include.list'] = debezium_config['table_include_list']
        
        # Add decimal handling
        if 'decimal_handling_mode' in debezium_config:
            properties['decimal.handling.mode'] = debezium_config['decimal_handling_mode']
        
        # Add time precision
        if 'time_precision_mode' in debezium_config:
            properties['time.precision.mode'] = debezium_config['time_precision_mode']
        
        return properties
    
    def get_source_db_config(self) -> Dict[str, Any]:
        """Get source database configuration"""
        return self.config['source_database']
    
    def get_target_db_config(self) -> Dict[str, Any]:
        """Get target database configuration"""
        return self.config['target_database']
    
    def get_mqtt_config(self) -> Dict[str, Any]:
        """Get MQTT configuration"""
        return self.config['mqtt']
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration"""
        return self.config['security']
    
    def get_app_config(self) -> Dict[str, Any]:
        """Get application configuration"""
        return self.config['application']
    
    def is_encryption_enabled(self) -> bool:
        """Check if encryption is enabled"""
        return self.config['security'].get('enable_encryption', False)
    
    def get_sensitive_fields(self) -> list:
        """Get list of fields to encrypt"""
        return self.config['security'].get('sensitive_fields', [])
    
    def get_batch_size(self) -> int:
        """Get batch processing size"""
        return self.config['application'].get('batch_size', 100)
    
    def get_retry_config(self) -> Dict[str, int]:
        """Get retry configuration"""
        return {
            'attempts': self.config['application'].get('retry_attempts', 3),
            'delay_seconds': self.config['application'].get('retry_delay_seconds', 5)
        }
    
    def __repr__(self) -> str:
        """String representation (hide sensitive data)"""
        safe_config = self.config.copy()
        
        # Hide sensitive fields
        if 'debezium' in safe_config:
            if 'database_password' in safe_config['debezium']:
                safe_config['debezium']['database_password'] = '***'
        
        if 'source_database' in safe_config:
            if 'password' in safe_config['source_database']:
                safe_config['source_database']['password'] = '***'
        
        if 'target_database' in safe_config:
            if 'password' in safe_config['target_database']:
                safe_config['target_database']['password'] = '***'
        
        if 'mqtt' in safe_config:
            if 'password' in safe_config['mqtt']:
                safe_config['mqtt']['password'] = '***'
        
        if 'security' in safe_config:
            if 'encryption_key' in safe_config['security']:
                safe_config['security']['encryption_key'] = '***'
            if 'signing_key' in safe_config['security']:
                safe_config['security']['signing_key'] = '***'
        
        return f"Settings({safe_config})"


def create_default_config(output_path: str = "config/config.yaml"):
    """
    Create a default configuration file
    
    Args:
        output_path: Path where to save the config file
    """
    default_config = """
application:
  instance_id: "sensor_sync_001"
  log_level: "INFO"
  batch_size: 100
  retry_attempts: 3
  retry_delay_seconds: 5

debezium:
  connector_class: "io.debezium.connector.postgresql.PostgresConnector"
  database_hostname: "localhost"
  database_port: 5432
  database_user: "postgres"
  database_password: "postgres123"
  database_dbname: "sensor_source"
  database_server_name: "sensor_source_server"
  plugin_name: "pgoutput"
  slot_name: "sensor_cdc_slot"
  publication_name: "sensor_publication"
  
  offset_storage: "org.apache.kafka.connect.storage.FileOffsetBackingStore"
  offset_storage_file_filename: "data/offsets/offset.dat"
  offset_flush_interval_ms: 10000
  
  schema_history_internal: "io.debezium.storage.file.history.FileSchemaHistory"
  schema_history_internal_file_filename: "data/history/schema_history.dat"
  
  snapshot_mode: "initial"
  decimal_handling_mode: "string"
  time_precision_mode: "adaptive"
  
  table_include_list: "public.sensor_readings,public.temperature_sensors,public.pressure_sensors"

mqtt:
  broker_host: "localhost"
  broker_port: 1883
  username: "sensor_sync"
  password: "mqtt123"
  topic_prefix: "sensors/data"
  ack_topic: "sensors/ack"
  qos: 2
  keepalive: 60
  reconnect_delay_seconds: 5

source_database:
  type: "postgresql"
  host: "localhost"
  port: 5432
  database: "sensor_source"
  user: "postgres"
  password: "postgres123"

target_database:
  type: "mongodb"
  host: "localhost"
  port: 27017
  database: "sensor_target"
  user: "mongo"
  password: "mongo123"

security:
  encryption_key: "change-this-to-a-secure-32-byte-key"
  signing_key: "change-this-to-a-secure-signing-key"
  enable_encryption: true
  sensitive_fields:
    - "device_id"
    - "location"
    - "metadata"

monitoring:
  enable_metrics: true
  metrics_port: 9090
  health_check_interval: 30
"""
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        f.write(default_config.strip())
    
    print(f"✓ Created default configuration: {output_path}")