"""
Main application entry point
Orchestrates CDC engine, MQTT publisher, and subscriber
"""

import sys
import signal
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.sensor_sync.config.settings import Settings
from src.sensor_sync.core.debezium_engine import DebeziumEngine
from src.sensor_sync.core.event_processor import EventProcessor
from src.sensor_sync.core.state_manager import StateManager
from src.sensor_sync.mqtt.publisher import MQTTPublisher
from src.sensor_sync.mqtt.subscriber import MQTTSubscriber
from src.sensor_sync.utils.logger import StructuredLogger
from src.sensor_sync.utils.crypto import CryptoManager
from src.sensor_sync.utils.metrics import MetricsCollector


class SensorSyncApplication:
    """
    Main application that orchestrates all components
    """
    
    def __init__(self, config_file: str = "config/config.yaml"):
        """
        Initialize application
        
        Args:
            config_file: Path to configuration file
        """
        print("=" * 70)
        print("  SENSOR SYNC SYSTEM - Real-time CDC with MQTT")
        print("=" * 70)
        
        # Load configuration
        print("\n[1/8] Loading configuration...")
        self.settings = Settings(config_file)
        print("✓ Configuration loaded")
        
        # Setup logger
        print("\n[2/8] Setting up logging...")
        self.logger = StructuredLogger(
            name="sensor_sync",
            log_dir="logs",
            level=self.settings.get('application.log_level', 'INFO')
        )
        self.logger.info("Logger initialized")
        print("✓ Logging configured")
        
        # Setup metrics
        print("\n[3/8] Setting up metrics...")
        self.metrics = MetricsCollector(
            enable_prometheus=self.settings.get('monitoring.enable_metrics', True),
            port=self.settings.get('monitoring.metrics_port', 9090)
        )
        self.logger.info("Metrics collector initialized")
        print("✓ Metrics configured")
        
        # Setup crypto
        print("\n[4/8] Setting up encryption...")
        self.crypto = CryptoManager(
            encryption_key=self.settings.get('security.encryption_key'),
            signing_key=self.settings.get('security.signing_key')
        )
        self.logger.info("Crypto manager initialized")
        print("✓ Encryption configured")
        
        # Setup state manager
        print("\n[5/8] Setting up state manager...")
        self.state_manager = StateManager(self.logger, self.metrics)
        self.logger.info("State manager initialized")
        print("✓ State manager ready")
        
        # Setup event processor
        print("\n[6/8] Setting up event processor...")
        self.event_processor = EventProcessor(
            crypto_manager=self.crypto,
            sensitive_fields=self.settings.get_sensitive_fields(),
            logger=self.logger,
            metrics=self.metrics
        )
        self.logger.info("Event processor initialized")
        print("✓ Event processor ready")
        
        # Setup MQTT publisher
        print("\n[7/8] Setting up MQTT publisher...")
        self.mqtt_publisher = MQTTPublisher(
            config=self.settings.get_mqtt_config(),
            state_manager=self.state_manager,
            logger=self.logger,
            metrics=self.metrics
        )
        self.logger.info("MQTT publisher initialized")
        print("✓ MQTT publisher ready")
        
        # Setup Debezium engine
        print("\n[8/8] Setting up Debezium CDC engine...")
        self.debezium_engine = DebeziumEngine(
            properties=self.settings.get_debezium_properties(),
            event_handler=self._handle_cdc_event,
            logger=self.logger
        )
        self.logger.info("Debezium engine initialized")
        print("✓ Debezium engine ready")
        
        # Running flag
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("\n✓ All components initialized successfully!")
        print("=" * 70)
    
    def _handle_cdc_event(self, raw_event: dict):
        """
        Handle CDC event from Debezium
        This is called by the Debezium engine for each captured event
        """
        try:
            # Increment capture counter
            self.metrics.increment_counter('events_captured')
            
            # Process event (transform, encrypt, sign)
            processed_event = self.event_processor.process_event(raw_event)
            
            if processed_event:
                # Publish to MQTT
                success = self.mqtt_publisher.publish_event(processed_event)
                
                if not success:
                    self.logger.error(
                        "Failed to publish event",
                        event_id=processed_event.get('message', {}).get('event_id')
                    )
            
        except Exception as e:
            self.logger.error(f"Error handling CDC event: {e}", exc_info=True)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the application"""
        try:
            self.logger.info("=" * 70)
            self.logger.info("Starting Sensor Sync System")
            self.logger.info("=" * 70)
            
            # Connect MQTT publisher
            self.logger.info("Connecting MQTT publisher...")
            self.mqtt_publisher.connect()
            
            # Start Debezium engine
            self.logger.info("Starting Debezium CDC engine...")
            self.debezium_engine.start()
            
            self.running = True
            
            self.logger.info("✓ System started successfully!")
            self.logger.info("Monitoring for database changes...")
            
            # Keep running
            while self.running:
                time.sleep(1)
                
                # Update metrics
                self.metrics.set_gauge(
                    'queue_size',
                    self.debezium_engine.get_queue_size()
                )
                
                # Cleanup old events periodically
                if int(time.time()) % 300 == 0:  # Every 5 minutes
                    self.state_manager.cleanup_old_events()
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting application: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the application"""
        if not self.running:
            return
        
        self.logger.info("Stopping Sensor Sync System...")
        self.running = False
        
        # Stop Debezium engine
        if self.debezium_engine:
            self.logger.info("Stopping Debezium engine...")
            self.debezium_engine.stop()
        
        # Disconnect MQTT publisher
        if self.mqtt_publisher:
            self.logger.info("Disconnecting MQTT publisher...")
            self.mqtt_publisher.disconnect()
        
        # Print final statistics
        stats = self.state_manager.get_statistics()
        self.logger.info("=" * 70)
        self.logger.info("Final Statistics:")
        self.logger.info(f"  Total Events: {stats['total_events']}")
        self.logger.info(f"  Completed: {stats['completed_events']}")
        self.logger.info(f"  Failed: {stats['failed_events']}")
        self.logger.info(f"  Pending: {stats['pending_events']}")
        self.logger.info("=" * 70)
        
        self.logger.info("✓ System stopped successfully")


class SubscriberApplication:
    """
    Separate application for MQTT subscriber
    Runs independently from the publisher
    """
    
    def __init__(self, config_file: str = "config/config.yaml"):
        """Initialize subscriber application"""
        print("=" * 70)
        print("  SENSOR SYNC SUBSCRIBER - MQTT to Database Writer")
        print("=" * 70)
        
        # Load configuration
        print("\n[1/5] Loading configuration...")
        self.settings = Settings(config_file)
        print("✓ Configuration loaded")
        
        # Setup logger
        print("\n[2/5] Setting up logging...")
        self.logger = StructuredLogger(
            name="sensor_sync_subscriber",
            log_dir="logs",
            level=self.settings.get('application.log_level', 'INFO')
        )
        print("✓ Logging configured")
        
        # Setup metrics
        print("\n[3/5] Setting up metrics...")
        self.metrics = MetricsCollector(
            enable_prometheus=False  # Disable for subscriber
        )
        print("✓ Metrics configured")
        
        # Setup crypto
        print("\n[4/5] Setting up encryption...")
        self.crypto = CryptoManager(
            encryption_key=self.settings.get('security.encryption_key'),
            signing_key=self.settings.get('security.signing_key')
        )
        print("✓ Encryption configured")
        
        # Setup MQTT subscriber
        print("\n[5/5] Setting up MQTT subscriber...")
        self.mqtt_subscriber = MQTTSubscriber(
            mqtt_config=self.settings.get_mqtt_config(),
            target_db_config=self.settings.get_target_db_config(),
            crypto_manager=self.crypto,
            logger=self.logger,
            metrics=self.metrics
        )
        print("✓ MQTT subscriber ready")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("\n✓ Subscriber initialized successfully!")
        print("=" * 70)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the subscriber"""
        try:
            self.logger.info("=" * 70)
            self.logger.info("Starting Subscriber")
            self.logger.info("=" * 70)
            
            # Connect to MQTT broker
            self.logger.info("Connecting to MQTT broker...")
            self.mqtt_subscriber.connect()
            
            self.logger.info("✓ Subscriber started successfully!")
            self.logger.info("Waiting for CDC events...")
            
            # Run subscriber (blocking)
            self.mqtt_subscriber.run()
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting subscriber: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the subscriber"""
        self.logger.info("Stopping Subscriber...")
        
        if self.mqtt_subscriber:
            self.mqtt_subscriber.disconnect()
        
        self.logger.info("✓ Subscriber stopped successfully")


def main():
    """Main entry point"""
    import os
    
    # Check service mode from environment
    service_mode = os.getenv('SERVICE_MODE', 'publisher').lower()
    
    if service_mode == 'publisher':
        # Run publisher (CDC -> MQTT)
        app = SensorSyncApplication()
        app.start()
    elif service_mode == 'subscriber':
        # Run subscriber (MQTT -> Target DB)
        app = SubscriberApplication()
        app.start()
    else:
        print(f"Unknown service mode: {service_mode}")
        print("Set SERVICE_MODE environment variable to 'publisher' or 'subscriber'")
        sys.exit(1)


if __name__ == "__main__":
    main()